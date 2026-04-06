// ============================================================
// extractor.js — Structured field extraction per document type
// ============================================================

import {
  DOC_TYPES, FIELD_SCHEMAS, INDUSTRY_BENCHMARKS, EBITDA_RANGE_MIDPOINTS,
} from './schemas.js';

/**
 * Extract structured fields from a classified document.
 *
 * @param {Object} classification - ClassificationResult from classifier.js
 * @param {Object} fileDescriptor - Original file descriptor with content
 * @param {Object} context        - { companyName, industry, ebitdaRange }
 * @returns {ExtractionResult}
 */
export function extractFields(classification, fileDescriptor, context = {}) {
  const { docType, confidence: classConfidence } = classification;
  const { content } = fileDescriptor;
  const schema = FIELD_SCHEMAS[docType];
  const allowSyntheticFallback = Boolean(context.allowDemoFallback);

  if (!schema || docType === DOC_TYPES.UNKNOWN) {
    return makeUnusableResult(docType);
  }

  if (content && typeof content === 'object' && content.__backendIngestion) {
    return extractFromBackendIngestion(
      content.backendFileResult,
      schema,
      docType,
      classConfidence,
      context,
      allowSyntheticFallback,
    );
  }

  // If real content is available, attempt structured parsing.
  // For now, content parsing is a placeholder — fall through to synthetic.
  if (content && typeof content === 'object' && content.__parsed) {
    return extractFromParsed(content, schema, docType, classConfidence);
  }

  if (!allowSyntheticFallback) {
    return {
      ...makeUnusableResult(docType),
      warnings: ['No normalized backend extraction was available, and demo fallback mode is disabled.'],
    };
  }

  // Synthetic mode: generate plausible data for demo/development
  return extractSynthetic(docType, schema, context, classConfidence);
}

// ---- Synthetic Data Generation ----

function extractSynthetic(docType, schema, context, classConfidence) {
  const { industry, ebitdaRange } = context;
  const bench = INDUSTRY_BENCHMARKS[industry] || INDUSTRY_BENCHMARKS['Other'];
  const ebitdaMid = EBITDA_RANGE_MIDPOINTS[ebitdaRange] || 2000000;
  const revenueMid = ebitdaMid / (bench.ebitdaMargin / 100);

  const generators = {
    [DOC_TYPES.INCOME_STATEMENT]:    () => genIncomeStatement(revenueMid, bench),
    [DOC_TYPES.BALANCE_SHEET]:       () => genBalanceSheet(revenueMid, bench),
    [DOC_TYPES.CASH_FLOW_STATEMENT]: () => genCashFlow(ebitdaMid),
    [DOC_TYPES.TAX_RETURN]:          () => genTaxReturn(revenueMid, ebitdaMid),
    [DOC_TYPES.QOE_REPORT]:          () => genQoE(ebitdaMid),
    [DOC_TYPES.PROJECTIONS]:         () => genProjections(revenueMid, ebitdaMid, bench),
    [DOC_TYPES.AR_AGING]:            () => genARAging(revenueMid),
    [DOC_TYPES.AP_AGING]:            () => genAPAging(revenueMid),
    [DOC_TYPES.DEBT_SCHEDULE]:       () => genDebtSchedule(ebitdaMid),
    [DOC_TYPES.REVENUE_BREAKDOWN]:   () => genRevenueBreakdown(revenueMid),
  };

  const generator = generators[docType];
  if (!generator) return makeUnusableResult(docType);

  const { data, periods, warnings: genWarnings } = generator();

  // Compute coverage
  const allFields = [...schema.requiredFields, ...schema.optionalFields];
  const samplePeriod = periods[0] || '_single';
  const sampleData = data[samplePeriod] || data;
  const found = allFields.filter(f => sampleData[f.name] !== undefined && sampleData[f.name] !== null);
  const missing = allFields.filter(f => sampleData[f.name] === undefined || sampleData[f.name] === null).map(f => f.name);

  // Sanity checks
  const warnings = [...(genWarnings || []), ...runSanityChecks(docType, data, periods)];

  // Confidence: based on classification confidence and coverage
  const coveragePct = found.length / Math.max(allFields.length, 1);
  let confidence = Math.min(classConfidence, 0.7 + coveragePct * 0.3); // synthetic caps at ~1.0
  confidence = round(confidence);

  const usable = found.length >= schema.minUsabilityFields;

  return {
    docType,
    periods,
    data,
    coverage: {
      total: allFields.length,
      found: found.length,
      missing,
      percentage: round(coveragePct * 100),
    },
    warnings,
    confidence,
    usable,
    synthetic: true,
  };
}

// ---- Placeholder for parsed content extraction ----

function extractFromBackendIngestion(fileResult, schema, docType, classConfidence, context, allowSyntheticFallback) {
  const backendExtraction = fileResult?.extraction;
  if (backendExtraction?.usable) {
    return {
      docType,
      periods: backendExtraction.periods || [],
      data: backendExtraction.data || {},
      coverage: backendExtraction.coverage || { total: 0, found: 0, missing: [], percentage: 0 },
      sourceFileName: fileResult?.file?.originalName || null,
      sourceSheetName: fileResult?.splitDocument?.sheetName || fileResult?.classification?.primarySheetName || null,
      sourceMetadata: backendExtraction.sourceMetadata || null,
      provenance: backendExtraction.provenance || null,
      interpretability: backendExtraction.interpretability || null,
      warnings: backendExtraction.warnings || [],
      confidence: round(backendExtraction.confidence ?? classConfidence),
      usable: true,
      synthetic: Boolean(backendExtraction.synthetic),
    };
  }

  const pipelineContent = fileResult?.normalization?.pipelineContent;
  if (pipelineContent?.__parsed) {
    return extractFromParsed(pipelineContent, schema, docType, classConfidence);
  }

  const backendWarnings = backendExtraction?.warnings || [];
  const reviewWarning = fileResult?.classification?.needsManualReview
    ? ['Backend classification confidence is low; review the file mapping before relying on modeled values.']
    : [];

  if (!allowSyntheticFallback) {
    return {
      ...makeUnusableResult(docType),
      provenance: backendExtraction?.provenance || null,
      sourceMetadata: backendExtraction?.sourceMetadata || null,
      interpretability: backendExtraction?.interpretability || null,
      warnings: [
        ...backendWarnings,
        ...reviewWarning,
        'Backend parsing completed, but normalized schema mapping was not strong enough for scoring. Demo fallback mode is disabled, so this document was excluded from scored outputs.',
      ],
    };
  }

  const syntheticResult = extractSynthetic(docType, schema, context, classConfidence);

  return {
    ...syntheticResult,
    warnings: [
      ...backendWarnings,
      ...reviewWarning,
      ...syntheticResult.warnings,
      'Backend parsing completed, but schema mapping is still pending. Using modeled values for scoring until extraction is implemented.',
    ],
  };
}

function extractFromParsed(parsedContent, schema, docType, classConfidence) {
  // Future: map parsedContent fields to schema fields
  // For now, return the parsed data directly if it matches expected shape
  const allFields = [...schema.requiredFields, ...schema.optionalFields];
  const data = parsedContent.data || {};
  const periods = parsedContent.periods || ['LTM'];

  const sampleData = data[periods[0]] || data;
  const found = allFields.filter(f => sampleData[f.name] != null);
  const missing = allFields.filter(f => sampleData[f.name] == null).map(f => f.name);
  const coveragePct = found.length / Math.max(allFields.length, 1);

  return {
    docType,
    periods,
    data,
    coverage: {
      total: allFields.length,
      found: found.length,
      missing,
      percentage: round(coveragePct * 100),
    },
    warnings: runSanityChecks(docType, data, periods),
    confidence: round(classConfidence * (0.5 + coveragePct * 0.5)),
    usable: found.length >= schema.minUsabilityFields,
    synthetic: false,
  };
}

// ---- Generators for each document type ----

function genIncomeStatement(revenueMid, bench) {
  const years = ['2021', '2022', '2023', '2024', 'LTM'];
  const growthRates = [0, 0.07, 0.06, 0.09, 0.05]; // YoY growth
  const data = {};
  let rev = revenueMid * 0.78; // start ~22% below LTM

  for (let i = 0; i < years.length; i++) {
    rev = i === 0 ? rev : rev * (1 + growthRates[i]);
    const gm = (bench.grossMargin + jitter(2)) / 100;
    const em = (bench.ebitdaMargin + jitter(1.5)) / 100;
    const nm = em * 0.55 + jitter(0.02);
    const cogs = rev * (1 - gm);
    const grossProfit = rev * gm;
    const ebitda = rev * em;
    const depreciation = rev * 0.02;
    const amortization = rev * 0.005;
    const interestExpense = ebitda * 0.12;
    const netIncome = rev * nm;

    data[years[i]] = {
      revenue: round2(rev),
      cogs: round2(cogs),
      grossProfit: round2(grossProfit),
      grossMargin: round(gm * 100),
      operatingExpenses: round2(grossProfit - ebitda),
      ebitda: round2(ebitda),
      ebitdaMargin: round(em * 100),
      depreciation: round2(depreciation),
      amortization: round2(amortization),
      interestExpense: round2(interestExpense),
      netIncome: round2(netIncome),
      netMargin: round(nm * 100),
    };
  }

  return { data, periods: years, warnings: [] };
}

function genBalanceSheet(revenueMid, bench) {
  const years = ['2021', '2022', '2023', '2024', 'LTM'];
  const data = {};
  let totalAssets = revenueMid * 0.85;

  for (let i = 0; i < years.length; i++) {
    totalAssets *= (1 + 0.05 + jitter(0.02));
    const cash = totalAssets * (0.08 + jitter(0.02));
    const ar = totalAssets * (0.15 + jitter(0.02));
    const inventory = totalAssets * (0.10 + jitter(0.01));
    const totalCurrentAssets = cash + ar + inventory + totalAssets * 0.03;
    const ppe = totalAssets * 0.25;
    const ap = totalAssets * (0.08 + jitter(0.01));
    const currentPortionLTD = totalAssets * 0.03;
    const totalCurrentLiabilities = ap + currentPortionLTD + totalAssets * 0.04;
    const longTermDebt = revenueMid * (bench.debtToEbitda / 100) * (bench.ebitdaMargin) * (0.9 + jitter(0.1));
    const totalLiabilities = totalCurrentLiabilities + longTermDebt;
    const equity = totalAssets - totalLiabilities;

    data[years[i]] = {
      cash: round2(cash),
      accountsReceivable: round2(ar),
      inventory: round2(inventory),
      totalCurrentAssets: round2(totalCurrentAssets),
      ppe: round2(ppe),
      totalAssets: round2(totalAssets),
      accountsPayable: round2(ap),
      currentPortionLTD: round2(currentPortionLTD),
      totalCurrentLiabilities: round2(totalCurrentLiabilities),
      longTermDebt: round2(longTermDebt),
      totalLiabilities: round2(totalLiabilities),
      equity: round2(equity),
    };
  }

  return { data, periods: years, warnings: [] };
}

function genCashFlow(ebitdaMid) {
  const years = ['2021', '2022', '2023', '2024', 'LTM'];
  const data = {};

  for (let i = 0; i < years.length; i++) {
    const ebitda = ebitdaMid * (0.78 + i * 0.06 + jitter(0.03));
    const ocf = ebitda * (0.85 + jitter(0.05));
    const capex = -Math.abs(ebitda * (0.15 + jitter(0.05)));
    const fcf = ocf + capex;
    const investing = capex - Math.abs(ebitda * 0.05);
    const financing = -(ebitda * (0.2 + jitter(0.1)));

    data[years[i]] = {
      operatingCashFlow: round2(ocf),
      capex: round2(capex),
      freeCashFlow: round2(fcf),
      investingCashFlow: round2(investing),
      financingCashFlow: round2(financing),
      netChangeInCash: round2(ocf + investing + financing),
    };
  }

  return { data, periods: years, warnings: [] };
}

function genTaxReturn(revenueMid, ebitdaMid) {
  const data = {
    '2023': {
      taxableIncome: round2(ebitdaMid * 0.8),
      totalTaxLiability: round2(ebitdaMid * 0.8 * 0.21),
      effectiveTaxRate: 21,
      entityType: 'S-Corp',
      filingYear: '2023',
      grossReceipts: round2(revenueMid * 0.95),
      officerCompensation: round2(ebitdaMid * 0.15),
    },
  };
  return { data, periods: ['2023'], warnings: [] };
}

function genQoE(ebitdaMid) {
  const years = ['2022', '2023', '2024'];
  const data = {};

  for (const yr of years) {
    const mult = yr === '2022' ? 0.85 : yr === '2023' ? 0.93 : 1.0;
    const reported = ebitdaMid * mult;
    const addBacks = round2(ebitdaMid * 0.08);
    data[yr] = {
      reportedEbitda: round2(reported),
      adjustedEbitda: round2(reported + addBacks),
      totalAddBacks: addBacks,
      addBacks: [
        { item: 'Owner compensation above market', amount: round2(addBacks * 0.55) },
        { item: 'One-time legal fees', amount: round2(addBacks * 0.25) },
        { item: 'Non-recurring consulting', amount: round2(addBacks * 0.20) },
      ],
      ownerCompensation: round2(ebitdaMid * 0.15),
      normalizedOwnerComp: round2(ebitdaMid * 0.06),
      workingCapitalTarget: round2(ebitdaMid * 0.3),
    };
  }

  return { data, periods: years, warnings: [] };
}

function genProjections(revenueMid, ebitdaMid, bench) {
  const years = ['2025', '2026', '2027'];
  const data = {};

  for (let i = 0; i < years.length; i++) {
    const growthRate = 12 + jitter(2);
    const rev = revenueMid * Math.pow(1 + growthRate / 100, i + 1);
    const margin = bench.ebitdaMargin + 1 + i * 0.5;

    data[years[i]] = {
      projectedRevenue: round2(rev),
      projectedEbitda: round2(rev * margin / 100),
      projectedGrowthRate: round(growthRate),
      projectedMargin: round(margin),
      assumptions: [
        'Continued organic growth in existing markets',
        'Stable gross margins with modest pricing increases',
        'Headcount growth of 5-8% annually',
      ],
    };
  }

  return { data, periods: years, warnings: [] };
}

function genARAging(revenueMid) {
  const totalAR = revenueMid * 0.14;
  const data = {
    _single: {
      totalAR: round2(totalAR),
      current: round2(totalAR * 0.65),
      days30: round2(totalAR * 0.18),
      days60: round2(totalAR * 0.09),
      days90: round2(totalAR * 0.05),
      days90Plus: round2(totalAR * 0.03),
      concentrationTopCustomer: round(22 + jitter(5)),
      concentrationTop5: round(55 + jitter(8)),
    },
  };
  return { data, periods: ['_single'], warnings: [] };
}

function genAPAging(revenueMid) {
  const totalAP = revenueMid * 0.08;
  const data = {
    _single: {
      totalAP: round2(totalAP),
      current: round2(totalAP * 0.72),
      days30: round2(totalAP * 0.16),
      days60: round2(totalAP * 0.07),
      days90: round2(totalAP * 0.03),
      days90Plus: round2(totalAP * 0.02),
    },
  };
  return { data, periods: ['_single'], warnings: [] };
}

function genDebtSchedule(ebitdaMid) {
  const totalDebt = ebitdaMid * (3.5 + jitter(0.5));
  const data = {
    _single: {
      totalDebt: round2(totalDebt),
      instruments: [
        { name: 'Senior Term Loan A', principal: round2(totalDebt * 0.55), rate: 6.5, maturityDate: '2027-06-15', type: 'term' },
        { name: 'Revolving Credit Facility', principal: round2(totalDebt * 0.20), rate: 5.75, maturityDate: '2026-12-01', type: 'revolver' },
        { name: 'Subordinated Note', principal: round2(totalDebt * 0.25), rate: 9.0, maturityDate: '2029-03-01', type: 'subordinated' },
      ],
      weightedAvgRate: round(6.5 * 0.55 + 5.75 * 0.20 + 9.0 * 0.25),
      annualDebtService: round2(totalDebt * 0.12),
      nearestMaturity: '2026-12-01',
    },
  };
  return { data, periods: ['_single'], warnings: [] };
}

function genRevenueBreakdown(revenueMid) {
  const customers = [
    { name: 'Customer A', revenue: round2(revenueMid * 0.22), percentage: 22 },
    { name: 'Customer B', revenue: round2(revenueMid * 0.14), percentage: 14 },
    { name: 'Customer C', revenue: round2(revenueMid * 0.11), percentage: 11 },
    { name: 'Customer D', revenue: round2(revenueMid * 0.08), percentage: 8 },
    { name: 'Customer E', revenue: round2(revenueMid * 0.06), percentage: 6 },
    { name: 'Other (35 customers)', revenue: round2(revenueMid * 0.39), percentage: 39 },
  ];
  const data = {
    _single: {
      totalRevenue: round2(revenueMid),
      customers,
      topCustomerPct: 22,
      top3Pct: 47,
      top5Pct: 61,
      customerCount: 40,
    },
  };
  return { data, periods: ['_single'], warnings: [] };
}

// ---- Sanity Checks ----

function runSanityChecks(docType, data, periods) {
  const warnings = [];
  const period = periods[periods.length - 1]; // check most recent
  const d = data[period] || data._single || {};

  if (docType === DOC_TYPES.INCOME_STATEMENT) {
    if (d.grossMargin != null && (d.grossMargin < 10 || d.grossMargin > 90)) {
      warnings.push(`Gross margin ${d.grossMargin}% is outside typical 10–90% range`);
    }
    if (d.ebitdaMargin != null && d.grossMargin != null && d.ebitdaMargin > d.grossMargin) {
      warnings.push('EBITDA margin exceeds gross margin — likely data error');
    }
    if (d.revenue != null && d.revenue < 0) {
      warnings.push('Negative revenue detected');
    }
  }

  if (docType === DOC_TYPES.AR_AGING && d.totalAR != null) {
    const sum = (d.current || 0) + (d.days30 || 0) + (d.days60 || 0) + (d.days90 || 0) + (d.days90Plus || 0);
    if (Math.abs(sum - d.totalAR) > d.totalAR * 0.05) {
      warnings.push('AR aging buckets do not sum to total AR (>5% variance)');
    }
  }

  if (docType === DOC_TYPES.REVENUE_BREAKDOWN && d.customers) {
    const totalPct = d.customers.reduce((s, c) => s + (c.percentage || 0), 0);
    if (Math.abs(totalPct - 100) > 5) {
      warnings.push(`Customer revenue percentages sum to ${totalPct}%, expected ~100%`);
    }
  }

  return warnings;
}

// ---- Utilities ----

function makeUnusableResult(docType) {
  return {
    docType,
    periods: [],
    data: {},
    coverage: { total: 0, found: 0, missing: [], percentage: 0 },
    warnings: ['Document type unknown or unrecognized — cannot extract fields'],
    confidence: 0,
    usable: false,
    synthetic: false,
  };
}

function jitter(range) {
  return (Math.random() - 0.5) * 2 * range;
}

function round(n) {
  return Math.round(n * 100) / 100;
}

function round2(n) {
  return Math.round(n);
}
