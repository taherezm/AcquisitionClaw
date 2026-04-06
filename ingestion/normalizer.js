// ============================================================
// normalizer.js — Unified normalized financial model
// ============================================================
//
// Assembles extracted document data into a unified financial model,
// delegates scoring to scoring/engine.js, and produces the final
// dashboard data object.

import { DOC_TYPES, DOC_TYPE_LABELS, INDUSTRY_BENCHMARKS } from './schemas.js';
import {
  buildAmbiguityWorkflowSummary,
  buildDocumentConfidenceDecomposition,
  buildEntityResolutionSummary,
  buildEvidenceResolutionSummary,
  buildTemporalAlignmentSummary,
} from './evidenceResolver.js';
import { buildReconciliationSummary } from './reconciliation.js';
import { runScoringEngine, formatDimensionsForDashboard } from '../scoring/engine.js';
import { validateFinancialData } from './validator.js';

/**
 * Aggregate extraction results into the dashboard data model.
 *
 * @param {ExtractionResult[]} results  - One per ingested document
 * @param {Object} context              - { companyName, industry, ebitdaRange }
 * @returns {Object} Dashboard-ready data
 */
export function normalizeToModel(results, context) {
  const { companyName, industry } = context;
  const reviewerSignals = context.reviewRankingSignals || null;

  // Index results by type (prefer highest confidence when duplicates)
  const byType = indexByType(results);
  const documentTypes = Object.keys(byType);

  // ---- Build unified financial model ----
  const timeSeries     = buildTimeSeries(byType);
  const revenueData    = buildRevenueAndEbitda(byType, timeSeries);
  const marginsData    = buildMargins(byType, timeSeries);
  const leverageData   = buildLeverage(byType, timeSeries);
  const cashflowData   = buildCashflow(byType);
  const concentration  = buildConcentration(byType);
  const earningsQuality = buildEarningsQuality(byType);
  const forecastData   = buildForecast(byType, revenueData);
  const balanceSheetData = buildBalanceSheetSummary(byType, timeSeries);

  const financialModel = {
    revenueData, marginsData, leverageData, cashflowData,
    concentration, earningsQuality, forecastData, balanceSheetData,
    documentTypes,
  };

  const reconciliation = buildReconciliationSummary(results);
  const evidenceResolution = buildEvidenceResolutionSummary(results, reconciliation, reviewerSignals);
  const temporalAlignment = buildTemporalAlignmentSummary(results, evidenceResolution, reviewerSignals);
  const entityResolution = buildEntityResolutionSummary(results, reviewerSignals);

  // ---- Validate normalized data before scoring ----
  const validation = validateFinancialData({
    byType,
    financialModel,
    reconciliation,
    evidenceResolution,
    temporalAlignment,
  });

  // ---- Run scoring engine ----
  const scoring = runScoringEngine(financialModel, industry, validation);

  // ---- Assemble narrative outputs ----
  const investmentSummary = generateInvestmentSummary({
    companyName, revenueData, marginsData, cashflowData,
    leverageData, concentration, overallScore: scoring.overall.score,
  });
  const customerBreakdown = buildCustomerBreakdown(byType);
  const expenseComposition = buildExpenseComposition(byType, timeSeries);
  const arAging = buildARAging(byType);
  const apAging = buildAPAging(byType);
  const debtProfile = buildDebtProfile(byType);
  const forecastComparison = buildForecastComparison(byType, revenueData);
  const workingCapital = buildWorkingCapital(byType, timeSeries);
  const acquisitionAdvice = generateAcquisitionAdvice({
    companyName,
    overall: scoring.overall,
    dimensions: scoring.dimensions,
    risks: scoring.risks,
    missingItems: scoring.missingItems,
    financialModel,
    leverageData,
    concentration,
    earningsQuality,
    forecastData,
    balanceSheetData,
  });
  const growthOpportunities = generateGrowthOpportunities({
    companyName,
    overall: scoring.overall,
    dimensions: scoring.dimensions,
    revenueData,
    marginsData,
    leverageData,
    cashflowData,
    concentration,
    earningsQuality,
    forecastData,
    customerBreakdown,
    expenseComposition,
    arAging,
    workingCapital,
    missingItems: scoring.missingItems,
    availableDocTypes: documentTypes,
  });
  const nextSteps = generateNextSteps(scoring.risks, scoring.dimensions);
  const ambiguityWorkflows = buildAmbiguityWorkflowSummary(
    results,
    reconciliation,
    evidenceResolution,
    temporalAlignment,
    entityResolution,
  );
  const dataQuality = buildDataQualitySummary(results, validation, reconciliation, {
    evidenceResolution,
    temporalAlignment,
    entityResolution,
    ambiguityWorkflows,
    reviewerSignals,
  });

  // ---- Return dashboard-ready object ----
  return {
    // Scores
    overallScore: scoring.overall.score,
    verdict:      scoring.overall.verdict,
    description:  scoring.overall.explanation,
    overallConfidence: scoring.overall.confidence,
    baseConfidence: scoring.overall.baseConfidence,

    // Sub-scores (with full explanation data)
    subScores: formatDimensionsForDashboard(scoring.dimensions),

    // Risk flags (backward compat)
    riskFlags: scoring.flags,

    // New: detailed analysis outputs
    strengths:    scoring.strengths,
    risks:        scoring.risks,
    missingItems: scoring.missingItems,

    // Chart data — core
    revenue: {
      labels: revenueData.labels,
      revenue: revenueData.revenue,
      ebitda: revenueData.ebitda,
    },
    margins: {
      labels: marginsData.labels,
      gross: marginsData.gross,
      ebitda: marginsData.ebitda,
      net: marginsData.net,
    },
    leverage: {
      labels: leverageData.labels,
      debtToEbitda: leverageData.debtToEbitda,
      interestCoverage: leverageData.interestCoverage,
    },
    cashflow: {
      labels: cashflowData.labels,
      values: cashflowData.values,
    },

    // Chart data — extended
    customerBreakdown,
    expenseComposition,
    arAging,
    apAging,
    debtProfile,
    forecastComparison,
    workingCapital,

    // Data quality & validation
    dataQuality,
    validation,

    // Narratives
    investmentSummary,
    acquisitionAdvice,
    growthOpportunities,
    nextSteps,
  };
}

// ============================================================
// Data assembly functions (unchanged in purpose, refined)
// ============================================================

function indexByType(results) {
  const map = {};
  for (const r of results) {
    if (!r.usable) continue;
    if (!map[r.docType] || r.confidence > map[r.docType].confidence) {
      map[r.docType] = r;
    }
  }
  return map;
}

function buildTimeSeries(byType) {
  const is = byType[DOC_TYPES.INCOME_STATEMENT];
  if (is && is.periods.length > 0) return is.periods;
  const bs = byType[DOC_TYPES.BALANCE_SHEET];
  if (bs && bs.periods.length > 0) return bs.periods;
  return ['2021', '2022', '2023', '2024', 'LTM'];
}

function buildRevenueAndEbitda(byType, periods) {
  const is  = byType[DOC_TYPES.INCOME_STATEMENT];
  const qoe = byType[DOC_TYPES.QOE_REPORT];
  const labels = periods.filter(p => p !== '_single');
  const revenue = [], ebitda = [];

  for (const yr of labels) {
    const isD  = is?.data?.[yr];
    const qoeD = qoe?.data?.[yr];
    revenue.push(toMillions(isD?.revenue));
    ebitda.push(toMillions(qoeD?.adjustedEbitda ?? isD?.ebitda));
  }
  return { labels, revenue, ebitda };
}

function buildMargins(byType, periods) {
  const is = byType[DOC_TYPES.INCOME_STATEMENT];
  const labels = periods.filter(p => p !== '_single');
  const gross = [], ebitdaM = [], net = [];

  for (const yr of labels) {
    const d = is?.data?.[yr];
    gross.push(d?.grossMargin ?? null);
    ebitdaM.push(d?.ebitdaMargin ?? null);
    net.push(d?.netMargin ?? null);
  }
  return { labels, gross, ebitda: ebitdaM, net };
}

function buildLeverage(byType, periods) {
  const bs = byType[DOC_TYPES.BALANCE_SHEET];
  const is = byType[DOC_TYPES.INCOME_STATEMENT];
  const ds = byType[DOC_TYPES.DEBT_SCHEDULE];
  const labels = periods.filter(p => p !== '_single');
  const debtToEbitda = [], interestCoverage = [];

  for (const yr of labels) {
    const debt = ds?.data?._single?.totalDebt ?? bs?.data?.[yr]?.longTermDebt;
    const ebitda = is?.data?.[yr]?.ebitda;
    const interest = is?.data?.[yr]?.interestExpense;

    debtToEbitda.push(
      debt != null && ebitda > 0 ? round(debt / ebitda, 1) : null
    );
    interestCoverage.push(
      ebitda != null && interest > 0 ? round(ebitda / interest, 1) : null
    );
  }
  return { labels, debtToEbitda, interestCoverage };
}

function buildCashflow(byType) {
  const cf = byType[DOC_TYPES.CASH_FLOW_STATEMENT];
  const latest = cf ? cf.periods[cf.periods.length - 1] : null;
  const d = latest ? cf.data[latest] : null;

  if (d) {
    return {
      labels: ['Operating', 'Investing', 'Financing', 'Free Cash Flow'],
      values: [
        toMillions(d.operatingCashFlow),
        toMillions(d.investingCashFlow),
        toMillions(d.financingCashFlow),
        toMillions(d.freeCashFlow),
      ],
    };
  }
  return { labels: ['Operating', 'Investing', 'Financing', 'Free Cash Flow'], values: [0, 0, 0, 0] };
}

function buildConcentration(byType) {
  const rb = byType[DOC_TYPES.REVENUE_BREAKDOWN];
  const ar = byType[DOC_TYPES.AR_AGING];
  const d  = rb?.data?._single ?? {};
  const arD = ar?.data?._single ?? {};
  const proxyCustomers = Array.isArray(arD.customers) ? arD.customers : [];
  const proxyPercentages = proxyCustomers
    .map((customer) => customer.percentage)
    .filter((value) => value != null)
    .sort((left, right) => right - left);
  const proxyTop3Pct = proxyPercentages.length > 0
    ? round(proxyPercentages.slice(0, 3).reduce((sum, value) => sum + value, 0), 1)
    : null;

  return {
    topCustomerPct: d.topCustomerPct ?? arD.concentrationTopCustomer ?? null,
    top3Pct:        d.top3Pct ?? proxyTop3Pct,
    top5Pct:        d.top5Pct ?? arD.concentrationTop5 ?? null,
    customerCount:  d.customerCount ?? (proxyCustomers.length || null),
  };
}

function buildEarningsQuality(byType) {
  const qoe = byType[DOC_TYPES.QOE_REPORT];
  if (!qoe) return { addBackPct: null, ownerCompAboveMarket: null };

  const latest = qoe.periods[qoe.periods.length - 1];
  const d = qoe.data[latest] || {};

  return {
    addBackPct: d.adjustedEbitda && d.totalAddBacks
      ? round((d.totalAddBacks / d.adjustedEbitda) * 100, 1) : null,
    ownerCompAboveMarket: d.ownerCompensation && d.normalizedOwnerComp
      ? d.ownerCompensation - d.normalizedOwnerComp : null,
  };
}

function buildForecast(byType, revenueData) {
  const proj = byType[DOC_TYPES.PROJECTIONS];
  if (!proj) return { projectedGrowth: null, historicalCAGR: null, gap: null };

  const d = proj.data[proj.periods[0]] || {};
  const projectedGrowth = d.projectedGrowthRate ?? null;
  const revs = revenueData.revenue.filter(r => r != null && r > 0);

  let historicalCAGR = null;
  if (revs.length >= 2) {
    historicalCAGR = round((Math.pow(revs[revs.length - 1] / revs[0], 1 / (revs.length - 1)) - 1) * 100, 1);
  }

  const gap = projectedGrowth != null && historicalCAGR != null
    ? round(projectedGrowth - historicalCAGR, 1) : null;

  return { projectedGrowth, historicalCAGR, gap };
}

function buildBalanceSheetSummary(byType, periods) {
  const bs = byType[DOC_TYPES.BALANCE_SHEET];
  if (!bs) return { latest: {}, previous: {} };

  const validPeriods = periods.filter(p => p !== '_single' && bs.data[p]);
  const latestPeriod = validPeriods[validPeriods.length - 1];
  const prevPeriod   = validPeriods.length > 1 ? validPeriods[validPeriods.length - 2] : null;

  return {
    latest:   latestPeriod ? bs.data[latestPeriod] : {},
    previous: prevPeriod ? bs.data[prevPeriod] : {},
  };
}

// ============================================================
// Extended chart data builders
// ============================================================

function buildCustomerBreakdown(byType) {
  const rb = byType[DOC_TYPES.REVENUE_BREAKDOWN];
  const customers = rb?.data?._single?.customers;
  if (customers && customers.length > 0) {
    return {
      customers: customers.map(c => ({ name: c.name, percentage: c.percentage })),
      sourceLabel: rb?.data?._single?.breakdownBasis === 'service_line' ? 'Service-line revenue mix' : 'Customer revenue concentration',
      proxy: rb?.data?._single?.breakdownBasis === 'service_line',
    };
  }

  const arCustomers = byType[DOC_TYPES.AR_AGING]?.data?._single?.customers;
  if (!arCustomers || arCustomers.length === 0) return null;

  return {
    customers: arCustomers
      .filter((customer) => customer.percentage != null)
      .sort((left, right) => (right.percentage || 0) - (left.percentage || 0))
      .slice(0, 8)
      .map((customer) => ({ name: customer.name, percentage: customer.percentage })),
    sourceLabel: 'AR concentration proxy',
    proxy: true,
  };
}

function buildExpenseComposition(byType, periods) {
  const is = byType[DOC_TYPES.INCOME_STATEMENT];
  if (!is) return null;
  const latest = periods.filter(p => p !== '_single' && is.data[p]).pop();
  const d = latest ? is.data[latest] : null;
  if (!d || !d.revenue) return null;

  const cogs = d.cogs || 0;
  const opex = d.operatingExpenses || 0;
  const depr = (d.depreciation || 0) + (d.amortization || 0);
  const interest = d.interestExpense || 0;
  const other = Math.max(0, d.revenue - d.netIncome - cogs - opex - depr - interest) || 0;

  const labels = [];
  const values = [];
  if (cogs > 0) { labels.push('COGS'); values.push(toMillions(cogs)); }
  if (opex > 0) { labels.push('Operating Expenses'); values.push(toMillions(opex)); }
  if (depr > 0) { labels.push('D&A'); values.push(toMillions(depr)); }
  if (interest > 0) { labels.push('Interest'); values.push(toMillions(interest)); }
  if (other > 0) { labels.push('Tax & Other'); values.push(toMillions(other)); }

  return labels.length > 0 ? { labels, values } : null;
}

function buildARAging(byType) {
  const ar = byType[DOC_TYPES.AR_AGING];
  const d = ar?.data?._single;
  if (!d) return null;

  return {
    labels: ['Current', '1–30 Days', '31–60 Days', '61–90 Days', '90+ Days'],
    values: [d.current || 0, d.days30 || 0, d.days60 || 0, d.days90 || 0, d.days90Plus || 0],
  };
}

function buildAPAging(byType) {
  const ap = byType[DOC_TYPES.AP_AGING];
  const d = ap?.data?._single;
  if (!d) return null;

  return {
    labels: ['Current', '1–30 Days', '31–60 Days', '61–90 Days', '90+ Days'],
    values: [d.current || 0, d.days30 || 0, d.days60 || 0, d.days90 || 0, d.days90Plus || 0],
  };
}

function buildDebtProfile(byType) {
  const ds = byType[DOC_TYPES.DEBT_SCHEDULE];
  const instruments = ds?.data?._single?.instruments;
  if (!instruments || instruments.length === 0) return null;

  return {
    instruments: instruments.map(i => ({
      name: i.name,
      principal: toMillions(i.principal),
      rate: i.rate,
      maturity: i.maturityDate || '—',
    })),
  };
}

function buildForecastComparison(byType, revenueData) {
  const proj = byType[DOC_TYPES.PROJECTIONS];
  if (!proj) return null;

  const histLabels = revenueData.labels || [];
  const histRevenue = revenueData.revenue || [];
  const histEbitda = revenueData.ebitda || [];

  const projPeriods = proj.periods.filter(p => p !== '_single');
  const projRevenue = projPeriods.map(yr => toMillions(proj.data[yr]?.projectedRevenue));
  const projEbitda = projPeriods.map(yr => toMillions(proj.data[yr]?.projectedEbitda));

  const labels = [...histLabels, ...projPeriods];
  const dividerIndex = histLabels.length;

  // Pad arrays so historical and projected don't overlap
  const hRev = [...histRevenue, ...projPeriods.map(() => null)];
  const pRev = [...histLabels.map(() => null), ...projRevenue];
  const ebitdaLine = [...histEbitda, ...projEbitda];

  return {
    labels,
    historicalRevenue: hRev,
    projectedRevenue: pRev,
    historicalEbitda: ebitdaLine.slice(0, dividerIndex),
    projectedEbitda: ebitdaLine.slice(dividerIndex),
    dividerIndex,
  };
}

function buildWorkingCapital(byType, periods) {
  const bs = byType[DOC_TYPES.BALANCE_SHEET];
  if (!bs) return null;

  const labels = periods.filter(p => p !== '_single' && bs.data[p]);
  if (labels.length === 0) return null;

  const currentAssets = labels.map(yr => toMillions(bs.data[yr]?.totalCurrentAssets));
  const currentLiabilities = labels.map(yr => toMillions(bs.data[yr]?.totalCurrentLiabilities));
  const netWorkingCapital = labels.map((_, i) =>
    currentAssets[i] != null && currentLiabilities[i] != null
      ? round(currentAssets[i] - currentLiabilities[i], 1) : null
  );

  return { labels, currentAssets, currentLiabilities, netWorkingCapital };
}

// ============================================================
// Narrative generators
// ============================================================

function generateInvestmentSummary(ctx) {
  const { companyName, revenueData, marginsData, cashflowData, leverageData, concentration, overallScore } = ctx;
  const latestRev    = lastNonNull(revenueData.revenue);
  const latestEbitda = lastNonNull(revenueData.ebitda);
  const latestMargin = lastNonNull(marginsData.ebitda);
  const latestLev    = lastNonNull(leverageData.debtToEbitda);
  const ocf          = cashflowData.values?.[0];

  const revStr    = latestRev != null ? `$${latestRev.toFixed(1)}M` : 'N/A';
  const ebitdaStr = latestEbitda != null ? `$${latestEbitda.toFixed(2)}M` : 'N/A';
  const marginStr = latestMargin != null ? `${latestMargin.toFixed(1)}%` : 'N/A';

  let s = `<strong>${companyName}</strong> is a mid-market business generating ${revStr} in trailing revenue with ${ebitdaStr} adjusted EBITDA (${marginStr} margin). `;

  const revs = revenueData.revenue.filter(r => r != null && r > 0);
  if (revs.length >= 2) {
    const cagr = ((Math.pow(revs[revs.length - 1] / revs[0], 1 / (revs.length - 1)) - 1) * 100).toFixed(1);
    s += `The company has demonstrated top-line growth of ~${cagr}% CAGR with `;
    s += latestMargin != null && latestMargin > 15
      ? 'healthy margins, indicating pricing power and operational efficiency gains.'
      : 'stable margins across the review period.';
  }

  s += '<br><br>';

  if (ocf != null && latestEbitda != null && latestEbitda > 0) {
    const conv = Math.round((ocf / latestEbitda) * 100);
    s += `Key strengths include ${conv >= 80 ? 'strong' : 'adequate'} cash flow conversion (${conv}% OCF/EBITDA)`;
  } else {
    s += 'Key strengths include the company\'s established market position';
  }

  if (concentration.topCustomerPct != null && concentration.topCustomerPct > 20) {
    s += `. However, meaningful customer concentration (top customer = ${concentration.topCustomerPct}% of revenue)`;
    s += latestLev != null && latestLev > 3
      ? ` and above-target leverage (${latestLev.toFixed(1)}x) present integration and financing risks.`
      : ' presents a key integration risk that should be addressed through deal structuring.';
  } else {
    s += ' and a well-diversified customer base.';
  }

  s += '<br><br>';

  if (latestEbitda != null) {
    const lo = overallScore >= 72 ? 5.0 : 4.0;
    const hi = overallScore >= 72 ? 7.0 : 5.5;
    s += `At a preliminary valuation range of ${lo.toFixed(1)}–${hi.toFixed(1)}x EBITDA ($${(latestEbitda * lo).toFixed(1)}M–$${(latestEbitda * hi).toFixed(1)}M enterprise value), the deal is actionable contingent on satisfactory Quality of Earnings findings and negotiation of appropriate risk mitigants.`;
  }

  return s;
}

function generateAcquisitionAdvice(ctx) {
  const {
    companyName,
    overall,
    dimensions,
    risks,
    missingItems,
    leverageData,
    concentration,
    earningsQuality,
    forecastData,
    balanceSheetData,
  } = ctx;

  const priorities = [];
  const add = (stage, priority, title, category, confidence, body, support = {}) => {
    priorities.push({
      stage,
      priority,
      title,
      category,
      confidence,
      body,
      support,
    });
  };

  const dim = (key) => dimensions.find((entry) => entry.key === key);
  const profitability = dim('profitability');
  const revenueStability = dim('revenueStability');
  const liquidity = dim('liquidity');
  const leverage = dim('leverage');
  const cashConversion = dim('cashConversion');
  const concentrationDim = dim('concentration');
  const earningsQualityDim = dim('earningsQuality');
  const forecastCredibility = dim('forecastCredibility');

  const hasMissing = (pattern) => missingItems.some((item) => item.text.toLowerCase().includes(pattern.toLowerCase()));
  const highOrCriticalMissing = missingItems.filter((item) => item.impact === 'critical' || item.impact === 'high');
  const highRisks = risks.filter((risk) => risk.severity === 'high');

  if (highOrCriticalMissing.length > 0) {
    add(
      'critical',
      1,
      'Close core financial verification gaps before price discovery',
      'financial verification',
      overall.confidence === 'high' ? 'medium' : 'high',
      `Do not anchor valuation too tightly until the seller provides the missing core support, including ${highOrCriticalMissing.slice(0, 3).map((item) => item.text.replace(' not provided', '')).join(', ')}. The current view is directionally useful, but incomplete document coverage materially limits confidence in EBITDA, leverage, and working-capital conclusions.`,
      {
        requests: highOrCriticalMissing.map((item) => item.text),
        valuationImpact: 'Incomplete support should justify a narrower LOI range, heavier diligence conditions, or a delayed indication of value.',
      }
    );
  }

  if ((concentration?.topCustomerPct ?? 0) >= 15 || (concentrationDim?.score ?? 100) < 65) {
    add(
      'critical',
      2,
      'Underwrite customer concentration before committing to a clean LOI',
      'customer diligence',
      concentrationDim?.confidence || 'medium',
      `Customer concentration appears meaningful${concentration?.topCustomerPct != null ? `, with the top customer at ${concentration.topCustomerPct}% of revenue` : ''}. A buyer should request top-customer contracts, renewal history, churn data, and gross-margin by account to determine whether revenue is transferable and whether concentration should drive an earnout, customer-retention holdback, or lower upfront multiple.`,
      {
        managementQuestions: [
          'Which customer relationships are personally owned by the seller?',
          'What has renewal and pricing behavior looked like for the top 10 accounts?',
          'Are there any contracts up for renewal, rebid, or repricing in the next 12 months?',
        ],
        structure: 'Consider retention-based holdback or earnout if a small number of accounts drive a disproportionate share of EBITDA.',
      }
    );
  }

  if ((leverage?.score ?? 100) < 65 || (lastNonNull(leverageData.debtToEbitda) ?? 0) > 3.5) {
    const latestLev = lastNonNull(leverageData.debtToEbitda);
    add(
      'critical',
      3,
      'Pressure-test debt capacity and existing lender constraints',
      'debt and covenant review',
      leverage?.confidence || 'medium',
      `Leverage looks elevated${latestLev != null ? ` at ${latestLev.toFixed(1)}x Debt/EBITDA` : ''}. Before an LOI hardens, request the full debt schedule, note agreements, covenant package, and any lender consent requirements. This is a likely area for price tension if refinancing costs, prepayment penalties, or covenant tightness reduce post-close cash flow.`,
      {
        requests: [
          'Full debt schedule by instrument',
          'Current covenant compliance certificates',
          'Payoff letters and prepayment penalty detail',
        ],
        structure: 'May require price adjustment for debt-like items or a lower leverage assumption in the capital structure.',
      }
    );
  }

  if ((earningsQualityDim?.score ?? 100) < 70 || earningsQuality.addBackPct != null || earningsQuality.ownerCompAboveMarket != null) {
    const normalizationNotes = [];
    if (earningsQuality.addBackPct != null) normalizationNotes.push(`add-backs equal ${earningsQuality.addBackPct.toFixed(1)}% of adjusted EBITDA`);
    if (earningsQuality.ownerCompAboveMarket != null) normalizationNotes.push(`owner compensation appears above market by about $${Math.round(earningsQuality.ownerCompAboveMarket / 1000)}K`);

    add(
      'critical',
      4,
      'Validate earnings normalization and QoE adjustments',
      'earnings normalization review',
      earningsQualityDim?.confidence || 'medium',
      `Normalized earnings need direct verification${normalizationNotes.length > 0 ? ` because ${normalizationNotes.join(' and ')}` : ''}. The buyer should commission QoE work that ties reported EBITDA to tax returns, general ledger support, and bank statements before relying on headline multiple math.`,
      {
        requests: [
          'Monthly P&L detail and general ledger export',
          'Owner compensation detail and related-party expenses',
          'Bank statements and tax returns for tie-out testing',
        ],
        structure: 'Potential purchase price adjustment if EBITDA normalization proves aggressive.',
      }
    );
  }

  if ((profitability?.score ?? 100) < 70) {
    add(
      'important',
      5,
      'Test whether current margins are sustainable under new ownership',
      'margin sustainability',
      profitability?.confidence || 'medium',
      'Margin quality should be validated by reviewing product or service mix, supplier concentration, pricing power, and any deferred maintenance in SG&A. A search-fund buyer should be cautious about paying for margins that depend on founder relationships, underinvestment, or temporary cost deferrals.',
      {
        managementQuestions: [
          'Which gross-margin gains were structural versus one-time?',
          'What expenses have been intentionally delayed or minimized by the current owner?',
          'How should compensation, rent, or shared services be normalized post-close?',
        ],
      }
    );
  }

  if ((cashConversion?.score ?? 100) < 72 || (liquidity?.score ?? 100) < 70 || hasMissing('AR Aging Schedule') || hasMissing('AP Aging Schedule')) {
    add(
      'important',
      6,
      'Run a focused working-capital diligence workstream',
      'working capital diligence',
      cashConversion?.confidence || liquidity?.confidence || 'medium',
      'Working capital should be diligenced separately from EBITDA. Request month-end AR/AP agings, inventory detail where relevant, and a trailing monthly net working capital rollforward to determine the normalized peg and identify any seasonality, slow collections, or stretched payables.',
      {
        requests: [
          'Trailing 12-month monthly working-capital rollforward',
          'AR aging and bad-debt history',
          'AP aging and key vendor terms',
        ],
        structure: 'A weak peg can be handled through a post-close true-up rather than headline price, but it still affects buyer returns.',
      }
    );
  }

  if ((forecastCredibility?.score ?? 100) < 68 || forecastData.gap != null) {
    add(
      'important',
      7,
      'Challenge the management case and downside scenario',
      'financial verification',
      forecastCredibility?.confidence || 'medium',
      `Management projections need a bottoms-up review${forecastData.gap != null ? ` because projected growth appears to exceed historical growth by about ${forecastData.gap.toFixed(1)} percentage points` : ''}. Ask management to reconcile the forecast to bookings, pipeline, customer retention assumptions, and hiring plans before underwriting any upside in valuation.`,
      {
        managementQuestions: [
          'What specific commercial initiatives drive the forecast above historical trend?',
          'How much of next year revenue is already contracted or highly visible?',
          'What downside case does management use internally?',
        ],
      }
    );
  }

  add(
    'important',
    8,
    'Assess owner dependence and management transition risk',
    'management reliance / owner dependence',
    overall.confidence === 'low' ? 'medium' : 'high',
    `${companyName} should be diligenced for key-person dependence even if the financial profile is workable. A search-fund buyer should map who owns customer relationships, operational decision-making, lender relationships, and vendor negotiations to determine whether a transition services agreement, seller rollover, or retention package is required.`,
    {
      managementQuestions: [
        'Which decisions still require direct owner approval today?',
        'Who are the top second-layer managers and what are their retention expectations?',
        'Which customer or vendor relationships would be most disrupted by a transition?',
      ],
      structure: 'May support a seller rollover, consulting agreement, or earnout tied to transition success.',
    }
  );

  priorities.sort((a, b) => a.priority - b.priority);

  return {
    attractiveness: getAttractiveness(overall.score, overall.confidence, highRisks.length, highOrCriticalMissing.length),
    confidence: overall.confidence,
    summary: buildAdviceSummary(overall.score, overall.confidence, highRisks.length, highOrCriticalMissing.length),
    keyRisks: risks.slice(0, 5),
    criticalBeforeLoi: priorities.filter((item) => item.stage === 'critical'),
    importantDuringDiligence: priorities.filter((item) => item.stage === 'important'),
  };
}

function generateGrowthOpportunities(ctx) {
  const {
    companyName,
    overall,
    dimensions,
    revenueData,
    marginsData,
    leverageData,
    cashflowData,
    concentration,
    earningsQuality,
    forecastData,
    customerBreakdown,
    expenseComposition,
    arAging,
    workingCapital,
    missingItems,
    availableDocTypes,
  } = ctx;

  const latestRevenue = lastNonNull(revenueData.revenue);
  const latestEbitda = lastNonNull(revenueData.ebitda);
  const latestGrossMargin = lastNonNull(marginsData.gross);
  const firstGrossMargin = firstNonNull(marginsData.gross);
  const latestEbitdaMargin = lastNonNull(marginsData.ebitda);
  const revs = revenueData.revenue.filter(v => v != null && v > 0);
  const historicalGrowth = revs.length >= 2
    ? ((Math.pow(revs[revs.length - 1] / revs[0], 1 / (revs.length - 1)) - 1) * 100)
    : null;
  const operatingExpense = expenseComposition?.labels?.includes('Operating Expenses')
    ? expenseComposition.values[expenseComposition.labels.indexOf('Operating Expenses')]
    : null;
  const cogs = expenseComposition?.labels?.includes('COGS')
    ? expenseComposition.values[expenseComposition.labels.indexOf('COGS')]
    : null;
  const over30Ar = arAging?.values ? arAging.values.slice(2).reduce((sum, val) => sum + val, 0) : null;
  const totalAr = arAging?.values ? arAging.values.reduce((sum, val) => sum + val, 0) : null;
  const arDragPct = totalAr ? (over30Ar / totalAr) * 100 : null;

  const opportunities = [];
  const push = ({
    category,
    title,
    why,
    signals,
    impactLow,
    impactHigh,
    confidence,
    priority,
    assumptions,
    evidenceDocTypes = [],
    executionWindow = 'post-close initiative',
  }) => {
    opportunities.push({
      category,
      title,
      whyItExists: why,
      supportingMetrics: signals.filter(Boolean),
      estimatedImpact: formatImpactRange(impactLow, impactHigh),
      confidence,
      priority,
      underwritingAssumptions: normalizeAssumptions(assumptions),
      evidenceDocuments: mapEvidenceDocs(evidenceDocTypes, availableDocTypes),
      executionWindow,
      midpointImpact: ((impactLow || 0) + (impactHigh || 0)) / 2,
    });
  };

  if (latestRevenue != null && latestGrossMargin != null && latestEbitdaMargin != null && concentration.top3Pct != null && concentration.top3Pct >= 35) {
    const top3Revenue = latestRevenue * (concentration.top3Pct / 100);
    const low = top3Revenue * 0.01 * (latestGrossMargin / 100) * 0.75;
    const high = top3Revenue * 0.02 * (latestGrossMargin / 100) * 0.8;
    push({
      category: 'Revenue Growth Opportunities',
      title: 'Targeted price-and-mix expansion within the largest accounts',
      why: `The company already supports meaningful wallet share with its top accounts, and gross margin has improved from ${firstGrossMargin?.toFixed(1) || 'N/A'}% to ${latestGrossMargin.toFixed(1)}% without interrupting growth. That pattern usually indicates room for selective repricing, premium mix migration, or higher-value service bundling rather than broad-based volume chasing.`,
      signals: [
        `Top 3 customers represent ${concentration.top3Pct}% of revenue`,
        `Gross margin improved to ${latestGrossMargin.toFixed(1)}%`,
        historicalGrowth != null ? `Historical revenue CAGR: ${historicalGrowth.toFixed(1)}%` : null,
      ],
      impactLow: low,
      impactHigh: high,
      confidence: 'medium',
      priority: 1,
      assumptions: [
        'Assumes 1% to 2% net price-and-mix improvement inside the top 3 customer base.',
        `Assumes ${Math.round((latestGrossMargin / 100) * 100)}% gross-margin conversion and 75% to 80% drop-through to EBITDA.`,
      ],
      evidenceDocTypes: [DOC_TYPES.INCOME_STATEMENT, DOC_TYPES.REVENUE_BREAKDOWN],
      executionWindow: 'quick win',
    });
  }

  if (operatingExpense != null && latestRevenue != null && operatingExpense / latestRevenue >= 0.2) {
    const opexRatio = (operatingExpense / latestRevenue) * 100;
    const low = operatingExpense * 0.05;
    const high = operatingExpense * 0.1;
    push({
      category: 'Margin Expansion Opportunities',
      title: 'SG&A rationalization against the current revenue base',
      why: `Operating expense appears elevated relative to the current scale of the business. With EBITDA margin already at ${latestEbitdaMargin?.toFixed(1) || 'N/A'}%, the next leg of value creation is likely to come from tightening overhead, eliminating owner-era spend, and imposing a cleaner budgeting cadence rather than relying solely on incremental revenue.`,
      signals: [
        `Operating expenses are ${opexRatio.toFixed(1)}% of revenue`,
        latestEbitdaMargin != null ? `EBITDA margin: ${latestEbitdaMargin.toFixed(1)}%` : null,
        latestRevenue != null ? `Trailing revenue: $${latestRevenue.toFixed(1)}M` : null,
      ],
      impactLow: low,
      impactHigh: high,
      confidence: missingItems.some(item => item.text.includes('Income Statement')) ? 'low' : 'medium',
      priority: 2,
      assumptions: [
        'Assumes 5% to 10% of operating expense can be removed without impairing growth.',
        'Assumes identified savings are overhead or owner-era costs rather than frontline selling capacity.',
      ],
      evidenceDocTypes: [DOC_TYPES.INCOME_STATEMENT, DOC_TYPES.QOE_REPORT],
      executionWindow: 'quick win',
    });
  }

  if (latestRevenue != null && latestGrossMargin != null && firstGrossMargin != null && latestGrossMargin > firstGrossMargin) {
    const low = latestRevenue * 0.005;
    const high = latestRevenue * 0.01;
    push({
      category: 'Operational Improvements',
      title: 'Systematize procurement and gross-margin discipline',
      why: `Gross margin has been moving in the right direction, which suggests management has already demonstrated some pricing or procurement control. A buyer can formalize that playbook post-close through vendor consolidation, tighter quoting discipline, and product/customer profitability reporting.`,
      signals: [
        `Gross margin expanded from ${firstGrossMargin.toFixed(1)}% to ${latestGrossMargin.toFixed(1)}%`,
        cogs != null ? `Current COGS base: $${cogs.toFixed(1)}M` : null,
      ],
      impactLow: low,
      impactHigh: high,
      confidence: 'medium',
      priority: 3,
      assumptions: [
        'Impact assumes another 50 to 100 bps of gross-margin improvement on the current revenue base.',
        'Assumes gains come from procurement, quoting discipline, and customer/product mix management rather than one-off supplier concessions.',
      ],
      evidenceDocTypes: [DOC_TYPES.INCOME_STATEMENT],
      executionWindow: 'post-close initiative',
    });
  }

  if (earningsQuality.ownerCompAboveMarket != null && latestEbitda != null) {
    const uplift = earningsQuality.ownerCompAboveMarket / 1000000;
    push({
      category: 'Strategic / Acquisition Levers',
      title: 'Capture immediate EBITDA lift through owner-cost normalization',
      why: `Reported profitability appears to include seller-specific compensation structure. For a buy-side process, this is one of the clearest and most underwritable sources of EBITDA uplift because it does not depend on commercial outperformance.`,
      signals: [
        `Owner compensation above market: ~$${Math.round(earningsQuality.ownerCompAboveMarket / 1000)}K`,
        `Current EBITDA: $${latestEbitda.toFixed(2)}M`,
      ],
      impactLow: uplift * 0.9,
      impactHigh: uplift,
      confidence: 'high',
      priority: 4,
      assumptions: [
        'Assumes the identified owner compensation is genuinely non-recurring and can be normalized post-close.',
        'Assumes no like-for-like replacement hire is needed beyond the market-rate replacement cost already embedded in the QoE adjustment.',
      ],
      evidenceDocTypes: [DOC_TYPES.QOE_REPORT, DOC_TYPES.TAX_RETURN],
      executionWindow: 'quick win',
    });
  }

  if (arDragPct != null && arDragPct >= 10 && latestRevenue != null) {
    const low = latestRevenue * 0.002;
    const high = latestRevenue * 0.006;
    push({
      category: 'Operational Improvements',
      title: 'Tighten collections and order-to-cash execution',
      why: `Receivables aging suggests a meaningful portion of the book is drifting past 30 days. The direct EBITDA impact is smaller than the cash impact, but better collections discipline usually reduces bad-debt leakage, billing rework, and commercial discounting.`,
      signals: [
        `${arDragPct.toFixed(1)}% of AR is older than 30 days`,
        totalAr != null ? `Total AR balance: $${(totalAr / 1000000).toFixed(2)}M` : null,
        workingCapital?.netWorkingCapital ? `Latest net working capital: $${lastNonNull(workingCapital.netWorkingCapital)?.toFixed(1)}M` : null,
      ],
      impactLow: low,
      impactHigh: high,
      confidence: 'medium',
      priority: 5,
      assumptions: [
        'Impact assumes lower bad debt and reduced revenue leakage rather than a pure cash-only benefit.',
        'Assumes order-to-cash cleanup modestly reduces write-offs, credits, and billing friction.',
      ],
      evidenceDocTypes: [DOC_TYPES.AR_AGING, DOC_TYPES.BALANCE_SHEET],
      executionWindow: 'quick win',
    });
  }

  if (forecastData.gap != null && forecastData.gap > 2 && latestRevenue != null) {
    const underwritableGrowth = Math.min(forecastData.gap, 3);
    const low = latestRevenue * 0.01 * (latestEbitdaMargin || 15) / 100;
    const high = latestRevenue * (underwritableGrowth / 100) * (latestEbitdaMargin || 15) / 100;
    push({
      category: 'Strategic / Acquisition Levers',
      title: 'Underwrite only the portion of management growth that is supportable',
      why: `Management is projecting growth ahead of historical performance. That gap can be reframed as a value-creation opportunity for a disciplined buyer: underwrite only the portion backed by historical execution, then treat the excess as upside rather than base-case value.`,
      signals: [
        `Projected growth exceeds historical CAGR by ${forecastData.gap.toFixed(1)}pp`,
        historicalGrowth != null ? `Historical CAGR: ${historicalGrowth.toFixed(1)}%` : null,
        forecastData.projectedGrowth != null ? `Projected growth: ${forecastData.projectedGrowth.toFixed(1)}%` : null,
      ],
      impactLow: low,
      impactHigh: high,
      confidence: 'low',
      priority: 6,
      assumptions: [
        'This is an underwriting lever more than an operating certainty; confidence depends on pipeline support not currently in the file.',
        'Impact assumes only 1% to 3% of forecasted revenue growth is ultimately underwritten into the base plan.',
      ],
      evidenceDocTypes: [DOC_TYPES.PROJECTIONS, DOC_TYPES.INCOME_STATEMENT],
      executionWindow: 'post-close initiative',
    });
  }

  const ranked = opportunities
    .sort((a, b) => (a.priority - b.priority) || (b.midpointImpact - a.midpointImpact))
    .slice(0, 6);

  const categories = [
    'Revenue Growth Opportunities',
    'Margin Expansion Opportunities',
    'Operational Improvements',
    'Strategic / Acquisition Levers',
  ].map((category) => ({
    name: category,
    items: ranked.filter((item) => item.category === category),
  }));

  return {
    summary: buildGrowthSummary(companyName, overall, ranked),
    topOpportunities: ranked.slice(0, 6),
    categories,
    quickWins: ranked.filter((item) => item.executionWindow === 'quick win'),
    postCloseInitiatives: ranked.filter((item) => item.executionWindow === 'post-close initiative'),
  };
}

function buildGrowthSummary(companyName, overall, opportunities) {
  if (opportunities.length === 0) {
    return `${companyName} does not yet have enough structured data to support a credible value-creation plan. Additional operating detail is required before underwriting growth or margin expansion.`;
  }

  return `${companyName} shows ${opportunities.length} data-backed value-creation levers worth underwriting. The most credible path appears to be a mix of selective commercial expansion, overhead discipline, and acquisition-specific EBITDA normalization rather than a heroic revenue acceleration case. Overall underwriting confidence is ${overall.confidence}.`;
}

function formatImpactRange(low, high) {
  if (low == null && high == null) return 'Impact not estimable with current data';
  if (high == null || Math.abs(high - low) < 0.02) return `~$${(low || high).toFixed(2)}M EBITDA`;
  return `$${low.toFixed(2)}M-$${high.toFixed(2)}M EBITDA`;
}

function normalizeAssumptions(assumptions) {
  if (!assumptions) return [];
  return Array.isArray(assumptions) ? assumptions : [assumptions];
}

function mapEvidenceDocs(docTypes, availableDocTypes = []) {
  const available = new Set(availableDocTypes || []);
  return (docTypes || []).map((docType) => ({
    type: docType,
    label: DOC_TYPE_LABELS[docType] || docType,
    provided: available.has(docType),
  }));
}

function getAttractiveness(score, confidence, highRiskCount, missingCount) {
  if (score >= 72 && highRiskCount <= 1 && missingCount === 0 && confidence === 'high') {
    return 'Attractive for deeper diligence';
  }
  if (score >= 58) {
    return 'Potentially attractive, but only with targeted diligence';
  }
  return 'Not yet attractive for a clean pursuit';
}

function buildAdviceSummary(score, confidence, highRiskCount, missingCount) {
  if (score >= 72 && highRiskCount <= 1 && missingCount === 0) {
    return `The target appears financeable enough to justify deeper diligence, but the recommendation assumes current findings hold up under third-party verification. Confidence is ${confidence}.`;
  }
  if (score >= 58) {
    return `The opportunity is actionable for a search-fund buyer, but valuation and structure should remain conditional until the main risk items and document gaps are closed. Confidence is ${confidence}.`;
  }
  return `The current file supports caution rather than speed. A buyer should avoid a clean LOI until the major financial and diligence gaps are resolved. Confidence is ${confidence}.`;
}

const STEP_LIBRARY = [
  { trigger: () => true,
    title: 'Commission Quality of Earnings (QoE)',
    desc: 'Engage a third-party accounting firm to validate adjusted EBITDA, normalize owner comp, and stress-test working capital assumptions.' },
  { trigger: (_, dims) => (dims.find(d => d.key === 'concentration')?.score ?? 65) < 70,
    title: 'Customer Concentration Due Diligence',
    desc: 'Request detailed contract terms for top customers. Assess renewal risk, switching costs, and historical churn. Consider holdback or earnout tied to key account retention.' },
  { trigger: (_, dims) => (dims.find(d => d.key === 'leverage')?.score ?? 65) < 70,
    title: 'Debt Capacity & Financing Structure',
    desc: 'Model acquisition financing at target leverage. Explore SBA 7(a) or mezzanine tranches. Sensitivity-test debt service coverage under downside scenarios.' },
  { trigger: () => true,
    title: 'Management Transition Assessment',
    desc: 'Evaluate owner dependency. If owner is critical to key relationships, negotiate a 12–24 month transition period with incentive alignment.' },
  { trigger: () => true,
    title: 'Preliminary LOI & Deal Structuring',
    desc: 'Draft LOI with seller note and earnout tied to revenue retention and EBITDA targets over 24 months post-close.' },
  { trigger: (_, dims) => (dims.find(d => d.key === 'forecastCredibility')?.score ?? 65) < 65,
    title: 'Validate Management Projections',
    desc: 'Scrutinize forward assumptions against historical performance. Request bottom-up revenue build and customer pipeline data to substantiate growth claims.' },
  { trigger: (_, dims) => (dims.find(d => d.key === 'cashConversion')?.score ?? 65) < 70,
    title: 'Cash Flow Deep Dive',
    desc: 'Analyze working capital cycle, capex requirements, and one-time items affecting cash flow. Determine sustainable free cash flow for debt service.' },
  { trigger: (_, dims) => (dims.find(d => d.key === 'profitability')?.score ?? 65) < 65,
    title: 'Operational Improvement Assessment',
    desc: 'Identify margin expansion opportunities through cost rationalization, procurement optimization, or pricing strategy adjustments post-acquisition.' },
];

function generateNextSteps(risks, dimensions) {
  return STEP_LIBRARY
    .filter(s => s.trigger(risks, dimensions))
    .slice(0, 6)
    .map(s => ({ title: s.title, desc: s.desc }));
}

function buildDataQualitySummary(results, validation, reconciliation = null, evidenceBundle = {}) {
  const usableResults = results.filter((result) => result.usable);
  const avgConfidence = usableResults.length > 0
    ? usableResults.reduce((sum, result) => sum + (result.confidence || 0), 0) / usableResults.length
    : 0;

  const extractionDocuments = usableResults
    .slice()
    .sort((left, right) => (right.confidence || 0) - (left.confidence || 0))
    .map((result) => ({
      docType: result.docType,
      label: DOC_TYPE_LABELS[result.docType] || result.docType,
      sourceFileName: result.sourceFileName || null,
      sourceSheetName: result.sourceSheetName || null,
      confidencePct: Math.round((result.confidence || 0) * 100),
      confidenceLabel: toConfidenceLabel(result.confidence || 0),
      source: result.synthetic ? 'modeled fallback' : 'normalized upload',
      missingFields: Array.isArray(result.coverage?.missing) ? result.coverage.missing.slice(0, 4) : [],
      warningCount: Array.isArray(result.warnings) ? result.warnings.length : 0,
      confidenceDecomposition: buildDocumentConfidenceDecomposition(result, reconciliation, evidenceBundle.reviewerSignals || null),
      interpretability: {
        mappedCount: result.interpretability?.mappedCount || 0,
        derivedCount: result.interpretability?.derivedCount || 0,
        ambiguousCount: result.interpretability?.ambiguousCount || 0,
        unmappedCount: result.interpretability?.unmappedCount || 0,
        lowConfidenceCount: result.interpretability?.lowConfidenceCount || 0,
        exactMatchCount: result.interpretability?.exactMatchCount || 0,
        heuristicMatchCount: result.interpretability?.heuristicMatchCount || 0,
        manualMatchCount: result.interpretability?.manualMatchCount || 0,
        needsReview: Boolean(result.interpretability?.needsReview),
        reviewPriority: result.interpretability?.reviewPriority || 'none',
        recommendations: result.interpretability?.recommendations || [],
      },
      provenancePreview: buildProvenancePreview(result.provenance),
      reviewPacket: buildReviewPacket(result, reconciliation, evidenceBundle.reviewerSignals || null),
    }));

  const extractionMissing = extractionDocuments
    .filter((document) => document.missingFields.length > 0)
    .map((document) => ({
      message: `${document.label} is missing mapped fields: ${document.missingFields.join(', ')}.`,
      impact: document.missingFields.length >= 3 ? 'medium' : 'low',
      docType: document.docType,
    }));

  return {
    extractionConfidence: {
      averagePct: Math.round(avgConfidence * 100),
      averageLabel: toConfidenceLabel(avgConfidence),
      documentCount: extractionDocuments.length,
      modeledFallbacks: extractionDocuments.filter((document) => document.source === 'modeled fallback').length,
    },
    documents: extractionDocuments,
    ambiguityHighlights: buildAmbiguityHighlights(results),
    validationStatus: validation.status,
    validationWarnings: validation.warnings || [],
    hardErrors: validation.hardErrors || [],
    missingDataNotes: [...(validation.missingDataNotes || []), ...extractionMissing].slice(0, 10),
    confidenceAdjustment: validation.confidenceAdjustment,
    summary: validation.summary,
    reconciliation: reconciliation || validation.reconciliation || null,
    evidenceResolution: evidenceBundle.evidenceResolution || null,
    temporalAlignment: evidenceBundle.temporalAlignment || null,
    entityResolution: evidenceBundle.entityResolution || null,
    ambiguityWorkflows: evidenceBundle.ambiguityWorkflows || null,
    reviewerSignals: evidenceBundle.reviewerSignals || null,
    assumptionLedger: buildAssumptionLedger(results, validation, reconciliation, evidenceBundle),
    confidenceRecommendations: buildConfidenceRecommendations(results, validation, reconciliation, evidenceBundle),
  };
}

// ============================================================
// Utilities
// ============================================================

function toMillions(val) {
  if (val == null) return null;
  return Math.round(val / 100000) / 10;
}

function lastNonNull(arr) {
  if (!arr) return null;
  for (let i = arr.length - 1; i >= 0; i--) if (arr[i] != null) return arr[i];
  return null;
}

function firstNonNull(arr) {
  if (!arr) return null;
  for (let i = 0; i < arr.length; i++) if (arr[i] != null) return arr[i];
  return null;
}

function round(n, decimals = 2) {
  const f = Math.pow(10, decimals);
  return Math.round(n * f) / f;
}

function toConfidenceLabel(value) {
  if (value >= 0.8) return 'high';
  if (value >= 0.55) return 'medium';
  if (value > 0) return 'low';
  return 'none';
}

function buildProvenancePreview(provenance) {
  if (!provenance) return null;
  const mappedExamples = (provenance.mappedRows || []).slice(0, 3).map((entry) => ({
    fieldName: entry.fieldName,
    rowLabel: entry.rowLabel,
    period: entry.period,
    sourceType: entry.sourceType,
  }));

  return {
    mappedExamples,
    ambiguousRows: (provenance.ambiguousRows || []).slice(0, 2),
    unmappedRows: (provenance.unmappedRows || []).slice(0, 3),
    lowConfidenceRows: (provenance.lowConfidenceRows || []).slice(0, 2),
    derivedFields: (provenance.derivedFields || []).slice(0, 3),
  };
}

function buildReviewPacket(result, reconciliation = null, reviewerSignals = null) {
  const provenance = result.provenance || {};

  return {
    sourceFileName: result.sourceFileName || null,
    sourceSheetName: result.sourceSheetName || null,
    confidenceDecomposition: buildDocumentConfidenceDecomposition(result, reconciliation, reviewerSignals),
    mappedRows: prioritizeManualMappings(provenance.mappedRows || []).slice(0, 14).map((entry) => ({
      fieldName: entry.fieldName,
      rowLabel: entry.rowLabel,
      period: entry.period,
      sourceType: entry.sourceType,
      matchAlias: entry.matchAlias,
      matchType: entry.matchType,
      matchScore: entry.matchScore,
      matchConfidence: entry.matchConfidence,
    })),
    derivedFields: (provenance.derivedFields || []).slice(0, 10).map((entry) => ({
      fieldName: entry.fieldName,
      period: entry.period,
      note: entry.note,
    })),
    ambiguousRows: (provenance.ambiguousRows || []).slice(0, 8).map((entry) => ({
      rowLabel: entry.rowLabel,
      rowIndex: entry.rowIndex,
      candidates: entry.candidates || [],
      reviewType: 'mapping_choice',
      suggestedAction: 'Choose the correct schema field or ignore the row if it is subtotal noise.',
    })),
    unmappedRows: (provenance.unmappedRows || []).slice(0, 10).map((entry) => ({
      rowLabel: entry.rowLabel,
      rowIndex: entry.rowIndex,
      reviewType: 'unmapped_materiality',
      suggestedAction: 'Map the row if it is economically material; otherwise ignore it as non-operating noise.',
    })),
    lowConfidenceRows: (provenance.lowConfidenceRows || []).slice(0, 10).map((entry) => ({
      rowLabel: entry.rowLabel,
      rowIndex: entry.rowIndex,
      fieldName: entry.fieldName,
      alias: entry.alias,
      score: entry.score,
      matchType: entry.matchType,
      reviewType: 'heuristic_confirmation',
      suggestedAction: 'Confirm or override the heuristic mapping before trusting the normalized field.',
    })),
    warnings: (result.warnings || []).slice(0, 6),
    missingFields: (result.coverage?.missing || []).slice(0, 8),
    recommendations: result.interpretability?.recommendations || [],
  };
}

function buildAmbiguityHighlights(results) {
  return results
    .filter((result) => result.provenance?.ambiguousRows?.length || result.provenance?.unmappedRows?.length || result.provenance?.lowConfidenceRows?.length)
    .slice(0, 6)
    .map((result) => ({
      docType: result.docType,
      label: DOC_TYPE_LABELS[result.docType] || result.docType,
      ambiguousRows: (result.provenance?.ambiguousRows || []).slice(0, 2),
      unmappedRows: (result.provenance?.unmappedRows || []).slice(0, 3),
      lowConfidenceRows: (result.provenance?.lowConfidenceRows || []).slice(0, 2),
    }));
}

function prioritizeManualMappings(rows = []) {
  return [...rows].sort((left, right) => {
    const leftScore = left.sourceType === 'manual_override' ? 0 : 1;
    const rightScore = right.sourceType === 'manual_override' ? 0 : 1;
    return leftScore - rightScore;
  });
}

function buildAssumptionLedger(results, validation, reconciliation = null, evidenceBundle = {}) {
  const entries = [];

  results.forEach((result, index) => {
    const docLabel = DOC_TYPE_LABELS[result.docType] || result.docType;
    const sourceMetadata = result.sourceMetadata || {};
    const sourceRef = formatSourceRef(result);

    if (sourceMetadata.ocrApplied) {
      entries.push({
        id: `${result.docType}-ocr-${index}`,
        category: 'ocr',
        severity: 'medium',
        title: `${docLabel} uses OCR-derived text`,
        detail: `${sourceRef} required OCR because native PDF text was unavailable. OCR output should be treated as lower-confidence than digital exports.`,
        confidenceImpact: 'medium',
      });
    }

    if ((sourceMetadata.layoutMetadata?.columnCount || 1) > 1) {
      entries.push({
        id: `${result.docType}-layout-columns-${index}`,
        category: 'layout',
        severity: 'low',
        title: `${docLabel} was parsed from a multi-column page`,
        detail: `${sourceRef} required column-order reconstruction across ${sourceMetadata.layoutMetadata.columnCount} PDF columns.`,
        confidenceImpact: 'low',
      });
    }

    if ((sourceMetadata.layoutMetadata?.footnotes || []).length > 0) {
      entries.push({
        id: `${result.docType}-footnotes-${index}`,
        category: 'footnotes',
        severity: 'low',
        title: `${docLabel} includes extracted footnotes`,
        detail: `${sourceRef} surfaced ${sourceMetadata.layoutMetadata.footnotes.length} footnote-style line${sourceMetadata.layoutMetadata.footnotes.length === 1 ? '' : 's'} that may contain unit or period qualifiers.`,
        confidenceImpact: 'low',
      });
    }

    if (sourceMetadata.valueScale && sourceMetadata.valueScale !== 1) {
      entries.push({
        id: `${result.docType}-scale-${index}`,
        category: 'units',
        severity: 'low',
        title: `${docLabel} values were scaled from source units`,
        detail: `${sourceRef} was normalized using a ${sourceMetadata.valueScale.toLocaleString()}x value scale inferred from labels like "in thousands" or "in millions".`,
        confidenceImpact: 'low',
      });
    }

    if (sourceMetadata.sourceKind === 'sheet-section') {
      entries.push({
        id: `${result.docType}-segment-${index}`,
        category: 'segmentation',
        severity: 'low',
        title: `${docLabel} was split from a combined sheet`,
        detail: `${sourceRef} came from a section-level split of a larger workbook tab. Review the section boundary if totals appear incomplete.`,
        confidenceImpact: 'low',
      });
    }

    (result.provenance?.derivedFields || []).slice(0, 6).forEach((entry, derivedIndex) => {
      entries.push({
        id: `${result.docType}-derived-${index}-${derivedIndex}`,
        category: 'derived',
        severity: 'low',
        title: `${docLabel} derived ${humanizeFieldName(entry.fieldName)}`,
        detail: `${sourceRef} derived ${humanizeFieldName(entry.fieldName)}${entry.period && entry.period !== '_single' ? ` for ${entry.period}` : ''}: ${entry.note}`,
        confidenceImpact: 'low',
      });
    });

    (result.provenance?.lowConfidenceRows || []).slice(0, 4).forEach((entry, heuristicIndex) => {
      entries.push({
        id: `${result.docType}-heuristic-${index}-${heuristicIndex}`,
        category: 'heuristic_mapping',
        severity: 'medium',
        title: `${docLabel} accepted a heuristic row match`,
        detail: `${sourceRef} mapped "${entry.rowLabel}" to ${humanizeFieldName(entry.fieldName)} using a ${Math.round((entry.score || 0) * 100)}% ${entry.matchType || 'heuristic'} match.`,
        confidenceImpact: 'medium',
      });
    });
  });

  (reconciliation?.findings || []).forEach((finding, index) => {
    entries.push({
      id: `reconciliation-${index}`,
      category: 'reconciliation',
      severity: finding.severity === 'hard_error' ? 'high' : finding.severity === 'warning' ? 'medium' : 'low',
      title: finding.label || 'Cross-document reconciliation finding',
      detail: finding.message,
      confidenceImpact: finding.severity === 'hard_error' ? 'high' : finding.severity === 'warning' ? 'medium' : 'low',
    });
  });

  (evidenceBundle.evidenceResolution?.conflicts || []).slice(0, 6).forEach((conflict, index) => {
    entries.push({
      id: `evidence-conflict-${index}`,
      category: 'evidence_resolution',
      severity: conflict.severity === 'high' ? 'high' : conflict.severity === 'medium' ? 'medium' : 'low',
      title: conflict.label,
      detail: conflict.summary,
      confidenceImpact: conflict.severity === 'high' ? 'high' : 'medium',
    });
  });

  (evidenceBundle.temporalAlignment?.conflicts || []).slice(0, 4).forEach((conflict, index) => {
    entries.push({
      id: `timeline-${index}`,
      category: 'time_alignment',
      severity: conflict.severity === 'high' ? 'high' : 'medium',
      title: conflict.label,
      detail: conflict.summary,
      confidenceImpact: conflict.severity === 'high' ? 'high' : 'medium',
    });
  });

  if ((evidenceBundle.reviewerSignals?.ruleCount || 0) > 0) {
    entries.push({
      id: 'reviewer-memory',
      category: 'reviewer_memory',
      severity: 'low',
      title: 'Evidence ranking is using persisted reviewer memory',
      detail: evidenceBundle.reviewerSignals.summary,
      confidenceImpact: 'low',
    });
  }

  (evidenceBundle.reviewerSignals?.noisyLabels || []).slice(0, 3).forEach((entry, index) => {
    entries.push({
      id: `reviewer-noise-${index}`,
      category: 'reviewer_memory',
      severity: 'medium',
      title: `${humanizeFieldName(entry.docType)} label treated as likely noise`,
      detail: `"${entry.rowLabel}" has been ignored in prior review decisions and is now de-prioritized unless explicitly remapped.`,
      confidenceImpact: 'medium',
    });
  });

  return entries.slice(0, 18);
}

function buildConfidenceRecommendations(results, validation, reconciliation = null, evidenceBundle = {}) {
  const recommendations = [];
  const docTypes = new Set(results.filter((result) => result.usable).map((result) => result.docType));

  const requestedDocs = [
    [DOC_TYPES.INCOME_STATEMENT, 'Upload a clean income statement export with explicit period headers.'],
    [DOC_TYPES.BALANCE_SHEET, 'Upload a balance sheet with total assets, liabilities, and equity on the same basis date.'],
    [DOC_TYPES.CASH_FLOW_STATEMENT, 'Upload a cash flow statement to improve cash-conversion confidence.'],
    [DOC_TYPES.QOE_REPORT, 'Upload the QoE summary to support adjusted EBITDA and add-back review.'],
    [DOC_TYPES.DEBT_SCHEDULE, 'Upload the debt schedule to tighten leverage and debt-service analysis.'],
    [DOC_TYPES.REVENUE_BREAKDOWN, 'Upload customer or service-line concentration detail to support concentration scoring.'],
  ];

  requestedDocs.forEach(([docType, action], index) => {
    if (docTypes.has(docType)) return;
    recommendations.push({
      id: `missing-doc-${index}`,
      priority: docType === DOC_TYPES.INCOME_STATEMENT || docType === DOC_TYPES.BALANCE_SHEET ? 'high' : 'medium',
      title: `Add ${DOC_TYPE_LABELS[docType] || docType}`,
      action,
      rationale: `${DOC_TYPE_LABELS[docType] || docType} is missing from the normalized document set.`,
      expectedLift: docType === DOC_TYPES.INCOME_STATEMENT || docType === DOC_TYPES.BALANCE_SHEET ? 'large' : 'moderate',
    });
  });

  results.forEach((result, index) => {
    const docLabel = DOC_TYPE_LABELS[result.docType] || result.docType;
    const sourceRef = formatSourceRef(result);

    if ((result.provenance?.ambiguousRows?.length || 0) > 0 || (result.provenance?.unmappedRows?.length || 0) > 0) {
      recommendations.push({
        id: `mapping-review-${index}`,
        priority: 'high',
        title: `Resolve open row mapping issues in ${docLabel}`,
        action: `Use the review panel to map or ignore ambiguous/unmapped rows from ${sourceRef}.`,
        rationale: `${(result.provenance?.ambiguousRows?.length || 0) + (result.provenance?.unmappedRows?.length || 0)} row-level issues are suppressing confidence.`,
        expectedLift: 'moderate',
      });
    }

    if ((result.provenance?.lowConfidenceRows?.length || 0) > 0) {
      recommendations.push({
        id: `heuristic-review-${index}`,
        priority: 'medium',
        title: `Confirm heuristic matches in ${docLabel}`,
        action: `Spot-check heuristic mappings from ${sourceRef} and convert the correct ones into explicit learned aliases.`,
        rationale: `${result.provenance.lowConfidenceRows.length} fields were accepted through weak similarity rather than exact label matches.`,
        expectedLift: 'moderate',
      });
    }

    if (result.sourceMetadata?.ocrApplied) {
      recommendations.push({
        id: `ocr-upgrade-${index}`,
        priority: 'medium',
        title: `Replace OCR-derived ${docLabel} with a native export`,
        action: `Upload an Excel/CSV export or a digital PDF for ${sourceRef} instead of a scanned page.`,
        rationale: 'OCR text is usable, but materially less reliable for row labels, periods, and units.',
        expectedLift: 'moderate',
      });
    }
  });

  (reconciliation?.findings || [])
    .filter((finding) => finding.severity === 'warning' || finding.severity === 'hard_error')
    .slice(0, 4)
    .forEach((finding, index) => {
      recommendations.push({
        id: `reconciliation-${index}`,
        priority: finding.severity === 'hard_error' ? 'high' : 'medium',
        title: finding.label || 'Resolve reconciliation gap',
        action: `Upload the support needed to reconcile this conflict and confirm that periods and units match.`,
        rationale: finding.message,
        expectedLift: finding.severity === 'hard_error' ? 'large' : 'moderate',
      });
    });

  (validation?.missingDataNotes || []).slice(0, 4).forEach((note, index) => {
    recommendations.push({
      id: `missing-note-${index}`,
      priority: note.impact === 'high' ? 'high' : note.impact === 'medium' ? 'medium' : 'low',
      title: 'Close a data-quality gap',
      action: note.message,
      rationale: 'Validation identified this missing support as a driver of reduced confidence.',
      expectedLift: note.impact === 'high' ? 'large' : 'moderate',
    });
  });

  (evidenceBundle.evidenceResolution?.conflicts || []).slice(0, 4).forEach((conflict, index) => {
    recommendations.push({
      id: `evidence-conflict-${index}`,
      priority: conflict.severity === 'high' ? 'high' : 'medium',
      title: `Resolve ${conflict.label.toLowerCase()}`,
      action: conflict.recommendedAction,
      rationale: conflict.summary,
      expectedLift: conflict.severity === 'high' ? 'large' : 'moderate',
    });
  });

  (evidenceBundle.entityResolution?.ambiguousClusters || []).slice(0, 3).forEach((cluster, index) => {
    recommendations.push({
      id: `entity-resolution-${index}`,
      priority: 'medium',
      title: `Confirm ${cluster.canonicalName} entity aliases`,
      action: 'Review the clustered aliases and confirm whether they refer to the same real-world entity.',
      rationale: cluster.summary,
      expectedLift: 'moderate',
    });
  });

  if ((evidenceBundle.reviewerSignals?.ruleCount || 0) === 0) {
    recommendations.push({
      id: 'reviewer-memory-bootstrap',
      priority: 'low',
      title: 'Seed reviewer memory with a few row decisions',
      action: 'Use the review panel to explicitly map or ignore the highest-priority ambiguous rows so future evidence ranking can learn trustworthy document families and noisy labels.',
      rationale: 'No persisted reviewer decisions are currently available to refine evidence ranking beyond default priors.',
      expectedLift: 'moderate',
    });
  }

  (evidenceBundle.reviewerSignals?.noisySheets || []).slice(0, 2).forEach((entry, index) => {
    recommendations.push({
      id: `noisy-sheet-${index}`,
      priority: 'medium',
      title: `Review noisy section in ${DOC_TYPE_LABELS[entry.docType] || entry.docType}`,
      action: `Check ${entry.sheetName} and confirm whether subtotal or memo rows should keep being ignored.`,
      rationale: `Reviewer history shows a ${Math.round((entry.noiseRatio || 0) * 100)}% ignore rate for this section, suggesting structural noise.`,
      expectedLift: 'moderate',
    });
  });

  return dedupeConfidenceRecommendations(recommendations).slice(0, 10);
}

function dedupeConfidenceRecommendations(recommendations) {
  const seen = new Set();
  return recommendations.filter((recommendation) => {
    const key = `${recommendation.title}::${recommendation.action}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  }).sort((left, right) => recommendationPriorityScore(left.priority) - recommendationPriorityScore(right.priority));
}

function recommendationPriorityScore(priority) {
  if (priority === 'high') return 0;
  if (priority === 'medium') return 1;
  return 2;
}

function formatSourceRef(result) {
  const meta = result.sourceMetadata || {};
  if (meta.pageNumber) return `page ${meta.pageNumber}`;
  if (meta.segmentLabel) return meta.segmentLabel;
  if (result.sourceSheetName) return result.sourceSheetName;
  if (result.sourceFileName) return result.sourceFileName;
  return DOC_TYPE_LABELS[result.docType] || result.docType;
}

function humanizeFieldName(value = '') {
  return String(value)
    .replace(/^__/, '')
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (match) => match.toUpperCase());
}
