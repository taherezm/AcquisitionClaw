import { DOC_TYPES, FIELD_SCHEMAS } from '../../../ingestion/schemas.js';
import { COLUMN_ALIASES, DOCUMENT_FIELD_ALIASES } from './fieldAliases.js';
import { mergeLearnedAliases } from './learnedAliasService.js';
import { createProvenanceTracker, finalizeExtraction, getFieldMatchCandidates, matchFieldByAliases, normalizeLabel, round, toDateValue, toNumericValue, toPercentageValue } from './mappingUtils.js';
import { describePeriodKey, detectPeriodColumns, inferFallbackPeriod, sortPeriodKeys } from './periodUtils.js';

const MIN_FIELD_MATCH_SCORE = 0.6;
const LOW_CONFIDENCE_MATCH_SCORE = 0.78;
const AMBIGUITY_SCORE_GAP = 0.08;

export function mapSheetToSchema({ docType, sheet, fileName = '', learnedAliasRules = [] }) {
  if (!sheet) {
    return {
      periods: [],
      data: {},
      coverage: { total: 0, found: 0, missing: [], percentage: 0 },
      missingFields: [],
      mappingConfidence: 0,
      usable: false,
      warnings: ['No parsed sheet was selected for schema mapping.'],
    };
  }

  switch (docType) {
    case DOC_TYPES.INCOME_STATEMENT:
      return mapIncomeStatement(sheet, fileName, learnedAliasRules);
    case DOC_TYPES.BALANCE_SHEET:
      return finalizeWithDerivations(docType, mapMatrixDocument(docType, sheet, fileName, learnedAliasRules), 0);
    case DOC_TYPES.CASH_FLOW_STATEMENT:
      return mapCashFlow(sheet, fileName, learnedAliasRules);
    case DOC_TYPES.AR_AGING:
      return mapAgingSheet(docType, sheet, learnedAliasRules);
    case DOC_TYPES.AP_AGING:
      return mapAgingSheet(docType, sheet, learnedAliasRules);
    case DOC_TYPES.DEBT_SCHEDULE:
      return mapDebtSchedule(sheet, learnedAliasRules);
    case DOC_TYPES.REVENUE_BREAKDOWN:
      return mapRevenueConcentration(sheet, learnedAliasRules);
    case DOC_TYPES.PROJECTIONS:
      return mapProjections(sheet, fileName, learnedAliasRules);
    case DOC_TYPES.TAX_RETURN:
      return mapTaxReturn(sheet, fileName, learnedAliasRules);
    case DOC_TYPES.QOE_REPORT:
      return mapQoeSummary(sheet, fileName, learnedAliasRules);
    default:
      return {
        periods: [],
        data: {},
        coverage: { total: 0, found: 0, missing: [], percentage: 0 },
        missingFields: [],
        mappingConfidence: 0,
        usable: false,
        warnings: ['No schema mapper is defined for this document type.'],
      };
  }
}

function mapIncomeStatement(sheet, fileName, learnedAliasRules) {
  const result = mapMatrixDocument(DOC_TYPES.INCOME_STATEMENT, sheet, fileName, learnedAliasRules);
  let derivedFields = 0;

  result.periods.forEach((period) => {
    const periodData = result.data[period];
    if (!periodData) return;

    if (periodData.grossProfit == null && periodData.revenue != null && periodData.cogs != null) {
      periodData.grossProfit = round(periodData.revenue - periodData.cogs);
      result.warnings.push(`Derived gross profit for ${period} from revenue and COGS.`);
      addDerivedProvenance(result.provenance, 'grossProfit', period, 'Derived from revenue and COGS.');
      derivedFields += 1;
    }

    const combinedDa = periodData.__daCombined;
    const operatingProfit = periodData.__operatingProfit;
    const depreciation = periodData.depreciation ?? null;
    const amortization = periodData.amortization ?? null;
    const daValue = combinedDa ?? ((depreciation || 0) + (amortization || 0));
    if (periodData.ebitda == null && operatingProfit != null && daValue != null) {
      periodData.ebitda = round(operatingProfit + daValue);
      result.warnings.push(`Derived EBITDA for ${period} from operating profit plus D&A.`);
      addDerivedProvenance(result.provenance, 'ebitda', period, 'Derived from operating profit plus D&A.');
      derivedFields += 1;
    }

    if (periodData.grossMargin == null && periodData.revenue && periodData.grossProfit != null) {
      periodData.grossMargin = round((periodData.grossProfit / periodData.revenue) * 100);
      addDerivedProvenance(result.provenance, 'grossMargin', period, 'Derived from gross profit divided by revenue.');
      derivedFields += 1;
    }

    if (periodData.ebitdaMargin == null && periodData.revenue && periodData.ebitda != null) {
      periodData.ebitdaMargin = round((periodData.ebitda / periodData.revenue) * 100);
      addDerivedProvenance(result.provenance, 'ebitdaMargin', period, 'Derived from EBITDA divided by revenue.');
      derivedFields += 1;
    }

    if (periodData.netMargin == null && periodData.revenue && periodData.netIncome != null) {
      periodData.netMargin = round((periodData.netIncome / periodData.revenue) * 100);
      addDerivedProvenance(result.provenance, 'netMargin', period, 'Derived from net income divided by revenue.');
      derivedFields += 1;
    }

    cleanupIntermediateFields(periodData);
  });

  return finalizeWithDerivations(DOC_TYPES.INCOME_STATEMENT, result, derivedFields);
}

function mapCashFlow(sheet, fileName, learnedAliasRules) {
  const result = mapMatrixDocument(DOC_TYPES.CASH_FLOW_STATEMENT, sheet, fileName, learnedAliasRules);
  let derivedFields = 0;

  result.periods.forEach((period) => {
    const periodData = result.data[period];
    if (!periodData) return;

    if (periodData.freeCashFlow == null && periodData.operatingCashFlow != null && periodData.capex != null) {
      const capexImpact = periodData.capex < 0 ? periodData.capex : -periodData.capex;
      periodData.freeCashFlow = round(periodData.operatingCashFlow + capexImpact);
      result.warnings.push(`Derived free cash flow for ${period} from operating cash flow and capex.`);
      addDerivedProvenance(result.provenance, 'freeCashFlow', period, 'Derived from operating cash flow and capex.');
      derivedFields += 1;
    }
  });

  return finalizeWithDerivations(DOC_TYPES.CASH_FLOW_STATEMENT, result, derivedFields);
}

function mapProjections(sheet, fileName, learnedAliasRules) {
  const result = mapMatrixDocument(DOC_TYPES.PROJECTIONS, sheet, fileName, learnedAliasRules);
  let derivedFields = 0;

  result.periods.forEach((period) => {
    const periodData = result.data[period];
    if (!periodData) return;

    if (periodData.projectedEbitda == null && periodData.projectedRevenue != null && periodData.projectedMargin != null) {
      periodData.projectedEbitda = round(periodData.projectedRevenue * (periodData.projectedMargin / 100));
      addDerivedProvenance(result.provenance, 'projectedEbitda', period, 'Derived from projected revenue multiplied by projected EBITDA margin.');
      derivedFields += 1;
    }

    if (periodData.projectedMargin == null && periodData.projectedRevenue && periodData.projectedEbitda != null) {
      periodData.projectedMargin = round((periodData.projectedEbitda / periodData.projectedRevenue) * 100);
      addDerivedProvenance(result.provenance, 'projectedMargin', period, 'Derived from projected EBITDA divided by projected revenue.');
      derivedFields += 1;
    }
  });

  return finalizeWithDerivations(DOC_TYPES.PROJECTIONS, result, derivedFields);
}

function mapTaxReturn(sheet, fileName, learnedAliasRules) {
  const result = mapMatrixDocument(DOC_TYPES.TAX_RETURN, sheet, fileName, learnedAliasRules);
  let derivedFields = 0;

  result.periods.forEach((period) => {
    const periodData = result.data[period];
    if (!periodData) return;

    if (periodData.filingYear == null) {
      const filingYear = /^\d{4}$/.test(period) ? period : extractYearFromString(fileName);
      if (filingYear) {
        periodData.filingYear = filingYear;
        addDerivedProvenance(result.provenance, 'filingYear', period, 'Derived from file name or period label.');
        derivedFields += 1;
      }
    }
  });

  return finalizeWithDerivations(DOC_TYPES.TAX_RETURN, result, derivedFields);
}

function mapQoeSummary(sheet, fileName, learnedAliasRules) {
  const result = mapMatrixDocument(DOC_TYPES.QOE_REPORT, sheet, fileName, learnedAliasRules);
  let derivedFields = 0;

  result.periods.forEach((period) => {
    const periodData = result.data[period];
    if (!periodData) return;

    if (periodData.totalAddBacks == null && Array.isArray(periodData.addBacks) && periodData.addBacks.length > 0) {
      periodData.totalAddBacks = round(periodData.addBacks.reduce((sum, item) => sum + (item.amount || 0), 0));
      addDerivedProvenance(result.provenance, 'totalAddBacks', period, 'Derived from individual add-back rows.');
      derivedFields += 1;
    }
  });

  return finalizeWithDerivations(DOC_TYPES.QOE_REPORT, result, derivedFields);
}

function mapMatrixDocument(docType, sheet, fileName, learnedAliasRules = []) {
  const aliasMap = mergeLearnedAliases(DOCUMENT_FIELD_ALIASES[docType] || {}, docType, learnedAliasRules);
  const view = getMatrixSheetView(sheet);
  const valueScale = sheet.valueScale || 1;
  const labelColumn = view.labelColumn;
  const periodColumns = detectPeriodColumns(view.valueColumns);
  const periods = periodColumns.length > 0
    ? sortPeriodKeys(periodColumns.map((periodColumn) => periodColumn.periodKey))
    : [inferFallbackPeriod(sheet.name, fileName)];
  const periodMetadata = periods.map((period) => describePeriodKey(period));
  const data = Object.fromEntries(periods.map((period) => [period, {}]));
  const warnings = [];
  const provenance = createProvenanceTracker();
  let directMatches = 0;

  for (const record of view.records) {
    const rowLabel = record.values[labelColumn];
    const candidates = getFieldMatchCandidates(rowLabel, aliasMap);
    const chosenCandidate = candidates[0] || null;
    const fieldName = chosenCandidate?.score >= MIN_FIELD_MATCH_SCORE ? chosenCandidate.fieldName : null;
    if (!fieldName) {
      captureUnmappedRow(provenance, rowLabel, record.rowIndex);
      continue;
    }
    captureAmbiguousRow(provenance, rowLabel, record.rowIndex, candidates);
    captureLowConfidenceRow(provenance, rowLabel, record.rowIndex, chosenCandidate);

    if (periodColumns.length > 0) {
      periodColumns.forEach((periodColumn) => {
        const rawValue = record.values[periodColumn.columnKey];
        const mappedValue = coerceFieldValue(docType, fieldName, rawValue, valueScale);
        if (mappedValue == null) return;
        assignMappedValue(data[periodColumn.periodKey], fieldName, mappedValue, rowLabel, provenance, {
          rowIndex: record.rowIndex,
          period: periodColumn.periodKey,
          rawValue,
          match: chosenCandidate,
          candidates,
        });
      });
    } else {
      const fallbackValue = findFirstMappedValue(docType, fieldName, record, labelColumn, valueScale);
      if (fallbackValue != null) {
        assignMappedValue(data[periods[0]], fieldName, fallbackValue, rowLabel, provenance, {
          rowIndex: record.rowIndex,
          period: periods[0],
          rawValue: fallbackValue,
          match: chosenCandidate,
          candidates,
        });
      }
    }

    directMatches += 1;
  }

  return {
    docType,
    periods,
    data,
    directMatches,
    derivedFields: 0,
    provenance,
    periodMetadata,
    sourceMetadata: buildSheetSourceMetadata(sheet, periodMetadata),
    warnings,
  };
}

function mapAgingSheet(docType, sheet, learnedAliasRules = []) {
  const aliasMap = mergeLearnedAliases(DOCUMENT_FIELD_ALIASES[docType], docType, learnedAliasRules);
  const valueScale = sheet.valueScale || 1;
  const labelColumn = sheet.header[0];
  const data = { _single: {} };
  const warnings = [];
  const provenance = createProvenanceTracker();
  let directMatches = 0;

  for (const record of sheet.records) {
    const rowLabel = record.values[labelColumn];
    const candidates = getFieldMatchCandidates(rowLabel, aliasMap);
    const chosenCandidate = candidates[0] || null;
    const fieldName = chosenCandidate?.score >= MIN_FIELD_MATCH_SCORE ? chosenCandidate.fieldName : null;
    if (!fieldName) {
      captureUnmappedRow(provenance, rowLabel, record.rowIndex);
      continue;
    }
    captureAmbiguousRow(provenance, rowLabel, record.rowIndex, candidates);
    captureLowConfidenceRow(provenance, rowLabel, record.rowIndex, chosenCandidate);
    const mappedValue = findFirstMappedValue(docType, fieldName, record, labelColumn, valueScale);
    if (mappedValue == null) continue;
    data._single[fieldName] = mappedValue;
    addMappedProvenance(provenance, {
      fieldName,
      rowLabel,
      rowIndex: record.rowIndex,
      period: '_single',
      rawValue: mappedValue,
      match: chosenCandidate,
      candidates,
      sourceType: 'direct',
    });
    directMatches += 1;
  }

  if (data._single.totalAR == null && docType === DOC_TYPES.AR_AGING) {
    data._single.totalAR = sumAgingBuckets(data._single);
    addDerivedProvenance(provenance, 'totalAR', '_single', 'Derived from aging buckets.');
  }
  if (data._single.totalAP == null && docType === DOC_TYPES.AP_AGING) {
    data._single.totalAP = sumAgingBuckets(data._single);
    addDerivedProvenance(provenance, 'totalAP', '_single', 'Derived from aging buckets.');
  }

  if (directMatches === 0 || countPresentFields(data._single) < 2) {
    const matrixFallback = mapAgingMatrixByColumns(docType, sheet, provenance);
    if (countPresentFields(matrixFallback.data._single) > countPresentFields(data._single)) {
      data._single = matrixFallback.data._single;
      directMatches = Math.max(directMatches, matrixFallback.directMatches);
      warnings.push(`Mapped ${docType.replace(/_/g, ' ')} from customer aging columns due to missing summary rows.`);
    }
  }

  return finalizeExtraction({
    docType,
    periods: ['_single'],
    data,
    directMatches,
    derivedFields: 0,
    provenance,
    sourceMetadata: buildSheetSourceMetadata(sheet, [describePeriodKey('_single')]),
    warnings,
  });
}

function mapAgingMatrixByColumns(docType, sheet, provenance) {
  const data = { _single: {} };
  const bucketMap = getAgingColumnFieldMap(docType);
  const valueScale = sheet.valueScale || 1;
  const headerRecord = sheet.records[0];
  if (!headerRecord) {
    return { data, directMatches: 0 };
  }

  const columnEntries = Object.entries(headerRecord.values)
    .filter(([columnKey]) => columnKey !== sheet.header[0]);
  const matchedColumns = {};
  for (const [columnKey, label] of columnEntries) {
    const candidates = getFieldMatchCandidates(label, bucketMap);
    if (candidates[0]?.fieldName) {
      matchedColumns[candidates[0].fieldName] = columnKey;
    }
  }

  const customerRows = sheet.records.slice(1).filter((record) => {
    const label = normalizeLabel(record.values[sheet.header[0]]);
    return label && !label.includes('total') && !label.includes('summary');
  });
  const pctColumn = matchedColumns.concentrationTopCustomer || matchedColumns.__percentOfTotal;

  const numericFields = Object.keys(matchedColumns);
  for (const fieldName of numericFields) {
    const total = customerRows.reduce((sum, record) => {
      const value = scaleCurrencyValue(docType, fieldName, toNumericValue(record.values[matchedColumns[fieldName]]), valueScale);
      return sum + (value || 0);
    }, 0);
    if (total > 0) {
      data._single[fieldName] = round(total);
      addDerivedProvenance(provenance, fieldName, '_single', `Aggregated from ${customerRows.length} customer aging rows.`);
    }
  }

  if (docType === DOC_TYPES.AR_AGING) {
    if (pctColumn) {
      const percentages = customerRows
        .map((record) => toPercentageValue(record.values[pctColumn]))
        .filter((value) => value != null)
        .sort((left, right) => right - left);
      if (percentages.length > 0) {
        data._single.concentrationTopCustomer = percentages[0];
        data._single.concentrationTop5 = round(percentages.slice(0, 5).reduce((sum, value) => sum + value, 0));
        addDerivedProvenance(provenance, 'concentrationTopCustomer', '_single', 'Derived from percent-of-total aging column.');
        addDerivedProvenance(provenance, 'concentrationTop5', '_single', 'Derived from top five percent-of-total aging rows.');
      }
    }
  }

  if (pctColumn) {
    data._single.customers = customerRows.map((record) => {
      const name = record.values[sheet.header[0]];
      const amount = scaleCurrencyValue(docType, 'total', toNumericValue(record.values[matchedColumns.total]), valueScale);
      const percentage = toPercentageValue(record.values[pctColumn]);
      if (!name || (amount == null && percentage == null)) return null;
      return {
        name: String(name),
        amount: amount == null ? null : round(amount),
        percentage,
      };
    }).filter(Boolean);
    if (data._single.customers.length > 0) {
      addDerivedProvenance(provenance, 'customers', '_single', 'Captured customer-level rows from the aging schedule.');
    }
  }

  if (docType === DOC_TYPES.AR_AGING && data._single.totalAR == null) {
    data._single.totalAR = data._single.total ?? sumAgingBuckets(data._single);
  }
  if (docType === DOC_TYPES.AP_AGING && data._single.totalAP == null) {
    data._single.totalAP = data._single.total ?? sumAgingBuckets(data._single);
  }

  delete data._single.total;

  return {
    data,
    directMatches: numericFields.length,
  };
}

function mapDebtSchedule(sheet) {
  const data = { _single: { instruments: [] } };
  const provenance = createProvenanceTracker();
  const view = getMatrixSheetView(sheet);
  const valueScale = sheet.valueScale || 1;
  const instrumentColumn = findViewColumn(view, COLUMN_ALIASES.debtSchedule.instrument, true);
  const principalColumn = findViewColumn(view, COLUMN_ALIASES.debtSchedule.principal) || findLatestBalanceColumn(view.valueColumns);
  const rateColumn = findViewColumn(view, COLUMN_ALIASES.debtSchedule.interestRate);
  const maturityColumn = findViewColumn(view, COLUMN_ALIASES.debtSchedule.maturityDate);
  const debtServiceColumn = findViewColumn(view, COLUMN_ALIASES.debtSchedule.annualDebtService);
  let directMatches = 0;

  for (const record of view.records) {
    const name = record.values[instrumentColumn] ?? null;
    const principal = scaleCurrencyValue(DOC_TYPES.DEBT_SCHEDULE, 'totalDebt', toNumericValue(record.values[principalColumn]), valueScale);
    if (!name || principal == null || principal <= 0 || isSummaryLabel(name)) continue;

    data._single.instruments.push({
      name: String(name),
      principal: round(principal),
      rate: toPercentageValue(record.values[rateColumn]),
      maturityDate: toDateValue(record.values[maturityColumn]),
    });
    addMappedProvenance(provenance, {
      fieldName: 'instruments',
      rowLabel: String(name),
      rowIndex: record.rowIndex,
      period: '_single',
      rawValue: principal,
      match: { alias: 'instrument row', matchType: 'column' },
      candidates: [],
      sourceType: 'direct',
    });
    directMatches += 1;
  }

  data._single.totalDebt = round(data._single.instruments.reduce((sum, instrument) => sum + (instrument.principal || 0), 0));
  addDerivedProvenance(provenance, 'totalDebt', '_single', 'Derived from debt instrument principals.');

  const ratedInstruments = data._single.instruments.filter((instrument) => instrument.rate != null && instrument.principal != null);
  if (ratedInstruments.length > 0 && data._single.totalDebt > 0) {
    const weightedRate = ratedInstruments.reduce((sum, instrument) => sum + ((instrument.principal / data._single.totalDebt) * instrument.rate), 0);
    data._single.weightedAvgRate = round(weightedRate);
    addDerivedProvenance(provenance, 'weightedAvgRate', '_single', 'Weighted average of instrument rates by principal.');
  }

  const maturities = data._single.instruments.map((instrument) => instrument.maturityDate).filter(Boolean).sort();
  if (maturities.length > 0) {
    data._single.nearestMaturity = maturities[0];
    addDerivedProvenance(provenance, 'nearestMaturity', '_single', 'Earliest maturity date across instruments.');
  }

  if (debtServiceColumn) {
    const totalDebtService = view.records.reduce((sum, record) => {
      const value = scaleCurrencyValue(DOC_TYPES.DEBT_SCHEDULE, 'annualDebtService', toNumericValue(record.values[debtServiceColumn]), valueScale);
      return sum + (value || 0);
    }, 0);
    if (totalDebtService > 0) {
      data._single.annualDebtService = round(totalDebtService);
    }
  }

  return finalizeExtraction({
    docType: DOC_TYPES.DEBT_SCHEDULE,
    periods: ['_single'],
    data,
    directMatches: directMatches + (data._single.totalDebt != null ? 1 : 0),
    derivedFields: data._single.weightedAvgRate != null ? 1 : 0,
    provenance,
    sourceMetadata: buildSheetSourceMetadata(sheet, [describePeriodKey('_single')]),
    warnings: [],
  });
}

function mapRevenueConcentration(sheet) {
  const data = { _single: { customers: [] } };
  const provenance = createProvenanceTracker();
  const view = getMatrixSheetView(sheet);
  const valueScale = sheet.valueScale || 1;
  const nameColumn = findViewColumn(view, COLUMN_ALIASES.revenueConcentration.name, true);
  const revenueColumn = findViewColumn(view, COLUMN_ALIASES.revenueConcentration.revenue);
  const percentageColumn = findViewColumn(view, COLUMN_ALIASES.revenueConcentration.percentage);
  let directMatches = 0;

  for (const record of view.records) {
    const name = record.values[nameColumn];
    if (!name || normalizeLabel(name).includes('total')) continue;

    const revenue = scaleCurrencyValue(DOC_TYPES.REVENUE_BREAKDOWN, 'totalRevenue', toNumericValue(record.values[revenueColumn]), valueScale);
    const percentage = toPercentageValue(record.values[percentageColumn]);
    if (revenue == null && percentage == null) continue;

    data._single.customers.push({
      name: String(name),
      revenue: revenue == null ? null : round(revenue),
      percentage,
    });
    addMappedProvenance(provenance, {
      fieldName: 'customers',
      rowLabel: String(name),
      rowIndex: record.rowIndex,
      period: '_single',
      rawValue: revenue ?? percentage,
      match: { alias: 'customer row', matchType: 'column' },
      candidates: [],
      sourceType: 'direct',
    });
    directMatches += 1;
  }

  const explicitTotal = findSummaryValue({ ...sheet, records: view.records, header: [view.labelHeader, ...view.valueColumns.map((column) => column.header)] }, DOCUMENT_FIELD_ALIASES[DOC_TYPES.REVENUE_BREAKDOWN].totalRevenue);
  const calculatedTotal = data._single.customers.reduce((sum, customer) => sum + (customer.revenue || 0), 0);
  data._single.totalRevenue = round(explicitTotal ?? calculatedTotal);
  addDerivedProvenance(provenance, 'totalRevenue', '_single', explicitTotal != null ? 'Read from total revenue summary row.' : 'Derived from customer revenues.');

  if (data._single.totalRevenue > 0) {
    data._single.customers = data._single.customers.map((customer) => ({
      ...customer,
      percentage: customer.percentage ?? round((customer.revenue / data._single.totalRevenue) * 100),
    }));
  }

  const sortedCustomers = [...data._single.customers].sort((left, right) => (right.percentage || 0) - (left.percentage || 0));
  data._single.topCustomerPct = sortedCustomers[0]?.percentage ?? null;
  data._single.top3Pct = round(sumPercentages(sortedCustomers.slice(0, 3)));
  data._single.top5Pct = round(sumPercentages(sortedCustomers.slice(0, 5)));
  data._single.customerCount = sortedCustomers.length;
  data._single.breakdownBasis = normalizeLabel(view.labelHeader).includes('service line') ? 'service_line' : 'customer';
  addDerivedProvenance(provenance, 'topCustomerPct', '_single', 'Derived from sorted customer revenue shares.');
  addDerivedProvenance(provenance, 'top3Pct', '_single', 'Derived from top three customer shares.');
  addDerivedProvenance(provenance, 'top5Pct', '_single', 'Derived from top five customer shares.');
  addDerivedProvenance(provenance, 'customerCount', '_single', 'Derived from count of mapped customer rows.');

  return finalizeExtraction({
    docType: DOC_TYPES.REVENUE_BREAKDOWN,
    periods: ['_single'],
    data,
    directMatches: directMatches + 1,
    derivedFields: 3,
    provenance,
    sourceMetadata: buildSheetSourceMetadata(sheet, [describePeriodKey('_single')]),
    warnings: [],
  });
}

function finalizeWithDerivations(docType, partial, derivedFields) {
  return finalizeExtraction({
    docType,
    periods: partial.periods,
    data: partial.data,
    directMatches: partial.directMatches,
    derivedFields,
    provenance: partial.provenance,
    sourceMetadata: partial.sourceMetadata,
    warnings: partial.warnings,
  });
}

function assignMappedValue(target, fieldName, value, rowLabel, provenance, meta = {}) {
  const nextScore = meta.match?.score || 0;
  const existingScore = target.__matchScores?.[fieldName] ?? -1;
  if (target[fieldName] != null && nextScore < existingScore) {
    return;
  }
  if (!target.__matchScores) target.__matchScores = {};

  if (fieldName === '__operatingProfit' || fieldName === '__daCombined') {
    target[fieldName] = value;
    target.__matchScores[fieldName] = nextScore;
    addMappedProvenance(provenance, {
      fieldName,
      rowLabel,
      sourceType: 'direct',
      ...meta,
    });
    return;
  }

  if (fieldName === 'addBacks') {
    if (!Array.isArray(target.addBacks)) target.addBacks = [];
    target.addBacks.push({ item: String(rowLabel), amount: value });
    target.__matchScores[fieldName] = Math.max(existingScore, nextScore);
    addMappedProvenance(provenance, {
      fieldName,
      rowLabel,
      sourceType: 'direct',
      ...meta,
    });
    return;
  }

  target[fieldName] = value;
  target.__matchScores[fieldName] = nextScore;
  addMappedProvenance(provenance, {
    fieldName,
    rowLabel,
    sourceType: 'direct',
    ...meta,
  });
}

function coerceFieldValue(docType, fieldName, rawValue, valueScale = 1) {
  if (fieldName === 'entityType') {
    return rawValue == null ? null : String(rawValue);
  }
  if (fieldName === 'filingYear') {
    return rawValue == null ? null : String(rawValue);
  }
  if (fieldName === 'nearestMaturity') {
    return toDateValue(rawValue);
  }

  if (fieldName.endsWith('Margin') || fieldName.endsWith('Rate') || fieldName.includes('Pct')) {
    return toPercentageValue(rawValue);
  }

  return scaleCurrencyValue(docType, fieldName, toNumericValue(rawValue), valueScale);
}

function findFirstMappedValue(docType, fieldName, record, labelColumn, valueScale = 1) {
  const entries = Object.entries(record.values).filter(([column]) => column !== labelColumn);
  for (const [, rawValue] of entries) {
    const mappedValue = coerceFieldValue(docType, fieldName, rawValue, valueScale);
    if (mappedValue != null) return mappedValue;
  }
  return null;
}

function findColumn(headers, aliases) {
  return headers.find((header) => matchFieldByAliases(header, { match: aliases }) === 'match') || null;
}

function findSummaryValue(sheet, aliases) {
  const labelColumn = sheet.header[0];
  for (const record of sheet.records) {
    const matched = matchFieldByAliases(record.values[labelColumn], { match: aliases });
    if (!matched) continue;
    return Object.entries(record.values)
      .filter(([column]) => column !== labelColumn)
      .map(([, value]) => toNumericValue(value))
      .find((value) => value != null) ?? null;
  }
  return null;
}

function findViewColumn(view, aliases, includeLabelColumn = false) {
  if (includeLabelColumn && matchFieldByAliases(view.labelHeader, { match: aliases }) === 'match') {
    return view.labelColumn;
  }

  const matched = view.valueColumns.find((column) => matchFieldByAliases(column.header, { match: aliases }) === 'match');
  return matched?.columnKey || null;
}

function getAgingColumnFieldMap(docType) {
  const base = {
    current: ['current', '0-30', '0 30', '0 to 30'],
    days30: ['31-60', '30-60', '1-30', '1 to 30'],
    days60: ['61-90', '60-90', '31-60 days', '31 to 60'],
    days90: ['91-120', '90-120', '61-90 days', '61 to 90'],
    days90Plus: ['120+', '91+', '90+', 'over 90', 'over 120', '120 plus', '90 plus'],
    total: ['total'],
    __percentOfTotal: ['% of total', 'percent of total', '% total'],
  };

  if (docType === DOC_TYPES.AR_AGING) {
    return {
      ...base,
      concentrationTopCustomer: ['% of total'],
    };
  }

  return base;
}

function countPresentFields(data) {
  return Object.values(data || {}).filter((value) => value != null).length;
}

function findLatestBalanceColumn(valueColumns = []) {
  const candidates = valueColumns
    .filter((column) => /balance/i.test(String(column.header || '')))
    .map((column) => ({
      columnKey: column.columnKey,
      header: column.header,
      sortValue: extractYearSortValue(column.header),
    }))
    .sort((left, right) => right.sortValue - left.sortValue);

  return candidates[0]?.columnKey || null;
}

function getMatrixSheetView(sheet) {
  const labelColumn = sheet.header[0];
  const baseValueColumns = sheet.columns.slice(1).map((column) => ({
    columnKey: column.header,
    header: column.header,
  }));

  const firstRecord = sheet.records[0];
  if (!firstRecord) {
    return {
      labelColumn,
      labelHeader: labelColumn,
      valueColumns: baseValueColumns,
      records: sheet.records,
    };
  }

  const candidateValueColumns = baseValueColumns.map((column) => ({
    columnKey: column.columnKey,
    header: firstRecord.values[column.columnKey] ?? column.header,
  }));
  const candidatePeriods = detectPeriodColumns(candidateValueColumns);

  if (candidatePeriods.length >= 2) {
    return {
      labelColumn,
      labelHeader: firstRecord.values[labelColumn] ?? labelColumn,
      valueColumns: candidateValueColumns,
      records: sheet.records.slice(1),
    };
  }

  return {
    labelColumn,
    labelHeader: labelColumn,
    valueColumns: baseValueColumns,
    records: sheet.records,
  };
}

function extractYearSortValue(label) {
  const match = String(label || '').match(/((19|20)\d{2})/);
  return match ? Number(match[1]) : 0;
}

function addMappedProvenance(provenance, entry) {
  if (!provenance) return;
  provenance.mappedRows.push({
    fieldName: entry.fieldName,
    rowLabel: String(entry.rowLabel || ''),
    rowIndex: entry.rowIndex ?? null,
    period: entry.period ?? null,
    rawValue: entry.rawValue ?? null,
    sourceType: entry.sourceType || 'direct',
    matchAlias: entry.match?.alias || null,
    matchType: entry.match?.matchType || null,
    matchScore: entry.match?.score ?? null,
    matchConfidence: entry.match?.confidenceLabel || null,
    candidateFields: (entry.candidates || []).map((candidate) => candidate.fieldName).slice(0, 3),
  });
}

function captureAmbiguousRow(provenance, rowLabel, rowIndex, candidates) {
  if (!provenance || !Array.isArray(candidates) || candidates.length < 2) return;
  if (provenance.ambiguousRows.length >= 10) return;
  const topScore = candidates[0].score;
  const nearMatches = candidates.filter((candidate) => Math.abs(candidate.score - topScore) <= AMBIGUITY_SCORE_GAP);
  if (nearMatches.length < 2) return;
  provenance.ambiguousRows.push({
    rowLabel: String(rowLabel || ''),
    rowIndex,
    candidates: nearMatches.slice(0, 4).map((candidate) => ({
      fieldName: candidate.fieldName,
      alias: candidate.alias,
      matchType: candidate.matchType,
      score: candidate.score,
    })),
  });
}

function captureUnmappedRow(provenance, rowLabel, rowIndex) {
  if (!provenance || provenance.unmappedRows.length >= 10) return;
  const normalized = normalizeLabel(rowLabel);
  if (!normalized || /^\d+$/.test(normalized) || shouldIgnoreRowLabel(normalized)) return;
  provenance.unmappedRows.push({
    rowLabel: String(rowLabel || ''),
    rowIndex,
  });
}

function captureLowConfidenceRow(provenance, rowLabel, rowIndex, candidate) {
  if (!provenance || !candidate || candidate.score >= LOW_CONFIDENCE_MATCH_SCORE) return;
  if (provenance.lowConfidenceRows.length >= 10) return;
  const normalized = normalizeLabel(rowLabel);
  if (!normalized || shouldIgnoreRowLabel(normalized)) return;

  provenance.lowConfidenceRows.push({
    rowLabel: String(rowLabel || ''),
    rowIndex,
    fieldName: candidate.fieldName,
    alias: candidate.alias,
    score: candidate.score,
    matchType: candidate.matchType,
  });
}

function addDerivedProvenance(provenance, fieldName, period, note) {
  if (!provenance) return;
  provenance.derivedFields.push({
    fieldName,
    period,
    note,
  });
}

function sumAgingBuckets(point) {
  return round(
    (point.current || 0)
    + (point.days30 || 0)
    + (point.days60 || 0)
    + (point.days90 || 0)
    + (point.days90Plus || 0),
  );
}

function cleanupIntermediateFields(periodData) {
  delete periodData.__operatingProfit;
  delete periodData.__daCombined;
  delete periodData.__matchScores;
}

function sumPercentages(customers) {
  return customers.reduce((sum, customer) => sum + (customer.percentage || 0), 0);
}

function extractYearFromString(value) {
  const match = String(value || '').match(/\b(19|20)\d{2}\b/);
  return match ? match[0] : null;
}

function scaleCurrencyValue(docType, fieldName, value, valueScale = 1) {
  if (value == null) return null;
  const fieldSchema = getFieldSchema(docType, fieldName);
  if (!fieldSchema) {
    return fieldName === 'total' || fieldName === 'amount'
      ? round(value * valueScale)
      : value;
  }
  return fieldSchema.type === 'currency' ? round(value * valueScale) : value;
}

function getFieldSchema(docType, fieldName) {
  const schema = FIELD_SCHEMAS[docType];
  if (!schema) return null;
  return [
    ...(schema.requiredFields || []),
    ...(schema.optionalFields || []),
  ].find((field) => field.name === fieldName) || null;
}

function isSummaryLabel(value) {
  const normalized = normalizeLabel(value);
  return normalized.startsWith('total') || normalized.includes('summary');
}

function shouldIgnoreRowLabel(normalized) {
  return normalized.startsWith('note ')
    || normalized === 'notes'
    || normalized.includes('see accompanying')
    || normalized.includes('page ')
    || normalized.includes('continued');
}

function buildSheetSourceMetadata(sheet, periodMetadata = []) {
  if (!sheet) return null;

  return {
    sourceKind: sheet.sourceKind || 'tabular-sheet',
    sheetName: sheet.name,
    sheetTitle: sheet.title || null,
    headerRowIndex: sheet.headerRowIndex ?? null,
    valueScale: sheet.valueScale || 1,
    pageNumber: sheet.pageNumber ?? null,
    pageRange: sheet.pageRange || null,
    ocrApplied: Boolean(sheet.ocrApplied),
    ocrEngine: sheet.ocrEngine || null,
    extractionMode: sheet.extractionMode || 'tabular',
    parentSheetName: sheet.parentSheetName || null,
    parentSheetIndex: sheet.parentSheetIndex ?? null,
    segmentIndex: sheet.segmentIndex ?? null,
    segmentLabel: sheet.segmentLabel || null,
    segmentationReason: sheet.segmentationReason || null,
    periodMetadata,
  };
}
