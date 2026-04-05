import { DOC_TYPES } from '../../../ingestion/schemas.js';
import { COLUMN_ALIASES, DOCUMENT_FIELD_ALIASES } from './fieldAliases.js';
import { finalizeExtraction, matchFieldByAliases, normalizeLabel, round, toDateValue, toNumericValue, toPercentageValue } from './mappingUtils.js';
import { detectPeriodColumns, inferFallbackPeriod, sortPeriodKeys } from './periodUtils.js';

export function mapSheetToSchema({ docType, sheet, fileName = '' }) {
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
      return mapIncomeStatement(sheet, fileName);
    case DOC_TYPES.BALANCE_SHEET:
      return mapMatrixDocument(docType, sheet, fileName);
    case DOC_TYPES.CASH_FLOW_STATEMENT:
      return mapCashFlow(sheet, fileName);
    case DOC_TYPES.AR_AGING:
      return mapAgingSheet(docType, sheet);
    case DOC_TYPES.AP_AGING:
      return mapAgingSheet(docType, sheet);
    case DOC_TYPES.DEBT_SCHEDULE:
      return mapDebtSchedule(sheet);
    case DOC_TYPES.REVENUE_BREAKDOWN:
      return mapRevenueConcentration(sheet);
    case DOC_TYPES.PROJECTIONS:
      return mapProjections(sheet, fileName);
    case DOC_TYPES.TAX_RETURN:
      return mapTaxReturn(sheet, fileName);
    case DOC_TYPES.QOE_REPORT:
      return mapQoeSummary(sheet, fileName);
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

function mapIncomeStatement(sheet, fileName) {
  const result = mapMatrixDocument(DOC_TYPES.INCOME_STATEMENT, sheet, fileName);
  let derivedFields = 0;

  result.periods.forEach((period) => {
    const periodData = result.data[period];
    if (!periodData) return;

    if (periodData.grossProfit == null && periodData.revenue != null && periodData.cogs != null) {
      periodData.grossProfit = round(periodData.revenue - periodData.cogs);
      result.warnings.push(`Derived gross profit for ${period} from revenue and COGS.`);
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
      derivedFields += 1;
    }

    if (periodData.grossMargin == null && periodData.revenue && periodData.grossProfit != null) {
      periodData.grossMargin = round((periodData.grossProfit / periodData.revenue) * 100);
      derivedFields += 1;
    }

    if (periodData.ebitdaMargin == null && periodData.revenue && periodData.ebitda != null) {
      periodData.ebitdaMargin = round((periodData.ebitda / periodData.revenue) * 100);
      derivedFields += 1;
    }

    if (periodData.netMargin == null && periodData.revenue && periodData.netIncome != null) {
      periodData.netMargin = round((periodData.netIncome / periodData.revenue) * 100);
      derivedFields += 1;
    }

    cleanupIntermediateFields(periodData);
  });

  return finalizeWithDerivations(DOC_TYPES.INCOME_STATEMENT, result, derivedFields);
}

function mapCashFlow(sheet, fileName) {
  const result = mapMatrixDocument(DOC_TYPES.CASH_FLOW_STATEMENT, sheet, fileName);
  let derivedFields = 0;

  result.periods.forEach((period) => {
    const periodData = result.data[period];
    if (!periodData) return;

    if (periodData.freeCashFlow == null && periodData.operatingCashFlow != null && periodData.capex != null) {
      const capexImpact = periodData.capex < 0 ? periodData.capex : -periodData.capex;
      periodData.freeCashFlow = round(periodData.operatingCashFlow + capexImpact);
      result.warnings.push(`Derived free cash flow for ${period} from operating cash flow and capex.`);
      derivedFields += 1;
    }
  });

  return finalizeWithDerivations(DOC_TYPES.CASH_FLOW_STATEMENT, result, derivedFields);
}

function mapProjections(sheet, fileName) {
  const result = mapMatrixDocument(DOC_TYPES.PROJECTIONS, sheet, fileName);
  let derivedFields = 0;

  result.periods.forEach((period) => {
    const periodData = result.data[period];
    if (!periodData) return;

    if (periodData.projectedMargin == null && periodData.projectedRevenue && periodData.projectedEbitda != null) {
      periodData.projectedMargin = round((periodData.projectedEbitda / periodData.projectedRevenue) * 100);
      derivedFields += 1;
    }
  });

  return finalizeWithDerivations(DOC_TYPES.PROJECTIONS, result, derivedFields);
}

function mapTaxReturn(sheet, fileName) {
  const result = mapMatrixDocument(DOC_TYPES.TAX_RETURN, sheet, fileName);
  let derivedFields = 0;

  result.periods.forEach((period) => {
    const periodData = result.data[period];
    if (!periodData) return;

    if (periodData.filingYear == null) {
      const filingYear = /^\d{4}$/.test(period) ? period : extractYearFromString(fileName);
      if (filingYear) {
        periodData.filingYear = filingYear;
        derivedFields += 1;
      }
    }
  });

  return finalizeWithDerivations(DOC_TYPES.TAX_RETURN, result, derivedFields);
}

function mapQoeSummary(sheet, fileName) {
  const result = mapMatrixDocument(DOC_TYPES.QOE_REPORT, sheet, fileName);
  let derivedFields = 0;

  result.periods.forEach((period) => {
    const periodData = result.data[period];
    if (!periodData) return;

    if (periodData.totalAddBacks == null && Array.isArray(periodData.addBacks) && periodData.addBacks.length > 0) {
      periodData.totalAddBacks = round(periodData.addBacks.reduce((sum, item) => sum + (item.amount || 0), 0));
      derivedFields += 1;
    }
  });

  return finalizeWithDerivations(DOC_TYPES.QOE_REPORT, result, derivedFields);
}

function mapMatrixDocument(docType, sheet, fileName) {
  const aliasMap = DOCUMENT_FIELD_ALIASES[docType] || {};
  const labelColumn = sheet.header[0];
  const periodColumns = detectPeriodColumns(sheet.columns.slice(1));
  const periods = periodColumns.length > 0
    ? sortPeriodKeys(periodColumns.map((periodColumn) => periodColumn.periodKey))
    : [inferFallbackPeriod(sheet.name, fileName)];
  const data = Object.fromEntries(periods.map((period) => [period, {}]));
  const warnings = [];
  let directMatches = 0;

  for (const record of sheet.records) {
    const rowLabel = record.values[labelColumn];
    const fieldName = matchFieldByAliases(rowLabel, aliasMap);
    if (!fieldName) continue;

    if (periodColumns.length > 0) {
      periodColumns.forEach((periodColumn) => {
        const rawValue = record.values[periodColumn.columnKey];
        const mappedValue = coerceFieldValue(docType, fieldName, rawValue);
        if (mappedValue == null) return;
        assignMappedValue(data[periodColumn.periodKey], fieldName, mappedValue, rowLabel);
      });
    } else {
      const fallbackValue = findFirstMappedValue(docType, fieldName, record, labelColumn);
      if (fallbackValue != null) {
        assignMappedValue(data[periods[0]], fieldName, fallbackValue, rowLabel);
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
    warnings,
  };
}

function mapAgingSheet(docType, sheet) {
  const aliasMap = DOCUMENT_FIELD_ALIASES[docType];
  const labelColumn = sheet.header[0];
  const data = { _single: {} };
  const warnings = [];
  let directMatches = 0;

  for (const record of sheet.records) {
    const rowLabel = record.values[labelColumn];
    const fieldName = matchFieldByAliases(rowLabel, aliasMap);
    if (!fieldName) continue;
    const mappedValue = findFirstMappedValue(docType, fieldName, record, labelColumn);
    if (mappedValue == null) continue;
    data._single[fieldName] = mappedValue;
    directMatches += 1;
  }

  if (data._single.totalAR == null && docType === DOC_TYPES.AR_AGING) {
    data._single.totalAR = sumAgingBuckets(data._single);
  }
  if (data._single.totalAP == null && docType === DOC_TYPES.AP_AGING) {
    data._single.totalAP = sumAgingBuckets(data._single);
  }

  return finalizeExtraction({
    docType,
    periods: ['_single'],
    data,
    directMatches,
    derivedFields: 0,
    warnings,
  });
}

function mapDebtSchedule(sheet) {
  const data = { _single: { instruments: [] } };
  const headers = sheet.header;
  const instrumentColumn = findColumn(headers, COLUMN_ALIASES.debtSchedule.instrument);
  const principalColumn = findColumn(headers, COLUMN_ALIASES.debtSchedule.principal);
  const rateColumn = findColumn(headers, COLUMN_ALIASES.debtSchedule.interestRate);
  const maturityColumn = findColumn(headers, COLUMN_ALIASES.debtSchedule.maturityDate);
  const debtServiceColumn = findColumn(headers, COLUMN_ALIASES.debtSchedule.annualDebtService);
  let directMatches = 0;

  for (const record of sheet.records) {
    const name = record.values[instrumentColumn] ?? null;
    const principal = toNumericValue(record.values[principalColumn]);
    if (!name || principal == null) continue;

    data._single.instruments.push({
      name: String(name),
      principal: round(principal),
      rate: toPercentageValue(record.values[rateColumn]),
      maturityDate: toDateValue(record.values[maturityColumn]),
    });
    directMatches += 1;
  }

  data._single.totalDebt = round(data._single.instruments.reduce((sum, instrument) => sum + (instrument.principal || 0), 0));

  const ratedInstruments = data._single.instruments.filter((instrument) => instrument.rate != null && instrument.principal != null);
  if (ratedInstruments.length > 0 && data._single.totalDebt > 0) {
    const weightedRate = ratedInstruments.reduce((sum, instrument) => sum + ((instrument.principal / data._single.totalDebt) * instrument.rate), 0);
    data._single.weightedAvgRate = round(weightedRate);
  }

  const maturities = data._single.instruments.map((instrument) => instrument.maturityDate).filter(Boolean).sort();
  if (maturities.length > 0) {
    data._single.nearestMaturity = maturities[0];
  }

  if (debtServiceColumn) {
    const firstDebtService = sheet.records.map((record) => toNumericValue(record.values[debtServiceColumn])).find((value) => value != null);
    if (firstDebtService != null) {
      data._single.annualDebtService = round(firstDebtService);
    }
  }

  return finalizeExtraction({
    docType: DOC_TYPES.DEBT_SCHEDULE,
    periods: ['_single'],
    data,
    directMatches: directMatches + (data._single.totalDebt != null ? 1 : 0),
    derivedFields: data._single.weightedAvgRate != null ? 1 : 0,
    warnings: [],
  });
}

function mapRevenueConcentration(sheet) {
  const data = { _single: { customers: [] } };
  const nameColumn = findColumn(sheet.header, COLUMN_ALIASES.revenueConcentration.name);
  const revenueColumn = findColumn(sheet.header, COLUMN_ALIASES.revenueConcentration.revenue);
  const percentageColumn = findColumn(sheet.header, COLUMN_ALIASES.revenueConcentration.percentage);
  let directMatches = 0;

  for (const record of sheet.records) {
    const name = record.values[nameColumn];
    if (!name || normalizeLabel(name).includes('total')) continue;

    const revenue = toNumericValue(record.values[revenueColumn]);
    const percentage = toPercentageValue(record.values[percentageColumn]);
    if (revenue == null && percentage == null) continue;

    data._single.customers.push({
      name: String(name),
      revenue: revenue == null ? null : round(revenue),
      percentage,
    });
    directMatches += 1;
  }

  const explicitTotal = findSummaryValue(sheet, DOCUMENT_FIELD_ALIASES[DOC_TYPES.REVENUE_BREAKDOWN].totalRevenue);
  const calculatedTotal = data._single.customers.reduce((sum, customer) => sum + (customer.revenue || 0), 0);
  data._single.totalRevenue = round(explicitTotal ?? calculatedTotal);

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

  return finalizeExtraction({
    docType: DOC_TYPES.REVENUE_BREAKDOWN,
    periods: ['_single'],
    data,
    directMatches: directMatches + 1,
    derivedFields: 3,
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
    warnings: partial.warnings,
  });
}

function assignMappedValue(target, fieldName, value, rowLabel) {
  if (fieldName === '__operatingProfit' || fieldName === '__daCombined') {
    target[fieldName] = value;
    return;
  }

  if (fieldName === 'addBacks') {
    if (!Array.isArray(target.addBacks)) target.addBacks = [];
    target.addBacks.push({ item: String(rowLabel), amount: value });
    return;
  }

  target[fieldName] = value;
}

function coerceFieldValue(docType, fieldName, rawValue) {
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

  return toNumericValue(rawValue);
}

function findFirstMappedValue(docType, fieldName, record, labelColumn) {
  const entries = Object.entries(record.values).filter(([column]) => column !== labelColumn);
  for (const [, rawValue] of entries) {
    const mappedValue = coerceFieldValue(docType, fieldName, rawValue);
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
}

function sumPercentages(customers) {
  return customers.reduce((sum, customer) => sum + (customer.percentage || 0), 0);
}

function extractYearFromString(value) {
  const match = String(value || '').match(/\b(19|20)\d{2}\b/);
  return match ? match[0] : null;
}
