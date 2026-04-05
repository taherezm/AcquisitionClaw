// ============================================================
// schemas.js — Document type definitions, field schemas, enums
// ============================================================

export const DOC_TYPES = Object.freeze({
  INCOME_STATEMENT:    'income_statement',
  BALANCE_SHEET:       'balance_sheet',
  CASH_FLOW_STATEMENT: 'cash_flow_statement',
  TAX_RETURN:          'tax_return',
  QOE_REPORT:          'qoe_report',
  PROJECTIONS:         'projections',
  AR_AGING:            'ar_aging',
  AP_AGING:            'ap_aging',
  DEBT_SCHEDULE:       'debt_schedule',
  REVENUE_BREAKDOWN:   'revenue_breakdown',
  UNKNOWN:             'unknown',
});

export const DOC_TYPE_LABELS = Object.freeze({
  [DOC_TYPES.INCOME_STATEMENT]:    'Income Statement / P&L',
  [DOC_TYPES.BALANCE_SHEET]:       'Balance Sheet',
  [DOC_TYPES.CASH_FLOW_STATEMENT]: 'Cash Flow Statement',
  [DOC_TYPES.TAX_RETURN]:          'Tax Return',
  [DOC_TYPES.QOE_REPORT]:          'Quality of Earnings Report',
  [DOC_TYPES.PROJECTIONS]:         'Projections / Forecast',
  [DOC_TYPES.AR_AGING]:            'Accounts Receivable Aging',
  [DOC_TYPES.AP_AGING]:            'Accounts Payable Aging',
  [DOC_TYPES.DEBT_SCHEDULE]:       'Debt Schedule',
  [DOC_TYPES.REVENUE_BREAKDOWN]:   'Revenue Breakdown / Customer Concentration',
  [DOC_TYPES.UNKNOWN]:             'Unknown / Other',
});

// Field descriptor: { name, type, unit?, description }
// type: "currency" | "percentage" | "ratio" | "integer" | "string" | "date" | "array"

export const FIELD_SCHEMAS = Object.freeze({
  // ---- Income Statement ----
  [DOC_TYPES.INCOME_STATEMENT]: {
    periods: 'multi-year',
    minUsabilityFields: 3,
    requiredFields: [
      { name: 'revenue',           type: 'currency',   description: 'Total revenue / net sales' },
      { name: 'cogs',              type: 'currency',   description: 'Cost of goods sold' },
      { name: 'grossProfit',       type: 'currency',   description: 'Gross profit' },
      { name: 'ebitda',            type: 'currency',   description: 'EBITDA' },
      { name: 'netIncome',         type: 'currency',   description: 'Net income' },
    ],
    optionalFields: [
      { name: 'grossMargin',       type: 'percentage', description: 'Gross margin %' },
      { name: 'operatingExpenses', type: 'currency',   description: 'Total operating expenses' },
      { name: 'ebitdaMargin',      type: 'percentage', description: 'EBITDA margin %' },
      { name: 'depreciation',      type: 'currency',   description: 'Depreciation' },
      { name: 'amortization',      type: 'currency',   description: 'Amortization' },
      { name: 'interestExpense',   type: 'currency',   description: 'Interest expense' },
      { name: 'netMargin',         type: 'percentage', description: 'Net income margin %' },
      { name: 'sga',               type: 'currency',   description: 'Selling, general & administrative' },
    ],
  },

  // ---- Balance Sheet ----
  [DOC_TYPES.BALANCE_SHEET]: {
    periods: 'multi-year',
    minUsabilityFields: 4,
    requiredFields: [
      { name: 'cash',                  type: 'currency', description: 'Cash & equivalents' },
      { name: 'accountsReceivable',    type: 'currency', description: 'Accounts receivable' },
      { name: 'totalCurrentAssets',    type: 'currency', description: 'Total current assets' },
      { name: 'totalAssets',           type: 'currency', description: 'Total assets' },
      { name: 'accountsPayable',       type: 'currency', description: 'Accounts payable' },
      { name: 'totalCurrentLiabilities', type: 'currency', description: 'Total current liabilities' },
      { name: 'longTermDebt',          type: 'currency', description: 'Long-term debt' },
      { name: 'totalLiabilities',      type: 'currency', description: 'Total liabilities' },
      { name: 'equity',                type: 'currency', description: 'Total shareholders equity' },
    ],
    optionalFields: [
      { name: 'inventory',             type: 'currency', description: 'Inventory' },
      { name: 'ppe',                   type: 'currency', description: 'Property, plant & equipment (net)' },
      { name: 'currentPortionLTD',     type: 'currency', description: 'Current portion of long-term debt' },
      { name: 'goodwill',              type: 'currency', description: 'Goodwill' },
      { name: 'intangibles',           type: 'currency', description: 'Intangible assets' },
    ],
  },

  // ---- Cash Flow Statement ----
  [DOC_TYPES.CASH_FLOW_STATEMENT]: {
    periods: 'multi-year',
    minUsabilityFields: 3,
    requiredFields: [
      { name: 'operatingCashFlow',  type: 'currency', description: 'Cash from operations' },
      { name: 'capex',              type: 'currency', description: 'Capital expenditures' },
      { name: 'freeCashFlow',       type: 'currency', description: 'Free cash flow (OCF - CapEx)' },
    ],
    optionalFields: [
      { name: 'investingCashFlow',  type: 'currency', description: 'Cash from investing activities' },
      { name: 'financingCashFlow',  type: 'currency', description: 'Cash from financing activities' },
      { name: 'netChangeInCash',    type: 'currency', description: 'Net change in cash' },
      { name: 'dividendsPaid',      type: 'currency', description: 'Dividends paid' },
      { name: 'debtRepayment',      type: 'currency', description: 'Debt repayment' },
    ],
  },

  // ---- Tax Return ----
  [DOC_TYPES.TAX_RETURN]: {
    periods: 'single-year',
    minUsabilityFields: 2,
    requiredFields: [
      { name: 'taxableIncome',     type: 'currency',   description: 'Taxable income' },
      { name: 'totalTaxLiability', type: 'currency',   description: 'Total tax liability' },
      { name: 'filingYear',        type: 'string',     description: 'Tax year' },
    ],
    optionalFields: [
      { name: 'effectiveTaxRate',  type: 'percentage', description: 'Effective tax rate' },
      { name: 'entityType',        type: 'string',     description: 'Entity type (S-corp, C-corp, LLC, etc.)' },
      { name: 'grossReceipts',     type: 'currency',   description: 'Gross receipts / sales' },
      { name: 'officerCompensation', type: 'currency', description: 'Officer compensation' },
    ],
  },

  // ---- Quality of Earnings Report ----
  [DOC_TYPES.QOE_REPORT]: {
    periods: 'multi-year',
    minUsabilityFields: 2,
    requiredFields: [
      { name: 'adjustedEbitda',    type: 'currency',   description: 'Adjusted EBITDA' },
      { name: 'totalAddBacks',     type: 'currency',   description: 'Total add-backs / adjustments' },
    ],
    optionalFields: [
      { name: 'addBacks',          type: 'array',      description: 'List of individual add-back items' },
      { name: 'ownerCompensation', type: 'currency',   description: 'Owner / officer compensation' },
      { name: 'normalizedOwnerComp', type: 'currency', description: 'Market-rate replacement compensation' },
      { name: 'workingCapitalTarget', type: 'currency', description: 'Normalized working capital target' },
      { name: 'reportedEbitda',    type: 'currency',   description: 'Reported (unadjusted) EBITDA' },
    ],
  },

  // ---- Projections / Forecast ----
  [DOC_TYPES.PROJECTIONS]: {
    periods: 'multi-year-forward',
    minUsabilityFields: 2,
    requiredFields: [
      { name: 'projectedRevenue',    type: 'currency',   description: 'Projected revenue' },
      { name: 'projectedEbitda',     type: 'currency',   description: 'Projected EBITDA' },
    ],
    optionalFields: [
      { name: 'projectedGrowthRate', type: 'percentage', description: 'Projected revenue growth rate' },
      { name: 'projectedMargin',     type: 'percentage', description: 'Projected EBITDA margin' },
      { name: 'assumptions',         type: 'array',      description: 'Key forecast assumptions' },
    ],
  },

  // ---- AR Aging ----
  [DOC_TYPES.AR_AGING]: {
    periods: 'point-in-time',
    minUsabilityFields: 2,
    requiredFields: [
      { name: 'totalAR',              type: 'currency',   description: 'Total accounts receivable' },
      { name: 'current',              type: 'currency',   description: 'Current (0–30 days)' },
    ],
    optionalFields: [
      { name: 'days30',               type: 'currency',   description: '31–60 days' },
      { name: 'days60',               type: 'currency',   description: '61–90 days' },
      { name: 'days90',               type: 'currency',   description: '91–120 days' },
      { name: 'days90Plus',           type: 'currency',   description: '120+ days' },
      { name: 'concentrationTopCustomer', type: 'percentage', description: 'Top customer as % of AR' },
      { name: 'concentrationTop5',    type: 'percentage', description: 'Top 5 customers as % of AR' },
    ],
  },

  // ---- AP Aging ----
  [DOC_TYPES.AP_AGING]: {
    periods: 'point-in-time',
    minUsabilityFields: 2,
    requiredFields: [
      { name: 'totalAP',   type: 'currency', description: 'Total accounts payable' },
      { name: 'current',   type: 'currency', description: 'Current (0–30 days)' },
    ],
    optionalFields: [
      { name: 'days30',    type: 'currency', description: '31–60 days' },
      { name: 'days60',    type: 'currency', description: '61–90 days' },
      { name: 'days90',    type: 'currency', description: '91–120 days' },
      { name: 'days90Plus', type: 'currency', description: '120+ days' },
    ],
  },

  // ---- Debt Schedule ----
  [DOC_TYPES.DEBT_SCHEDULE]: {
    periods: 'point-in-time',
    minUsabilityFields: 2,
    requiredFields: [
      { name: 'totalDebt',       type: 'currency',   description: 'Total outstanding debt' },
      { name: 'instruments',     type: 'array',       description: 'List of debt instruments' },
    ],
    optionalFields: [
      { name: 'weightedAvgRate', type: 'percentage',  description: 'Weighted average interest rate' },
      { name: 'annualDebtService', type: 'currency',  description: 'Annual debt service (P&I)' },
      { name: 'nearestMaturity', type: 'date',        description: 'Nearest maturity date' },
    ],
  },

  // ---- Revenue Breakdown / Customer Concentration ----
  [DOC_TYPES.REVENUE_BREAKDOWN]: {
    periods: 'point-in-time',
    minUsabilityFields: 2,
    requiredFields: [
      { name: 'totalRevenue',     type: 'currency',   description: 'Total revenue' },
      { name: 'customers',        type: 'array',       description: 'Customer-level revenue data' },
    ],
    optionalFields: [
      { name: 'topCustomerPct',   type: 'percentage',  description: 'Top customer as % of revenue' },
      { name: 'top3Pct',          type: 'percentage',  description: 'Top 3 customers as % of revenue' },
      { name: 'top5Pct',          type: 'percentage',  description: 'Top 5 customers as % of revenue' },
      { name: 'customerCount',    type: 'integer',     description: 'Total number of customers' },
    ],
  },

  // ---- Unknown ----
  [DOC_TYPES.UNKNOWN]: {
    periods: 'unknown',
    minUsabilityFields: 0,
    requiredFields: [],
    optionalFields: [],
  },
});

// Keywords used by the classifier for filename/content matching
export const CLASSIFICATION_KEYWORDS = Object.freeze({
  [DOC_TYPES.INCOME_STATEMENT]: [
    'income statement', 'income_statement', 'p&l', 'p_l', 'pnl', 'profit and loss',
    'profit_loss', 'profit-loss', 'operating results', 'earnings statement',
  ],
  [DOC_TYPES.BALANCE_SHEET]: [
    'balance sheet', 'balance_sheet', 'statement of financial position', 'assets and liabilities',
  ],
  [DOC_TYPES.CASH_FLOW_STATEMENT]: [
    'cash flow', 'cashflow', 'cash_flow', 'statement of cash flows', 'sources and uses',
  ],
  [DOC_TYPES.TAX_RETURN]: [
    'tax return', 'tax_return', '1120', '1065', '1040', 'schedule c', 'schedule k',
    'k-1', 'k1', 'tax filing',
  ],
  [DOC_TYPES.QOE_REPORT]: [
    'quality of earnings', 'qoe', 'q_o_e', 'earnings quality', 'adjusted ebitda',
    'add-back', 'addback', 'normalization',
  ],
  [DOC_TYPES.PROJECTIONS]: [
    'projection', 'forecast', 'pro forma', 'proforma', 'budget', 'forward-looking',
    'financial model', 'projected',
  ],
  [DOC_TYPES.AR_AGING]: [
    'ar aging', 'a/r aging', 'accounts receivable aging', 'receivable aging',
    'ar_aging', 'receivables aging',
  ],
  [DOC_TYPES.AP_AGING]: [
    'ap aging', 'a/p aging', 'accounts payable aging', 'payable aging',
    'ap_aging', 'payables aging',
  ],
  [DOC_TYPES.DEBT_SCHEDULE]: [
    'debt schedule', 'debt_schedule', 'loan schedule', 'debt summary',
    'outstanding debt', 'credit facility', 'note payable',
  ],
  [DOC_TYPES.REVENUE_BREAKDOWN]: [
    'revenue breakdown', 'revenue_breakdown', 'customer concentration',
    'customer list', 'revenue by customer', 'sales by customer',
    'customer revenue', 'top customers',
  ],
});

// Industry benchmark medians used for scoring calibration
export const INDUSTRY_BENCHMARKS = Object.freeze({
  'Manufacturing':              { grossMargin: 35, ebitdaMargin: 12, debtToEbitda: 2.5, currentRatio: 1.5 },
  'SaaS / Software':           { grossMargin: 72, ebitdaMargin: 20, debtToEbitda: 2.0, currentRatio: 2.0 },
  'Healthcare Services':       { grossMargin: 45, ebitdaMargin: 15, debtToEbitda: 3.0, currentRatio: 1.3 },
  'Professional Services':     { grossMargin: 50, ebitdaMargin: 18, debtToEbitda: 1.5, currentRatio: 1.8 },
  'Distribution / Logistics':  { grossMargin: 25, ebitdaMargin: 8,  debtToEbitda: 2.5, currentRatio: 1.4 },
  'Construction':              { grossMargin: 22, ebitdaMargin: 8,  debtToEbitda: 2.0, currentRatio: 1.3 },
  'Retail / E-Commerce':       { grossMargin: 38, ebitdaMargin: 10, debtToEbitda: 2.5, currentRatio: 1.2 },
  'Food & Beverage':           { grossMargin: 35, ebitdaMargin: 12, debtToEbitda: 3.0, currentRatio: 1.3 },
  'Other':                     { grossMargin: 40, ebitdaMargin: 14, debtToEbitda: 2.5, currentRatio: 1.5 },
});

// EBITDA midpoints for synthetic data generation
export const EBITDA_RANGE_MIDPOINTS = Object.freeze({
  'sub-1m':    750000,
  '1m-3m':     2000000,
  '3m-5m':     4000000,
  '5m-10m':    7500000,
  '10m-25m':   17500000,
  '25m-plus':  35000000,
});
