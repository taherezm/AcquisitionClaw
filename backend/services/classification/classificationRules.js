import { CLASSIFICATION_KEYWORDS, DOC_TYPES } from '../../../ingestion/schemas.js';

const FILE_NAME_WEIGHT = 0.25;
const SHEET_NAME_WEIGHT = 0.35;
const HEADER_WEIGHT = 0.4;

const CONFIDENCE_THRESHOLD_LOW = 0.3;
const CONFIDENCE_THRESHOLD_REVIEW = 0.55;
const AMBIGUITY_GAP_THRESHOLD = 0.12;
const STRONG_SHEET_THRESHOLD = 0.55;

const HEADER_KEYWORDS = Object.freeze({
  [DOC_TYPES.INCOME_STATEMENT]: [
    'revenue',
    'net sales',
    'gross profit',
    'ebitda',
    'net income',
    'operating expenses',
  ],
  [DOC_TYPES.BALANCE_SHEET]: [
    'cash',
    'accounts receivable',
    'inventory',
    'total assets',
    'accounts payable',
    'equity',
  ],
  [DOC_TYPES.CASH_FLOW_STATEMENT]: [
    'operating cash flow',
    'investing cash flow',
    'financing cash flow',
    'free cash flow',
    'capex',
    'net change in cash',
  ],
  [DOC_TYPES.TAX_RETURN]: [
    'taxable income',
    'gross receipts',
    'total tax liability',
    'effective tax rate',
    'officer compensation',
    'filing year',
  ],
  [DOC_TYPES.QOE_REPORT]: [
    'adjusted ebitda',
    'reported ebitda',
    'add-backs',
    'working capital target',
    'normalized owner comp',
    'owner compensation',
  ],
  [DOC_TYPES.PROJECTIONS]: [
    'projected revenue',
    'projected ebitda',
    'forecast',
    'budget',
    'growth rate',
    'assumptions',
  ],
  [DOC_TYPES.AR_AGING]: [
    'total ar',
    'accounts receivable',
    'current',
    '31-60',
    '61-90',
    '90+',
  ],
  [DOC_TYPES.AP_AGING]: [
    'total ap',
    'accounts payable',
    'current',
    '31-60',
    '61-90',
    '90+',
  ],
  [DOC_TYPES.DEBT_SCHEDULE]: [
    'total debt',
    'principal',
    'interest rate',
    'maturity',
    'annual debt service',
    'instrument',
  ],
  [DOC_TYPES.REVENUE_BREAKDOWN]: [
    'customer',
    'customer name',
    'revenue by customer',
    'top customer',
    'customer concentration',
    'percentage of revenue',
  ],
});

export function getClassificationRuleSet() {
  return Object.values(DOC_TYPES).filter((docType) => docType !== DOC_TYPES.UNKNOWN).map((docType) => ({
    docType,
    filenameKeywords: CLASSIFICATION_KEYWORDS[docType] || [],
    sheetNameKeywords: CLASSIFICATION_KEYWORDS[docType] || [],
    headerKeywords: HEADER_KEYWORDS[docType] || [],
  }));
}

export function getClassificationThresholds() {
  return {
    CONFIDENCE_THRESHOLD_LOW,
    CONFIDENCE_THRESHOLD_REVIEW,
    AMBIGUITY_GAP_THRESHOLD,
    STRONG_SHEET_THRESHOLD,
    FILE_NAME_WEIGHT,
    SHEET_NAME_WEIGHT,
    HEADER_WEIGHT,
  };
}
