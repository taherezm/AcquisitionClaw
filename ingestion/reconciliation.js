import { DOC_TYPES, DOC_TYPE_LABELS } from './schemas.js';

export function buildReconciliationSummary(results = []) {
  const usableResults = results.filter((result) => result.usable);
  const byType = Object.fromEntries(usableResults.map((result) => [result.docType, result]));
  const findings = [];
  const matches = [];

  for (const result of usableResults) {
    findings.push(...detectGranularityIssues(result));
    findings.push(...detectPdfOcrIssues(result));
  }

  evaluateComparablePair({
    left: byType[DOC_TYPES.INCOME_STATEMENT],
    leftField: 'revenue',
    right: byType[DOC_TYPES.TAX_RETURN],
    rightField: 'grossReceipts',
    code: 'revenue_vs_tax_receipts',
    label: 'Income statement revenue vs tax gross receipts',
    findings,
    matches,
  });

  evaluateComparablePair({
    left: byType[DOC_TYPES.INCOME_STATEMENT],
    leftField: 'ebitda',
    right: byType[DOC_TYPES.QOE_REPORT],
    rightField: 'adjustedEbitda',
    code: 'ebitda_vs_qoe',
    label: 'Income statement EBITDA vs QoE adjusted EBITDA',
    findings,
    matches,
    tolerance: 0.75,
  });

  findings.push(...detectDocumentStaleness(byType));

  const warningCount = findings.filter((finding) => finding.severity === 'warning').length;
  const hardErrorCount = findings.filter((finding) => finding.severity === 'hard_error').length;
  const noteCount = findings.filter((finding) => finding.severity === 'note').length;
  const consistencyScoreBase = Math.max(0, 100 - (warningCount * 9) - (hardErrorCount * 18) - (noteCount * 4));

  return {
    consistencyScore: consistencyScoreBase,
    findings,
    matches,
    summary: `${matches.length} reconciled comparison${matches.length === 1 ? '' : 's'}, ${findings.length} reconciliation finding${findings.length === 1 ? '' : 's'}.`,
  };
}

function detectGranularityIssues(result) {
  const granularities = [...new Set((result.sourceMetadata?.periodMetadata || [])
    .map((entry) => entry?.granularity)
    .filter((entry) => entry && entry !== 'unknown' && entry !== 'point_in_time'))];

  if (granularities.length <= 1) return [];

  return [{
    code: 'mixed_period_granularity',
    severity: 'warning',
    docType: result.docType,
    label: DOC_TYPE_LABELS[result.docType] || result.docType,
    message: `${DOC_TYPE_LABELS[result.docType] || result.docType} mixes multiple period granularities (${granularities.join(', ')}), which can distort trends and comparisons.`,
  }];
}

function detectPdfOcrIssues(result) {
  if (result.sourceMetadata?.sourceKind !== 'pdf-page' && result.sourceMetadata?.sourceKind !== 'sheet-section') {
    return [];
  }

  const ocrWarning = (result.warnings || []).find((warning) => /ocr/i.test(String(warning)));
  if (!ocrWarning) return [];

  return [{
    code: 'pdf_ocr_required',
    severity: 'note',
    docType: result.docType,
    label: DOC_TYPE_LABELS[result.docType] || result.docType,
    message: ocrWarning,
  }];
}

function evaluateComparablePair({
  left,
  leftField,
  right,
  rightField,
  code,
  label,
  findings,
  matches,
  tolerance = 0.35,
}) {
  if (!left || !right) return;

  const leftValue = getComparableFieldValue(left, leftField);
  const rightValue = getComparableFieldValue(right, rightField);
  if (!isFiniteNumber(leftValue) || !isFiniteNumber(rightValue)) return;

  const scaleSignal = detectScaleSignal(leftValue, rightValue);
  if (scaleSignal) {
    findings.push({
      code,
      severity: 'warning',
      docType: left.docType,
      relatedDocType: right.docType,
      label,
      message: `${label} looks scaled inconsistently; the two values differ by roughly ${scaleSignal}.`,
    });
    return;
  }

  const variance = relativeDifference(leftValue, rightValue);
  if (variance > tolerance) {
    findings.push({
      code,
      severity: variance > 0.9 ? 'hard_error' : 'warning',
      docType: left.docType,
      relatedDocType: right.docType,
      label,
      message: `${label} does not reconcile within tolerance (${formatPercent(variance)} variance).`,
    });
    return;
  }

  matches.push({
    code,
    label,
    variancePct: formatPercent(variance),
    docTypes: [left.docType, right.docType],
  });
}

function detectDocumentStaleness(byType) {
  const incomeStatement = byType[DOC_TYPES.INCOME_STATEMENT];
  const balanceSheet = byType[DOC_TYPES.BALANCE_SHEET];
  if (!incomeStatement || !balanceSheet) return [];

  const incomeYear = extractComparableYear(incomeStatement);
  const balanceYear = extractComparableYear(balanceSheet);
  if (!incomeYear || !balanceYear) return [];

  const gap = Math.abs(incomeYear - balanceYear);
  if (gap <= 1) return [];

  return [{
    code: 'document_staleness_gap',
    severity: 'warning',
    docType: DOC_TYPES.INCOME_STATEMENT,
    relatedDocType: DOC_TYPES.BALANCE_SHEET,
    label: 'Cross-document period alignment',
    message: `Income statement and balance sheet appear ${gap} years apart, so ratio analysis may be mixing stale periods.`,
  }];
}

function getComparableFieldValue(result, fieldName) {
  if (!result) return null;
  if (result.periods?.includes('_single')) {
    return result.data?._single?.[fieldName] ?? null;
  }

  const latestPeriod = result.periods?.[result.periods.length - 1];
  return latestPeriod ? result.data?.[latestPeriod]?.[fieldName] ?? null : null;
}

function extractComparableYear(result) {
  const latestPeriod = result?.periods?.[result.periods.length - 1];
  const match = String(latestPeriod || '').match(/\b(19|20)\d{2}\b/);
  return match ? Number(match[0]) : null;
}

function detectScaleSignal(leftValue, rightValue) {
  const ratio = Math.max(Math.abs(leftValue), Math.abs(rightValue)) / Math.max(Math.min(Math.abs(leftValue), Math.abs(rightValue)), 1);
  if (ratio >= 900 && ratio <= 1100) return '1,000x';
  if (ratio >= 900000 && ratio <= 1100000) return '1,000,000x';
  return null;
}

function relativeDifference(leftValue, rightValue) {
  const baseline = Math.max(Math.abs(leftValue), Math.abs(rightValue), 1);
  return Math.abs(leftValue - rightValue) / baseline;
}

function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

function formatPercent(value) {
  return `${Math.round(value * 100)}%`;
}
