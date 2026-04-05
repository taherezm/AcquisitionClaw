import { DOC_TYPES, DOC_TYPE_LABELS } from '../../../ingestion/schemas.js';
import { getClassificationRuleSet, getClassificationThresholds } from './classificationRules.js';

export function classifyUploadedFile(file, parsing) {
  if (parsing.status !== 'parsed' || !parsing.parsedDocument) {
    return makeUnknownClassification({
      warnings: parsing.warnings || ['Parsing did not complete, so classification could not run.'],
    });
  }

  const sheetClassifications = parsing.parsedDocument.sheets.map((sheet) =>
    classifyParsedSheet(file, sheet),
  );
  const primarySheet = choosePrimarySheetClassification(sheetClassifications);
  const strongTypedSheets = sheetClassifications.filter((sheetResult) =>
    sheetResult.docType !== DOC_TYPES.UNKNOWN && sheetResult.confidence >= THRESHOLDS.STRONG_SHEET_THRESHOLD,
  );

  const warnings = [...(primarySheet.warnings || [])];
  if (strongTypedSheets.length > 1) {
    warnings.push(
      `Workbook contains multiple classified sheet types: ${[...new Set(strongTypedSheets.map((sheet) => sheet.docTypeLabel))].join(', ')}.`,
    );
  }

  return {
    docType: primarySheet.docType,
    docTypeLabel: primarySheet.docTypeLabel,
    confidence: primarySheet.confidence,
    allScores: primarySheet.allScores,
    signals: primarySheet.signals,
    warnings: [...new Set(warnings)],
    needsManualReview: primarySheet.needsManualReview,
    primarySheetIndex: primarySheet.sheetIndex,
    primarySheetName: primarySheet.sheetName,
    sheetClassifications,
  };
}

const THRESHOLDS = getClassificationThresholds();
const CLASSIFICATION_RULES = getClassificationRuleSet();

function classifyParsedSheet(file, sheet) {
  const fileNameText = normalizeText(file.originalname);
  const sheetNameText = normalizeText(sheet.name);
  const headerText = normalizeText(sheet.header.join(' '));

  const allScores = CLASSIFICATION_RULES
    .map((rule) => {
      const filenameSignal = scoreSignalWithWeights(fileNameText, rule.filenameKeywords, {
        baseScore: 0.75,
        increment: 0.1,
      });
      const sheetNameSignal = scoreSignalWithWeights(sheetNameText, rule.sheetNameKeywords, {
        baseScore: 0.85,
        increment: 0.1,
      });
      const headerSignal = scoreSignalWithWeights(headerText, rule.headerKeywords, {
        baseScore: 0.45,
        increment: 0.15,
      });
      const score = round(
        (filenameSignal * THRESHOLDS.FILE_NAME_WEIGHT)
        + (sheetNameSignal * THRESHOLDS.SHEET_NAME_WEIGHT)
        + (headerSignal * THRESHOLDS.HEADER_WEIGHT),
      );

      return {
        docType: rule.docType,
        score,
        breakdown: {
          filenameSignal,
          sheetNameSignal,
          headerSignal,
        },
      };
    })
    .sort((left, right) => {
      if (right.score !== left.score) return right.score - left.score;
      return left.docType.localeCompare(right.docType);
    });

  const top = allScores[0] || { docType: DOC_TYPES.UNKNOWN, score: 0, breakdown: {} };
  const second = allScores[1] || { score: 0 };
  const warnings = [];
  const signals = buildSignals({
    fileNameText,
    sheetNameText,
    headers: sheet.header,
    docType: top.docType,
  });

  let docType = top.docType;
  let confidence = top.score;

  if (top.score < THRESHOLDS.CONFIDENCE_THRESHOLD_LOW) {
    docType = DOC_TYPES.UNKNOWN;
    confidence = round(top.score);
    warnings.push('Classification confidence is too low to assign a document type.');
  }

  if (confidence < THRESHOLDS.CONFIDENCE_THRESHOLD_REVIEW) {
    warnings.push('Weak classification: review this sheet before relying on downstream mapping.');
  }

  if (top.score - second.score < THRESHOLDS.AMBIGUITY_GAP_THRESHOLD && top.score >= THRESHOLDS.CONFIDENCE_THRESHOLD_LOW) {
    warnings.push(`Ambiguous classification: ${DOC_TYPE_LABELS[top.docType]} is close to another document type.`);
  }

  return {
    sheetIndex: sheet.index,
    sheetName: sheet.name,
    docType,
    docTypeLabel: DOC_TYPE_LABELS[docType] || DOC_TYPE_LABELS[DOC_TYPES.UNKNOWN],
    confidence: round(confidence),
    allScores: allScores.map(({ docType: candidateDocType, score }) => ({
      docType: candidateDocType,
      score,
    })),
    signals,
    warnings,
    needsManualReview: warnings.length > 0,
  };
}

function choosePrimarySheetClassification(sheetClassifications) {
  if (sheetClassifications.length === 0) {
    return makeUnknownClassification({
      warnings: ['No parsed sheets were available for classification.'],
    });
  }

  const sorted = [...sheetClassifications].sort((left, right) => {
    if (left.docType === DOC_TYPES.UNKNOWN && right.docType !== DOC_TYPES.UNKNOWN) return 1;
    if (right.docType === DOC_TYPES.UNKNOWN && left.docType !== DOC_TYPES.UNKNOWN) return -1;
    if (right.confidence !== left.confidence) return right.confidence - left.confidence;
    return left.sheetIndex - right.sheetIndex;
  });

  return sorted[0];
}

function buildSignals({ fileNameText, sheetNameText, headers, docType }) {
  const signals = [];
  const headerText = normalizeText(headers.join(' '));
  const rule = CLASSIFICATION_RULES.find((candidate) => candidate.docType === docType);

  if (!rule) return signals;

  for (const keyword of rule.filenameKeywords) {
    if (fileNameText.includes(normalizeText(keyword))) {
      signals.push(`Filename matches "${keyword}"`);
    }
  }

  for (const keyword of rule.sheetNameKeywords) {
    if (sheetNameText.includes(normalizeText(keyword))) {
      signals.push(`Sheet name matches "${keyword}"`);
    }
  }

  for (const keyword of rule.headerKeywords) {
    if (headerText.includes(normalizeText(keyword))) {
      signals.push(`Header matches "${keyword}"`);
    }
  }

  return [...new Set(signals)];
}

function scoreSignal(haystack, keywords) {
  return scoreSignalWithWeights(haystack, keywords, {
    baseScore: 0.45,
    increment: 0.15,
  });
}

function scoreSignalWithWeights(haystack, keywords, { baseScore, increment }) {
  if (!haystack || keywords.length === 0) return 0;

  let hitCount = 0;
  for (const keyword of keywords) {
    if (haystack.includes(normalizeText(keyword))) {
      hitCount += 1;
    }
  }

  if (hitCount === 0) return 0;
  return round(Math.min(baseScore + ((hitCount - 1) * increment), 1));
}

function normalizeText(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[_./\\\-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function makeUnknownClassification({ warnings = [] } = {}) {
  return {
    docType: DOC_TYPES.UNKNOWN,
    docTypeLabel: DOC_TYPE_LABELS[DOC_TYPES.UNKNOWN],
    confidence: 0,
    allScores: [],
    signals: [],
    warnings,
    needsManualReview: true,
    primarySheetIndex: null,
    primarySheetName: null,
    sheetClassifications: [],
  };
}

function round(value) {
  return Math.round(value * 100) / 100;
}
