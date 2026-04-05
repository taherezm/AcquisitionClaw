// ============================================================
// classifier.js — Document classification pipeline
// ============================================================

import { DOC_TYPES, CLASSIFICATION_KEYWORDS } from './schemas.js';

// Placeholder hooks for future OCR/parser integration.
// To integrate a parser, assign a function: (file) => string|null
export const PARSER_HOOKS = {
  '.pdf':  null,
  '.xlsx': null,
  '.xls':  null,
  '.csv':  null,
  '.doc':  null,
  '.docx': null,
};

const CONFIDENCE_THRESHOLD_LOW = 0.3;
const CONFIDENCE_THRESHOLD_REVIEW = 0.5;

/**
 * Classify a single file into a document type.
 *
 * @param {Object} fileDescriptor
 * @param {string} fileDescriptor.name     - Filename
 * @param {string} fileDescriptor.type     - MIME type
 * @param {number} fileDescriptor.size     - File size in bytes
 * @param {string|null} fileDescriptor.content - Text content (from OCR/parser or null)
 * @returns {ClassificationResult}
 */
export function classifyDocument(fileDescriptor) {
  const { name, type, size, content } = fileDescriptor;
  if (content && typeof content === 'object' && content.__backendIngestion) {
    return normalizeBackendClassification(content.backendFileResult?.classification);
  }

  const ext = getExtension(name);
  const nameLower = name.toLowerCase().replace(/[_\-\.]/g, ' ');
  const contentLower = typeof content === 'string' ? content.toLowerCase() : '';

  const scores = {};
  const signals = [];
  const classifiableTypes = Object.values(DOC_TYPES).filter(t => t !== DOC_TYPES.UNKNOWN);

  for (const docType of classifiableTypes) {
    const keywords = CLASSIFICATION_KEYWORDS[docType] || [];
    let filenameScore = 0;
    let contentScore = 0;
    let extensionScore = 0;

    // --- Filename keyword matching ---
    let filenameHits = 0;
    for (const kw of keywords) {
      if (nameLower.includes(kw.toLowerCase())) {
        filenameHits++;
        signals.push(`Filename matches "${kw}" → ${docType}`);
      }
    }
    if (keywords.length > 0) {
      // Any single hit on a specific keyword is a strong signal
      filenameScore = filenameHits > 0 ? Math.min(0.5 + filenameHits * 0.2, 1.0) : 0;
    }

    // --- Content keyword matching ---
    if (contentLower.length > 0) {
      let contentHits = 0;
      for (const kw of keywords) {
        if (contentLower.includes(kw.toLowerCase())) {
          contentHits++;
        }
      }
      if (keywords.length > 0) {
        contentScore = contentHits > 0 ? Math.min(0.5 + contentHits * 0.2, 1.0) : 0;
      }
    }

    // --- Extension heuristics ---
    extensionScore = extensionBias(ext, docType);

    // When content is unavailable, redistribute its weight to filename
    const hasContent = contentLower.length > 0;
    const fnWeight = hasContent ? 0.5 : 0.85;
    const ctWeight = hasContent ? 0.4 : 0;
    const exWeight = hasContent ? 0.1 : 0.15;

    scores[docType] = (filenameScore * fnWeight) + (contentScore * ctWeight) + (extensionScore * exWeight);
  }

  // Sort by score descending
  const allScores = classifiableTypes
    .map(dt => ({ docType: dt, score: round(scores[dt]) }))
    .sort((a, b) => b.score - a.score);

  const topScore = allScores[0].score;
  const secondScore = allScores.length > 1 ? allScores[1].score : 0;

  let docType = allScores[0].docType;
  let confidence = topScore;

  // Fall back to UNKNOWN if too low
  if (topScore < CONFIDENCE_THRESHOLD_LOW) {
    docType = DOC_TYPES.UNKNOWN;
    confidence = round(1.0 - topScore); // high confidence it's unknown
  }

  const needsManualReview = confidence < CONFIDENCE_THRESHOLD_REVIEW ||
    (topScore - secondScore < 0.1 && topScore > CONFIDENCE_THRESHOLD_LOW);

  return {
    docType,
    confidence: round(confidence),
    allScores,
    signals: [...new Set(signals)], // deduplicate
    needsManualReview,
  };
}

/**
 * Classify multiple files at once.
 * @param {Object[]} fileDescriptors
 * @returns {ClassificationResult[]}
 */
export function classifyAll(fileDescriptors) {
  return fileDescriptors.map(fd => ({
    ...classifyDocument(fd),
    file: fd,
  }));
}

// --- Helpers ---

function getExtension(filename) {
  const parts = filename.split('.');
  return parts.length > 1 ? ('.' + parts.pop().toLowerCase()) : '';
}

function extensionBias(ext, docType) {
  // Mild biases based on common file formats for each type
  const biases = {
    '.csv': {
      [DOC_TYPES.AR_AGING]: 0.6,
      [DOC_TYPES.AP_AGING]: 0.6,
      [DOC_TYPES.REVENUE_BREAKDOWN]: 0.5,
      [DOC_TYPES.DEBT_SCHEDULE]: 0.4,
    },
    '.pdf': {
      [DOC_TYPES.TAX_RETURN]: 0.7,
      [DOC_TYPES.QOE_REPORT]: 0.6,
      [DOC_TYPES.INCOME_STATEMENT]: 0.3,
      [DOC_TYPES.BALANCE_SHEET]: 0.3,
    },
    '.xlsx': {}, // neutral for all types
    '.xls': {},
  };

  const extBiases = biases[ext] || {};
  return extBiases[docType] || 0.2; // small default
}

function normalizeBackendClassification(classification = {}) {
  return {
    docType: classification.docType || DOC_TYPES.UNKNOWN,
    confidence: round(classification.confidence || 0),
    allScores: Array.isArray(classification.allScores) ? classification.allScores : [],
    signals: Array.isArray(classification.signals) ? classification.signals : [],
    warnings: Array.isArray(classification.warnings) ? classification.warnings : [],
    needsManualReview: Boolean(classification.needsManualReview),
    primarySheetIndex: classification.primarySheetIndex ?? null,
    primarySheetName: classification.primarySheetName ?? null,
    sheetClassifications: Array.isArray(classification.sheetClassifications) ? classification.sheetClassifications : [],
  };
}

function round(n) {
  return Math.round(n * 100) / 100;
}
