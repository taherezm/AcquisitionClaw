// ============================================================
// pipeline.js — Orchestrates classify → extract → normalize
// ============================================================

import { DOC_TYPES, DOC_TYPE_LABELS } from './schemas.js';
import { classifyDocument } from './classifier.js';
import { extractFields } from './extractor.js';
import { normalizeToModel } from './normalizer.js';

/**
 * Run the full ingestion pipeline on uploaded files.
 *
 * @param {Object[]} files          - Array of { name, type, size, content }
 * @param {Object}   companyContext  - { companyName, industry, ebitdaRange, ebitdaRangeValue }
 * @returns {PipelineResult}
 */
export function runPipeline(files, companyContext) {
  // Step 1: Classify each file
  const classifications = files.map(file => ({
    file,
    result: classifyDocument(file),
  }));

  // Step 2: Extract fields from each classified document
  const extractions = classifications.map(({ file, result }) =>
    extractFields(result, file, companyContext)
  );

  // Step 3: Deduplicate — if multiple files map to the same type,
  // merge if they cover different periods, otherwise keep highest confidence
  const deduplicated = deduplicateExtractions(extractions);

  // Step 4: Normalize into dashboard model
  const dashboardData = normalizeToModel(deduplicated, companyContext);

  // Step 5: Assemble diagnostics
  const diagnostics = buildDiagnostics(classifications, deduplicated, dashboardData);

  return { dashboardData, diagnostics };
}

/**
 * Get a human-readable label for a document type.
 */
export function getDocTypeLabel(docType) {
  return DOC_TYPE_LABELS[docType] || 'Unknown';
}

// ---- Deduplication ----

function deduplicateExtractions(extractions) {
  const byType = {};

  for (const ext of extractions) {
    if (!ext.usable) continue;
    const existing = byType[ext.docType];

    if (!existing) {
      byType[ext.docType] = ext;
      continue;
    }

    // If they cover different periods, merge
    const existingPeriods = new Set(existing.periods);
    const newPeriods = ext.periods.filter(p => !existingPeriods.has(p));

    if (newPeriods.length > 0) {
      // Merge: add new periods' data to existing
      for (const p of newPeriods) {
        existing.data[p] = ext.data[p];
        existing.periods.push(p);
      }
      existing.periods.sort();
      // Recalculate coverage
      existing.coverage.found = Math.max(existing.coverage.found, ext.coverage.found);
      existing.confidence = Math.max(existing.confidence, ext.confidence);
    } else if (ext.confidence > existing.confidence) {
      // Same periods — keep higher confidence
      byType[ext.docType] = ext;
    }
  }

  return Object.values(byType);
}

// ---- Diagnostics ----

function buildDiagnostics(classifications, extractions, dashboardData = {}) {
  const classResults = classifications.map(c => ({
    fileName: c.file.name,
    docType: c.result.docType,
    docTypeLabel: getDocTypeLabel(c.result.docType),
    confidence: c.result.confidence,
    needsManualReview: c.result.needsManualReview,
    signals: c.result.signals,
  }));

  const foundTypes = [...new Set(extractions.map(e => e.docType))];
  const allTypes = Object.values(DOC_TYPES).filter(t => t !== DOC_TYPES.UNKNOWN);
  const missingTypes = allTypes.filter(t => !foundTypes.includes(t));

  const criticalTypes = [DOC_TYPES.INCOME_STATEMENT, DOC_TYPES.BALANCE_SHEET];
  const missingCritical = criticalTypes.filter(t => !foundTypes.includes(t));

  // Overall confidence: product of document coverage and average extraction confidence
  const avgExtractionConfidence = extractions.length > 0
    ? extractions.reduce((s, e) => s + e.confidence, 0) / extractions.length
    : 0;
  const coverageFactor = Math.min(foundTypes.length / 5, 1.0); // 5+ doc types = full coverage credit
  let overallConfidence = Math.round(avgExtractionConfidence * coverageFactor * 100) / 100;
  if (missingCritical.length > 0) overallConfidence = Math.min(overallConfidence, 0.5);

  const warnings = [];
  if (missingCritical.length > 0) {
    warnings.push(`Critical documents missing: ${missingCritical.map(t => t.replace(/_/g, ' ')).join(', ')}`);
  }
  if (extractions.some(e => e.synthetic)) {
    warnings.push('Using synthetic data — connect a document parser for production use');
  }
  const reviewNeeded = classifications.filter(c => c.result.needsManualReview);
  if (reviewNeeded.length > 0) {
    warnings.push(`${reviewNeeded.length} document(s) flagged for manual classification review`);
  }
  if (dashboardData?.dataQuality?.validationWarnings?.length) {
    warnings.push(`${dashboardData.dataQuality.validationWarnings.length} validation warning(s) surfaced during normalization`);
  }
  if (dashboardData?.dataQuality?.hardErrors?.length) {
    warnings.push(`${dashboardData.dataQuality.hardErrors.length} validation hard error(s) require review before relying on the score`);
  }

  return {
    classifications: classResults,
    extractions: extractions.map(e => ({
      docType: e.docType,
      docTypeLabel: getDocTypeLabel(e.docType),
      periods: e.periods,
      coverage: e.coverage,
      confidence: e.confidence,
      usable: e.usable,
      synthetic: e.synthetic,
      warnings: e.warnings,
    })),
    documentCoverage: {
      found: foundTypes,
      missing: missingTypes,
      critical: missingCritical,
    },
    overallConfidence,
    overallConfidenceLabel: dashboardData?.overallConfidence || null,
    dataQuality: dashboardData?.dataQuality || null,
    warnings,
  };
}
