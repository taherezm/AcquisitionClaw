import { randomUUID } from 'node:crypto';

import { buildReviewMemoryBundle } from '../../ingestion/reviewOverrides.js';
import { DOC_TYPES, DOC_TYPE_LABELS } from '../../ingestion/schemas.js';
import { classifyUploadedFile } from '../services/classification/classificationService.js';
import { extractStructuredData } from '../services/extraction/extractionService.js';
import { normalizeExtractionResult } from '../services/normalization/normalizationService.js';
import { parseUploadedFile } from '../services/parsing/parsingService.js';
import { loadPersistedReviewMemory } from '../services/review/reviewMemoryService.js';
import { summarizeValidationResults, validateUploadedFile } from '../services/validation/validationService.js';
import { splitWorkbookIntoDocuments } from '../services/workbook/workbookSplitterService.js';
import { toUploadedFileSummary } from '../utils/fileUtils.js';

export async function ingestUploadedFiles(req, res) {
  const uploadedFiles = req.files || [];
  const companyContext = await buildCompanyContext(req.body || {});

  if (uploadedFiles.length === 0) {
    return res.status(400).json({
      error: 'No files were uploaded. Submit multipart/form-data with one or more "files" fields.',
    });
  }

  const results = await Promise.all(uploadedFiles.map(async (file) => {
    const validation = validateUploadedFile(file);

    try {
      const parsing = await parseUploadedFile(file, validation);
      const classification = classifyUploadedFile(file, parsing);
      const extraction = extractStructuredData({
        file,
        validation,
        parsing,
        classification,
        companyContext,
      });
      const normalization = normalizeExtractionResult({
        validation,
        extraction,
        classification,
      });
      const splitDocuments = splitWorkbookIntoDocuments({
        file,
        validation,
        parsing,
        classification,
        companyContext,
      });

      const { contentText: _contentText, ...publicParsing } = parsing;

      return {
        file: toUploadedFileSummary(file),
        validation,
        parsing: publicParsing,
        classification,
        extraction,
        normalization,
        splitDocuments,
      };
    } catch (error) {
      return buildErroredFileResult(file, validation, error);
    }
  }));

  return res.status(200).json({
    requestId: randomUUID(),
    receivedAt: new Date().toISOString(),
    companyContext: {
      companyName: companyContext.companyName,
      industry: companyContext.industry,
      ebitdaRange: companyContext.ebitdaRange,
    },
    summary: summarizeValidationResults(results),
    files: results,
  });
}

function buildErroredFileResult(file, validation, error) {
  return {
    file: toUploadedFileSummary(file),
    validation,
    parsing: {
      status: 'error',
      parser: null,
      warnings: [`Failed to process ${file?.originalname || 'uploaded file'}: ${error.message}`],
      tables: [],
      workbook: null,
      textPreview: '',
      parsedDocument: null,
    },
    classification: {
      docType: DOC_TYPES.UNKNOWN,
      docTypeLabel: DOC_TYPE_LABELS[DOC_TYPES.UNKNOWN],
      confidence: 0,
      allScores: [],
      signals: [],
      warnings: ['Processing stopped before classification completed.'],
      needsManualReview: true,
      primarySheetIndex: null,
      primarySheetName: null,
      sheetClassifications: [],
    },
    extraction: {
      status: 'error',
      usable: false,
      synthetic: false,
      periods: [],
      data: {},
      coverage: { total: 0, found: 0, missing: [], percentage: 0 },
      warnings: [`Extraction was skipped because processing failed: ${error.message}`],
      mappingHints: null,
      notes: [],
      confidence: 0,
    },
    normalization: {
      status: 'error',
      readyForPipeline: false,
      normalizedDocument: null,
      pipelineContent: null,
      notes: ['This file was excluded after an internal processing error.'],
    },
    splitDocuments: [],
  };
}

async function buildCompanyContext(body = {}) {
  const companyName = body.companyName || '';
  const dealName = body.dealName || '';
  const reviewerId = body.reviewerId || '';
  const persistedReviewMemory = companyName
    ? await loadPersistedReviewMemory({
      companyName,
      dealName,
      reviewerId,
    })
    : null;

  const requestAliasRules = safeJsonParse(body.reviewAliasRules, []);
  const requestOverrideRules = safeJsonParse(body.reviewOverrideRules, []);
  const requestSourcePreferences = safeJsonParse(body.reviewSourcePreferences, []);
  const requestConceptSuppressions = safeJsonParse(body.reviewConceptSuppressions, []);
  const requestTimeBasisOverrides = safeJsonParse(body.reviewTimeBasisOverrides, []);
  const requestEntityResolutions = safeJsonParse(body.reviewEntityResolutions, []);
  const reviewOverrides = mergeUniqueById(
    persistedReviewMemory?.reviewOverrides || [],
    requestOverrideRules,
    (entry) => entry?.id || `${entry?.docType || ''}::${entry?.rowLabel || ''}::${entry?.action || ''}`,
  );
  const sourcePreferences = mergeUniqueById(
    persistedReviewMemory?.sourcePreferences || [],
    requestSourcePreferences,
    (entry) => entry?.conceptKey || '',
  );
  const conceptSuppressions = mergeUniqueById(
    persistedReviewMemory?.conceptSuppressions || [],
    requestConceptSuppressions,
    (entry) => `${entry?.conceptKey || ''}::${entry?.docType || ''}::${entry?.sourceRefKey || ''}`,
  );
  const timeBasisOverrides = mergeUniqueById(
    persistedReviewMemory?.timeBasisOverrides || [],
    requestTimeBasisOverrides,
    (entry) => `${entry?.docType || ''}::${entry?.sourceRefKey || ''}`,
  );
  const entityResolutions = mergeUniqueById(
    persistedReviewMemory?.entityResolutions || [],
    requestEntityResolutions,
    (entry) => entry?.id || '',
  );
  const derivedReviewMemory = buildReviewMemoryBundle({
    reviewOverrides,
    sourcePreferences,
    conceptSuppressions,
    timeBasisOverrides,
    entityResolutions,
  });

  return {
    companyName,
    dealName,
    reviewerId,
    industry: body.industry || '',
    ebitdaRange: body.ebitdaRange || '',
    reviewOverrides: derivedReviewMemory.reviewOverrides,
    sourcePreferences: derivedReviewMemory.sourcePreferences,
    conceptSuppressions: derivedReviewMemory.conceptSuppressions,
    timeBasisOverrides: derivedReviewMemory.timeBasisOverrides,
    entityResolutions: derivedReviewMemory.entityResolutions,
    learnedAliasRules: mergeUniqueById(
      derivedReviewMemory.learnedAliasRules,
      requestAliasRules,
      (entry) => `${entry?.docType || ''}::${entry?.fieldName || ''}::${entry?.aliasNormalized || entry?.alias || ''}`,
    ),
    reviewRankingSignals: derivedReviewMemory.reviewerSignals,
    persistedReviewMemory,
  };
}

function safeJsonParse(rawValue, fallback) {
  if (!rawValue || typeof rawValue !== 'string') return fallback;
  try {
    return JSON.parse(rawValue);
  } catch (_error) {
    return fallback;
  }
}

function mergeUniqueById(storedItems = [], requestItems = [], buildId) {
  const map = new Map();
  [...storedItems, ...requestItems].forEach((item) => {
    const id = buildId(item);
    if (!id) return;
    map.set(id, item);
  });
  return [...map.values()];
}
