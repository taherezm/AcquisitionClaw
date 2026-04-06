#!/usr/bin/env node

import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { applyReviewOverridesToIngestionResponse, buildReviewMemoryBundle } from '../ingestion/reviewOverrides.js';
import { buildPipelineFileDescriptors } from '../api.js';
import { classifyUploadedFile } from '../backend/services/classification/classificationService.js';
import { extractStructuredData } from '../backend/services/extraction/extractionService.js';
import { normalizeExtractionResult } from '../backend/services/normalization/normalizationService.js';
import { parseUploadedFile } from '../backend/services/parsing/parsingService.js';
import { summarizeValidationResults, validateUploadedFile } from '../backend/services/validation/validationService.js';
import { splitWorkbookIntoDocuments } from '../backend/services/workbook/workbookSplitterService.js';
import { toUploadedFileSummary } from '../backend/utils/fileUtils.js';
import { runPipeline } from '../ingestion/pipeline.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, '..');
const BENCHMARK_DIR = path.join(ROOT_DIR, 'mock-data', 'benchmarks');

const selectedFixtureIds = new Set(process.argv.slice(2).filter(Boolean));

const fixturePaths = (await fs.readdir(BENCHMARK_DIR))
  .filter((name) => name.endsWith('.fixture.json'))
  .map((name) => path.join(BENCHMARK_DIR, name));

if (fixturePaths.length === 0) {
  console.error('No benchmark fixtures were found in mock-data/benchmarks.');
  process.exit(1);
}

let failed = false;
let executed = 0;

for (const fixturePath of fixturePaths) {
  const fixture = JSON.parse(await fs.readFile(fixturePath, 'utf8'));
  if (selectedFixtureIds.size > 0 && !selectedFixtureIds.has(fixture.id)) continue;

  executed += 1;
  const reviewMemory = buildReviewMemoryBundle({
    reviewOverrides: Array.isArray(fixture.reviewRules) ? fixture.reviewRules : (fixture.reviewMemory?.reviewOverrides || []),
    sourcePreferences: fixture.reviewMemory?.sourcePreferences || [],
    conceptSuppressions: fixture.reviewMemory?.conceptSuppressions || [],
    timeBasisOverrides: fixture.reviewMemory?.timeBasisOverrides || [],
    entityResolutions: fixture.reviewMemory?.entityResolutions || [],
  });
  const companyContext = {
    companyName: fixture.companyContext?.companyName || '',
    dealName: fixture.companyContext?.dealName || fixture.id,
    reviewerId: fixture.companyContext?.reviewerId || 'benchmark-runner',
    industry: fixture.companyContext?.industry || '',
    ebitdaRange: fixture.companyContext?.ebitdaRange || '1m-3m',
    reviewMemory,
    learnedAliasRules: reviewMemory.learnedAliasRules,
    reviewRankingSignals: reviewMemory.reviewerSignals,
  };

  const uploadedFiles = await Promise.all((fixture.files || []).map(async (relativePath) => {
    const absolutePath = path.resolve(BENCHMARK_DIR, relativePath);
    const buffer = await fs.readFile(absolutePath);
    return {
      fieldname: 'files',
      originalname: path.basename(absolutePath),
      mimetype: inferMimeType(absolutePath),
      size: buffer.length,
      encoding: '7bit',
      buffer,
    };
  }));

  const ingestionResponse = await runLocalIngestion(uploadedFiles, companyContext);
  const reviewerAwareIngestion = applyReviewOverridesToIngestionResponse(ingestionResponse, reviewMemory.reviewOverrides);
  const pipelineResult = runPipeline(buildPipelineFileDescriptors(reviewerAwareIngestion), {
    ...fixture.companyContext,
    dealName: companyContext.dealName,
    reviewerId: companyContext.reviewerId,
    reviewMemory,
    reviewAliasRules: companyContext.learnedAliasRules,
    reviewRankingSignals: companyContext.reviewRankingSignals,
  });
  const baselinePipelineResult = runPipeline(buildPipelineFileDescriptors(ingestionResponse), {
    ...fixture.companyContext,
    dealName: companyContext.dealName,
    reviewerId: companyContext.reviewerId,
    reviewMemory: buildReviewMemoryBundle(),
    reviewAliasRules: [],
    reviewRankingSignals: null,
  });
  const metrics = collectMetrics(reviewerAwareIngestion, pipelineResult, baselinePipelineResult, fixture.expectations || {});
  const failures = evaluateFixtureExpectations(fixture.expectations || {}, metrics);

  if (failures.length > 0) {
    failed = true;
    console.error(`FAIL ${fixture.id}`);
    failures.forEach((failure) => console.error(`  - ${failure}`));
  } else {
    console.log(`PASS ${fixture.id}`);
  }

  console.log(`  accepted=${metrics.acceptedFiles} splitDocs=${metrics.splitDocuments} score=${metrics.overallScore} resolved=${metrics.resolvedFields} conflicts=${metrics.evidenceConflicts} reviewerRules=${metrics.reviewerRuleCount}`);
  console.log(`  calibration labeled=${metrics.labeledFieldCount} accuracy=${metrics.labeledAccuracyPct}% falseHigh=${metrics.falseConfidentSelections} reviewerLift=${metrics.preferredSourceLift} corrected=${metrics.reviewerCorrectionRatePct}%`);
}

if (executed === 0) {
  console.error(`No benchmark fixtures matched: ${[...selectedFixtureIds].join(', ')}`);
  process.exit(1);
}

if (failed) {
  process.exit(1);
}

async function runLocalIngestion(uploadedFiles, companyContext) {
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
      return {
        file: toUploadedFileSummary(file),
        validation,
        parsing: {
          status: 'error',
          parser: null,
          warnings: [error.message],
          tables: [],
          workbook: null,
          textPreview: '',
          parsedDocument: null,
        },
        classification: {
          docType: 'unknown',
          confidence: 0,
          warnings: [error.message],
        },
        extraction: {
          status: 'error',
          usable: false,
          synthetic: false,
          periods: [],
          data: {},
          coverage: { total: 0, found: 0, missing: [], percentage: 0 },
          warnings: [error.message],
          notes: [],
          confidence: 0,
        },
        normalization: {
          status: 'error',
          readyForPipeline: false,
          normalizedDocument: null,
          pipelineContent: null,
          notes: [error.message],
        },
        splitDocuments: [],
      };
    }
  }));

  return {
    receivedAt: new Date().toISOString(),
    companyContext: {
      companyName: companyContext.companyName,
      industry: companyContext.industry,
      ebitdaRange: companyContext.ebitdaRange,
    },
    summary: summarizeValidationResults(results),
    files: results,
  };
}

function collectMetrics(ingestionResponse, pipelineResult, baselinePipelineResult, expectations = {}) {
  const dashboardData = pipelineResult?.dashboardData || {};
  const dataQuality = dashboardData.dataQuality || {};
  const resolvedFields = dataQuality.evidenceResolution?.resolvedFields || [];
  const baselineResolvedFields = baselinePipelineResult?.dashboardData?.dataQuality?.evidenceResolution?.resolvedFields || [];
  const evaluations = Object.entries(expectations.selectedDocTypes || {}).map(([fieldKey, expectedDocType]) => {
    const selected = resolvedFields.find((field) => field.key === fieldKey) || null;
    const baselineSelected = baselineResolvedFields.find((field) => field.key === fieldKey) || null;
    return {
      fieldKey,
      expectedDocType,
      actualDocType: selected?.selected?.docType || null,
      baselineDocType: baselineSelected?.selected?.docType || null,
      confidence: typeof selected?.selected?.rankingScore === 'number'
        ? selected.selected.rankingScore
        : ((selected?.confidencePct || 0) / 100),
    };
  });
  const labeledFieldCount = evaluations.length;
  const correctFieldCount = evaluations.filter((entry) => entry.actualDocType === entry.expectedDocType).length;
  const baselineCorrectFieldCount = evaluations.filter((entry) => entry.baselineDocType === entry.expectedDocType).length;
  const highConfidenceSelections = evaluations.filter((entry) => (entry.confidence || 0) >= 0.8).length;
  const falseConfidentSelections = evaluations.filter((entry) => (entry.confidence || 0) >= 0.8 && entry.actualDocType !== entry.expectedDocType).length;
  const reviewerCorrectionCount = evaluations.filter((entry) => (
    entry.actualDocType === entry.expectedDocType
    && entry.baselineDocType !== entry.expectedDocType
  )).length;

  return {
    acceptedFiles: ingestionResponse.summary?.acceptedFiles || 0,
    splitDocuments: (ingestionResponse.files || []).reduce((sum, fileResult) => sum + ((fileResult.splitDocuments || []).length || 0), 0),
    overallScore: dashboardData.overallScore || 0,
    resolvedFields: resolvedFields.length,
    evidenceConflicts: dataQuality.evidenceResolution?.conflicts?.length || 0,
    temporalConflicts: dataQuality.temporalAlignment?.conflicts?.length || 0,
    entityClusters: dataQuality.entityResolution?.clusters?.length || 0,
    ambiguityWorkflows: dataQuality.ambiguityWorkflows?.items?.length || 0,
    reviewerRuleCount: dataQuality.reviewerSignals?.ruleCount || 0,
    hardErrors: dataQuality.hardErrors?.length || 0,
    selectedDocTypes: Object.fromEntries(resolvedFields.map((field) => [field.key, field.selected?.docType || null])),
    labeledFieldCount,
    labeledAccuracyPct: labeledFieldCount > 0 ? Math.round((correctFieldCount / labeledFieldCount) * 100) : 0,
    highConfidenceSelections,
    falseConfidentSelections,
    falseConfidenceRatePct: highConfidenceSelections > 0 ? Math.round((falseConfidentSelections / highConfidenceSelections) * 100) : 0,
    preferredSourceLift: correctFieldCount - baselineCorrectFieldCount,
    reviewerCorrectionCount,
    reviewerCorrectionRatePct: labeledFieldCount > 0 ? Math.round((reviewerCorrectionCount / labeledFieldCount) * 100) : 0,
  };
}

function evaluateFixtureExpectations(expectations, metrics) {
  const failures = [];

  compareMinimum('accepted files', metrics.acceptedFiles, expectations.minAcceptedFiles, failures);
  compareMinimum('split documents', metrics.splitDocuments, expectations.minSplitDocuments, failures);
  compareMinimum('overall score', metrics.overallScore, expectations.minOverallScore, failures);
  compareMaximum('overall score', metrics.overallScore, expectations.maxOverallScore, failures);
  compareMinimum('resolved fields', metrics.resolvedFields, expectations.minResolvedFields, failures);
  compareMinimum('evidence conflicts', metrics.evidenceConflicts, expectations.minEvidenceConflicts, failures);
  compareMinimum('temporal conflicts', metrics.temporalConflicts, expectations.minTemporalConflicts, failures);
  compareMinimum('entity clusters', metrics.entityClusters, expectations.minEntityClusters, failures);
  compareMinimum('ambiguity workflows', metrics.ambiguityWorkflows, expectations.minAmbiguityWorkflows, failures);
  compareMinimum('reviewer rule count', metrics.reviewerRuleCount, expectations.minReviewerRuleCount, failures);
  compareMaximum('hard errors', metrics.hardErrors, expectations.maxHardErrors, failures);
  compareMinimum('reviewer lift', metrics.preferredSourceLift, expectations.minReviewerLift, failures);
  compareMaximum('false confident selections', metrics.falseConfidentSelections, expectations.maxFalseConfidentSelections, failures);

  Object.entries(expectations.selectedDocTypes || {}).forEach(([fieldKey, expectedDocType]) => {
    const actualDocType = metrics.selectedDocTypes[fieldKey];
    if (actualDocType !== expectedDocType) {
      failures.push(`expected ${fieldKey} to resolve from ${expectedDocType}, got ${actualDocType || 'none'}`);
    }
  });

  return failures;
}

function compareMinimum(label, actual, expected, failures) {
  if (typeof expected !== 'number') return;
  if (actual < expected) {
    failures.push(`expected ${label} >= ${expected}, got ${actual}`);
  }
}

function compareMaximum(label, actual, expected, failures) {
  if (typeof expected !== 'number') return;
  if (actual > expected) {
    failures.push(`expected ${label} <= ${expected}, got ${actual}`);
  }
}

function inferMimeType(filePath) {
  const extension = path.extname(filePath).toLowerCase();
  if (extension === '.csv') return 'text/csv';
  if (extension === '.xlsx') return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
  if (extension === '.pdf') return 'application/pdf';
  return 'application/octet-stream';
}
