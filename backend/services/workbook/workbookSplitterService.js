import { DOC_TYPES, DOC_TYPE_LABELS } from '../../../ingestion/schemas.js';
import { classifyParsedSheetLike } from '../classification/classificationService.js';
import { extractStructuredDataForClassification } from '../extraction/extractionService.js';
import { sortPeriodKeys } from '../mapping/periodUtils.js';
import { summarizeInterpretability } from '../mapping/mappingUtils.js';
import { normalizeExtractionResult } from '../normalization/normalizationService.js';
import { segmentParsedSheet } from './sectionSegmentationService.js';

export function splitWorkbookIntoDocuments({
  file,
  validation,
  parsing,
  classification,
  companyContext = {},
}) {
  if (!validation.accepted || parsing.status !== 'parsed') {
    return [];
  }

  const parsedSheets = Array.isArray(parsing.parsedDocument?.sheets)
    ? parsing.parsedDocument.sheets
    : [];
  if (parsedSheets.length === 0) return [];

  const baseSheetClassifications = new Map(
    (Array.isArray(classification.sheetClassifications) ? classification.sheetClassifications : [])
      .map((sheetClassification) => [sheetClassification.sheetIndex, sheetClassification]),
  );

  const classifiedSources = parsedSheets.flatMap((sheet) => {
    let sections = [];
    try {
      sections = segmentParsedSheet(sheet);
    } catch (_error) {
      sections = [];
    }
    const sectionCandidates = sections.length > 1
      ? sections.map((segment) => ({
        sheet,
        candidateSheet: segment,
        classification: classifyParsedSheetLike(file, segment),
      }))
      : [];

    const baseClassification = baseSheetClassifications.get(sheet.index) || classifyParsedSheetLike(file, sheet);
    const usableSections = sectionCandidates.filter(({ classification: result }) => (
      result.docType !== DOC_TYPES.UNKNOWN
      && (result.confidence || 0) > 0.3
    ));

    if (usableSections.length > 0) {
      return usableSections;
    }

    return [{
      sheet,
      candidateSheet: sheet,
      classification: baseClassification,
    }];
  });

  const typedSheets = classifiedSources.filter(({ classification: sheetClassification, candidateSheet }) => (
    sheetClassification.docType
    && sheetClassification.docType !== DOC_TYPES.UNKNOWN
    && (sheetClassification.confidence || 0) > 0.3
    && !/^(cover|summary|instructions?)$/i.test(String(candidateSheet.segmentLabel || sheetClassification.sheetName || '').trim())
  ));

  const documents = typedSheets.flatMap(({ sheet, candidateSheet, classification: sheetClassification }) => {
    try {
      const sheetClassificationResult = {
        ...sheetClassification,
        primarySheetIndex: sheet.index,
        primarySheetName: sheet.name,
        selectedSheet: candidateSheet,
      };
      const extraction = extractStructuredDataForClassification({
        file,
        validation,
        parsing,
        classification: sheetClassificationResult,
        companyContext,
      });
      const normalization = normalizeExtractionResult({
        validation,
        extraction,
        classification: sheetClassificationResult,
      });

      return [{
        documentId: candidateSheet.sourceKind === 'sheet-section'
          ? `${file.originalname}::sheet:${sheet.index}::segment:${candidateSheet.segmentIndex}::${sheetClassification.docType}`
          : `${file.originalname}::sheet:${sheet.index}::${sheetClassification.docType}`,
        sourceKind: candidateSheet.sourceKind || 'workbook-sheet',
        sourceFileName: file.originalname,
        sheetIndex: sheet.index,
        sheetName: sheet.name,
        parentSheetName: candidateSheet.parentSheetName || null,
        segmentIndex: candidateSheet.segmentIndex ?? null,
        segmentLabel: candidateSheet.segmentLabel || null,
        pageNumber: candidateSheet.pageNumber ?? null,
        docType: sheetClassification.docType,
        docTypeLabel: sheetClassification.docTypeLabel || DOC_TYPE_LABELS[sheetClassification.docType] || sheetClassification.docType,
        confidence: sheetClassification.confidence || 0,
        needsManualReview: Boolean(sheetClassification.needsManualReview),
        classification: sheetClassificationResult,
        extraction,
        normalization,
      }];
    } catch (_error) {
      return [];
    }
  });

  return deduplicateDocuments(documents);
}

function deduplicateDocuments(documents) {
  const byType = new Map();

  for (const document of documents) {
    const existing = byType.get(document.docType);
    if (!existing) {
      byType.set(document.docType, document);
      continue;
    }

    if (mergeDocumentsIfComplementary(existing, document)) {
      continue;
    }

    const existingReady = existing.normalization?.readyForPipeline;
    const nextReady = document.normalization?.readyForPipeline;

    if (nextReady && !existingReady) {
      byType.set(document.docType, document);
      continue;
    }
    if (existingReady && !nextReady) {
      continue;
    }
    if ((document.extraction?.confidence || 0) > (existing.extraction?.confidence || 0)) {
      byType.set(document.docType, document);
      continue;
    }

    if (scoreDocument(document) > scoreDocument(existing)) {
      byType.set(document.docType, document);
    }
  }

  return [...byType.values()];
}

function mergeDocumentsIfComplementary(existing, incoming) {
  const existingPeriods = Array.isArray(existing.extraction?.periods) ? existing.extraction.periods : [];
  const incomingPeriods = Array.isArray(incoming.extraction?.periods) ? incoming.extraction.periods : [];
  if (existingPeriods.includes('_single') || incomingPeriods.includes('_single')) {
    return false;
  }

  const newPeriods = incomingPeriods.filter((period) => !existingPeriods.includes(period));
  if (newPeriods.length === 0) return false;

  if (!existing.extraction?.data || !incoming.extraction?.data) return false;

  newPeriods.forEach((period) => {
    existing.extraction.data[period] = incoming.extraction.data[period];
  });
  existing.extraction.periods = sortPeriodKeys([...existingPeriods, ...newPeriods]);
  existing.extraction.coverage = {
    ...(existing.extraction.coverage || {}),
    found: Math.max(existing.extraction.coverage?.found || 0, incoming.extraction.coverage?.found || 0),
  };
  existing.extraction.missingFields = incoming.extraction.missingFields?.length < (existing.extraction.missingFields?.length || Number.MAX_SAFE_INTEGER)
    ? incoming.extraction.missingFields
    : existing.extraction.missingFields;
  existing.extraction.warnings = [...new Set([...(existing.extraction.warnings || []), ...(incoming.extraction.warnings || [])])];
  existing.extraction.confidence = Math.max(existing.extraction.confidence || 0, incoming.extraction.confidence || 0);
  if (existing.extraction.provenance && incoming.extraction.provenance) {
    existing.extraction.provenance.mappedRows = [
      ...(existing.extraction.provenance.mappedRows || []),
      ...(incoming.extraction.provenance.mappedRows || []),
    ].slice(0, 40);
    existing.extraction.provenance.derivedFields = [
      ...(existing.extraction.provenance.derivedFields || []),
      ...(incoming.extraction.provenance.derivedFields || []),
    ].slice(0, 20);
  }
  existing.extraction.interpretability = summarizeInterpretability(existing.extraction.provenance);
  if (existing.extraction.sourceMetadata && incoming.extraction.sourceMetadata) {
    existing.extraction.sourceMetadata.periodMetadata = [
      ...(existing.extraction.sourceMetadata.periodMetadata || []),
      ...(incoming.extraction.sourceMetadata.periodMetadata || []),
    ];
  }

  if (existing.normalization?.readyForPipeline && incoming.normalization?.readyForPipeline) {
    newPeriods.forEach((period) => {
      existing.normalization.normalizedDocument.data[period] = incoming.normalization.normalizedDocument.data[period];
      existing.normalization.pipelineContent.data[period] = incoming.normalization.pipelineContent.data[period];
    });
    existing.normalization.normalizedDocument.periods = existing.extraction.periods;
    existing.normalization.pipelineContent.periods = existing.extraction.periods;
  }

  existing.mergedSources = [
    ...(existing.mergedSources || [summarizeSource(existing)]),
    summarizeSource(incoming),
  ];
  return true;
}

function scoreDocument(document) {
  const extraction = document.extraction || {};
  const normalization = document.normalization || {};
  const classification = document.classification || {};

  return (
    (normalization.readyForPipeline ? 1 : 0) * 100
    + (extraction.usable ? 1 : 0) * 50
    + ((extraction.coverage?.found || 0) * 2)
    + Math.round((extraction.confidence || 0) * 100)
    + Math.round((classification.confidence || 0) * 100)
  );
}

function summarizeSource(document) {
  return {
    documentId: document.documentId,
    sourceKind: document.sourceKind,
    sheetName: document.sheetName,
    segmentIndex: document.segmentIndex ?? null,
    pageNumber: document.pageNumber ?? null,
  };
}
