import { DOC_TYPES, FIELD_SCHEMAS } from '../../../ingestion/schemas.js';
import { mapSheetToSchema } from '../mapping/schemaMapper.js';

export function extractStructuredData({
  file,
  validation,
  parsing,
  classification,
}) {
  if (!validation.accepted) {
    return {
      status: 'skipped',
      usable: false,
      synthetic: false,
      periods: [],
      data: {},
      coverage: { total: 0, found: 0, missing: [], percentage: 0 },
      warnings: [validation.reason],
      mappingHints: null,
      notes: [],
    };
  }

  if (parsing.status !== 'parsed') {
    return {
      status: 'blocked',
      usable: false,
      synthetic: false,
      periods: [],
      data: {},
      coverage: { total: 0, found: 0, missing: [], percentage: 0 },
      warnings: parsing.warnings,
      mappingHints: null,
      notes: ['Extraction is blocked until parsing succeeds.'],
    };
  }

  if (classification.docType === DOC_TYPES.UNKNOWN) {
    const selectedSheet = getSelectedSheet(parsing, classification);
    return {
      status: 'manual-review',
      usable: false,
      synthetic: false,
      periods: [],
      data: {},
      coverage: { total: 0, found: 0, missing: [], percentage: 0 },
      warnings: [
        'Document type is still unknown. Add a classifier override or improve filename/content signals before mapping.',
        ...(classification.warnings || []),
      ],
      mappingHints: {
        candidateHeaders: selectedSheet?.header || parsing.tables[0]?.header || [],
        candidateColumns: selectedSheet?.columns || [],
        detectedPeriods: detectPeriods(selectedSheet?.header || parsing.tables[0]?.header || []),
        sampleMetricLabels: detectMetricLabels(
          selectedSheet?.records || parsing.tables[0]?.previewRows || [],
          selectedSheet?.header?.[0] || parsing.tables[0]?.header?.[0],
        ),
        sheetClassifications: classification.sheetClassifications || [],
      },
      notes: [],
    };
  }

  const schema = FIELD_SCHEMAS[classification.docType];
  const expectedFields = [
    ...(schema?.requiredFields || []),
    ...(schema?.optionalFields || []),
  ].map((field) => field.name);

  const firstTable = parsing.tables[0] || { header: [], previewRows: [] };
  const selectedSheet = getSelectedSheet(parsing, classification);
  const mapped = mapSheetToSchema({
    docType: classification.docType,
    sheet: selectedSheet,
    fileName: file?.originalname || '',
  });

  if (mapped.usable) {
    return {
      status: 'mapped',
      usable: true,
      synthetic: false,
      periods: mapped.periods,
      data: mapped.data,
      coverage: mapped.coverage,
      missingFields: mapped.missingFields,
      mappingConfidence: mapped.mappingConfidence,
      warnings: [
        ...(classification.warnings || []),
        ...(mapped.warnings || []),
      ],
      confidence: round(Math.min(
        1,
        ((classification.confidence || 0) * 0.4) + ((mapped.mappingConfidence || 0) * 0.6),
      )),
      notes: [],
    };
  }

  return {
    status: 'partial',
    usable: false,
    synthetic: false,
    periods: mapped.periods,
    data: mapped.data,
    coverage: mapped.coverage || {
      total: expectedFields.length,
      found: 0,
      missing: expectedFields,
      percentage: 0,
    },
    missingFields: mapped.missingFields || expectedFields,
    mappingConfidence: mapped.mappingConfidence || 0,
    warnings: [
      `Parsed ${parsing.parser.toUpperCase()} data was partially mapped for ${classification.docTypeLabel}.`,
      ...(classification.warnings || []),
      ...(mapped.warnings || []),
    ],
    confidence: round(((classification.confidence || 0) * 0.4) + ((mapped.mappingConfidence || 0) * 0.6)),
    mappingHints: {
      candidateHeaders: selectedSheet?.header || firstTable.header,
      candidateColumns: selectedSheet?.columns || [],
      detectedPeriods: detectPeriods(selectedSheet?.header || firstTable.header),
      expectedFields,
      sampleMetricLabels: detectMetricLabels(
        selectedSheet?.records || firstTable.previewRows,
        selectedSheet?.header?.[0] || firstTable.header[0],
      ),
      sheetClassifications: classification.sheetClassifications || [],
    },
    notes: [
      'Map parsed columns and row labels into FIELD_SCHEMAS for this document type here.',
      'Once this service emits schema-aligned periods/data, normalization can pass it straight back to the frontend pipeline.',
    ],
  };
}

function detectPeriods(headers) {
  return headers.filter((header) => /\b(19|20)\d{2}\b|ltm|ttm|fy\d{2}|\bq[1-4]\b/i.test(String(header)));
}

function detectMetricLabels(previewRows, labelColumn) {
  return previewRows
    .map((row) => row?.[labelColumn] ?? row?.values?.[labelColumn] ?? null)
    .filter((value) => typeof value === 'string' && value.trim().length > 0)
    .slice(0, 8);
}

function getSelectedSheet(parsing, classification) {
  const sheetIndex = classification.primarySheetIndex ?? 0;
  return parsing.parsedDocument?.sheets?.find((sheet) => sheet.index === sheetIndex)
    || parsing.parsedDocument?.sheets?.[0]
    || null;
}

function round(value) {
  return Math.round(value * 100) / 100;
}
