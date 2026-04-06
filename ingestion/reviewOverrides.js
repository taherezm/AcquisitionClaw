import { FIELD_SCHEMAS } from './schemas.js';

const STORAGE_KEY = 'acquisitionclaw.review-overrides.v1';
const ALIAS_STORAGE_KEY = 'acquisitionclaw.learned-aliases.v1';
const SOURCE_PREFERENCE_STORAGE_KEY = 'acquisitionclaw.source-preferences.v1';
const CONCEPT_SUPPRESSION_STORAGE_KEY = 'acquisitionclaw.concept-suppressions.v1';
const TIME_BASIS_OVERRIDE_STORAGE_KEY = 'acquisitionclaw.time-basis-overrides.v1';
const ENTITY_RESOLUTION_STORAGE_KEY = 'acquisitionclaw.entity-resolutions.v1';

export function loadReviewOverrides() {
  if (typeof localStorage === 'undefined') return [];

  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed : [];
  } catch (_error) {
    return [];
  }
}

export function saveReviewOverride(ruleInput, options = {}) {
  const rules = Array.isArray(options.rules) ? options.rules : loadReviewOverrides();
  const nextRules = upsertReviewOverrideRule(rules, ruleInput);
  if (options.persist !== false) {
    persistRules(nextRules);
    persistLearnedAliasRules(deriveLearnedAliasRulesFromReviewOverrides(nextRules));
  }
  return nextRules;
}

export function removeReviewOverride(ruleInput, options = {}) {
  const rules = Array.isArray(options.rules) ? options.rules : loadReviewOverrides();
  const nextRules = deleteReviewOverrideRule(rules, ruleInput);
  if (options.persist !== false) {
    persistRules(nextRules);
    persistLearnedAliasRules(deriveLearnedAliasRulesFromReviewOverrides(nextRules));
  }
  return nextRules;
}

export function loadLearnedAliasRules() {
  if (typeof localStorage === 'undefined') return [];

  try {
    const raw = localStorage.getItem(ALIAS_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed) ? parsed : [];
  } catch (_error) {
    return [];
  }
}

export function loadSourcePreferences() {
  if (typeof localStorage === 'undefined') return [];

  try {
    const raw = localStorage.getItem(SOURCE_PREFERENCE_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed)
      ? parsed.map(normalizeSourcePreference).filter(Boolean)
      : [];
  } catch (_error) {
    return [];
  }
}

export function loadConceptSuppressions() {
  if (typeof localStorage === 'undefined') return [];

  try {
    const raw = localStorage.getItem(CONCEPT_SUPPRESSION_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed)
      ? parsed.map(normalizeConceptSuppression).filter(Boolean)
      : [];
  } catch (_error) {
    return [];
  }
}

export function loadTimeBasisOverrides() {
  if (typeof localStorage === 'undefined') return [];

  try {
    const raw = localStorage.getItem(TIME_BASIS_OVERRIDE_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed)
      ? parsed.map(normalizeTimeBasisOverride).filter(Boolean)
      : [];
  } catch (_error) {
    return [];
  }
}

export function loadEntityResolutions() {
  if (typeof localStorage === 'undefined') return [];

  try {
    const raw = localStorage.getItem(ENTITY_RESOLUTION_STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : [];
    return Array.isArray(parsed)
      ? parsed.map(normalizeEntityResolution).filter(Boolean)
      : [];
  } catch (_error) {
    return [];
  }
}

export function loadReviewerRankingSignals() {
  return buildReviewMemoryBundle({
    reviewOverrides: loadReviewOverrides(),
    sourcePreferences: loadSourcePreferences(),
    conceptSuppressions: loadConceptSuppressions(),
    timeBasisOverrides: loadTimeBasisOverrides(),
    entityResolutions: loadEntityResolutions(),
  }).reviewerSignals;
}

export function buildReviewerRankingSignals(input = []) {
  const config = Array.isArray(input)
    ? { reviewOverrides: input }
    : (input || {});
  const normalizedRules = Array.isArray(config.reviewOverrides)
    ? config.reviewOverrides.map(normalizeRule).filter((rule) => rule.docType && rule.rowLabelNormalized)
    : [];
  const sourcePreferences = normalizeSourcePreferences(config.sourcePreferences || []);
  const conceptSuppressions = normalizeConceptSuppressions(config.conceptSuppressions || []);
  const timeBasisOverrides = normalizeTimeBasisOverrides(config.timeBasisOverrides || []);
  const entityResolutions = normalizeEntityResolutions(config.entityResolutions || []);

  const docTypeMap = new Map();
  const fieldMap = new Map();
  const labelMap = new Map();
  const sheetMap = new Map();

  normalizedRules.forEach((rule) => {
    const docTypeKey = rule.docType;
    const docEntry = getOrCreateProfileEntry(docTypeMap, docTypeKey, () => ({
      docType: rule.docType,
      mapCount: 0,
      ignoreCount: 0,
      labelCount: 0,
    }));
    docEntry.labelCount += 1;
    if (rule.action === 'map') docEntry.mapCount += 1;
    if (rule.action === 'ignore') docEntry.ignoreCount += 1;

    const sheetKey = `${rule.docType}::${rule.sheetNameNormalized}`;
    const sheetEntry = getOrCreateProfileEntry(sheetMap, sheetKey, () => ({
      docType: rule.docType,
      sheetName: rule.sheetName || '',
      sheetNameNormalized: rule.sheetNameNormalized,
      mapCount: 0,
      ignoreCount: 0,
    }));
    if (rule.action === 'map') sheetEntry.mapCount += 1;
    if (rule.action === 'ignore') sheetEntry.ignoreCount += 1;

    const labelKey = `${rule.docType}::${rule.rowLabelNormalized}`;
    const labelEntry = getOrCreateProfileEntry(labelMap, labelKey, () => ({
      docType: rule.docType,
      rowLabel: rule.rowLabel,
      rowLabelNormalized: rule.rowLabelNormalized,
      mapCount: 0,
      ignoreCount: 0,
      fieldNames: new Set(),
    }));
    if (rule.action === 'map') {
      labelEntry.mapCount += 1;
      if (rule.fieldName) labelEntry.fieldNames.add(rule.fieldName);
    }
    if (rule.action === 'ignore') {
      labelEntry.ignoreCount += 1;
    }

    if (rule.action === 'map' && rule.fieldName) {
      const fieldKey = `${rule.docType}::${rule.fieldName}`;
      const fieldEntry = getOrCreateProfileEntry(fieldMap, fieldKey, () => ({
        docType: rule.docType,
        fieldName: rule.fieldName,
        mapCount: 0,
        sourceLabels: new Set(),
      }));
      fieldEntry.mapCount += 1;
      fieldEntry.sourceLabels.add(rule.rowLabel);
    }
  });

  const docTypes = [...docTypeMap.values()]
    .map((entry) => {
      const total = entry.mapCount + entry.ignoreCount;
      const net = entry.mapCount - (entry.ignoreCount * 0.75);
      const trustAdjustment = clamp(net * 0.035, -0.12, 0.12);
      const trustScore = clamp(0.5 + trustAdjustment + Math.min(entry.mapCount, 4) * 0.04, 0.2, 1);
      const noiseRatio = total > 0 ? round(entry.ignoreCount / total, 2) : 0;
      return {
        docType: entry.docType,
        mapCount: entry.mapCount,
        ignoreCount: entry.ignoreCount,
        ruleCount: total,
        trustAdjustment: round(trustAdjustment),
        trustScore,
        trustPct: Math.round(trustScore * 100),
        noiseRatio,
        summary: `${entry.mapCount} confirmed mapping${entry.mapCount === 1 ? '' : 's'} and ${entry.ignoreCount} ignored row${entry.ignoreCount === 1 ? '' : 's'} are informing ${entry.docType.replace(/_/g, ' ')} ranking.`,
      };
    })
    .sort((left, right) => {
      if (right.trustAdjustment !== left.trustAdjustment) return right.trustAdjustment - left.trustAdjustment;
      return right.ruleCount - left.ruleCount;
    });

  const fields = [...fieldMap.values()]
    .map((entry) => ({
      docType: entry.docType,
      fieldName: entry.fieldName,
      mapCount: entry.mapCount,
      confidenceBoost: round(clamp(entry.mapCount * 0.03, 0, 0.1)),
      sourceLabels: [...entry.sourceLabels].slice(0, 4),
    }))
    .sort((left, right) => right.mapCount - left.mapCount);

  const labelMappings = [...labelMap.values()]
    .filter((entry) => entry.mapCount > 0)
    .map((entry) => ({
      docType: entry.docType,
      rowLabel: entry.rowLabel,
      rowLabelNormalized: entry.rowLabelNormalized,
      mapCount: entry.mapCount,
      ignoreCount: entry.ignoreCount,
      mappedFields: [...entry.fieldNames],
      confidenceBoost: round(clamp(entry.mapCount * 0.04, 0, 0.14)),
      conflictPenalty: round(clamp(entry.ignoreCount * 0.03, 0, 0.12)),
    }))
    .sort((left, right) => {
      if (right.mapCount !== left.mapCount) return right.mapCount - left.mapCount;
      return left.ignoreCount - right.ignoreCount;
    });

  const noisyLabels = [...labelMap.values()]
    .filter((entry) => entry.ignoreCount > 0)
    .map((entry) => {
      const total = entry.mapCount + entry.ignoreCount;
      const noiseScore = total > 0 ? clamp((entry.ignoreCount / total) + (entry.ignoreCount * 0.08), 0, 1) : 0;
      return {
        docType: entry.docType,
        rowLabel: entry.rowLabel,
        rowLabelNormalized: entry.rowLabelNormalized,
        ignoreCount: entry.ignoreCount,
        mapCount: entry.mapCount,
        noiseScore: round(noiseScore),
        noisePct: Math.round(noiseScore * 100),
        mappedFields: [...entry.fieldNames],
      };
    })
    .sort((left, right) => right.noiseScore - left.noiseScore);

  const sheets = [...sheetMap.values()]
    .map((entry) => {
      const total = entry.mapCount + entry.ignoreCount;
      const noiseRatio = total > 0 ? round(entry.ignoreCount / total, 2) : 0;
      return {
        docType: entry.docType,
        sheetName: entry.sheetName || '(unnamed sheet)',
        mapCount: entry.mapCount,
        ignoreCount: entry.ignoreCount,
        noiseRatio,
        trustAdjustment: round(clamp((entry.mapCount * 0.03) - (entry.ignoreCount * 0.04), -0.1, 0.08)),
      };
    })
    .sort((left, right) => {
      if (right.noiseRatio !== left.noiseRatio) return right.noiseRatio - left.noiseRatio;
      return right.ignoreCount - left.ignoreCount;
    });

  return {
    generatedAt: new Date().toISOString(),
    ruleCount: normalizedRules.length,
    explicitActionCount: sourcePreferences.length + conceptSuppressions.length + timeBasisOverrides.length + entityResolutions.length,
    docTypes,
    fields: fields.slice(0, 18),
    labelMappings: labelMappings.slice(0, 18),
    noisyLabels: noisyLabels.slice(0, 12),
    noisySheets: sheets.slice(0, 8).filter((entry) => entry.ignoreCount > 0),
    sourcePreferences,
    sourcePreferenceCount: sourcePreferences.length,
    conceptSuppressions,
    conceptSuppressionCount: conceptSuppressions.length,
    timeBasisOverrides,
    timeBasisOverrideCount: timeBasisOverrides.length,
    entityResolutions,
    entityResolutionCount: entityResolutions.length,
    summary: normalizedRules.length === 0 && sourcePreferences.length === 0 && conceptSuppressions.length === 0 && timeBasisOverrides.length === 0 && entityResolutions.length === 0
      ? 'No persisted reviewer decisions are influencing evidence ranking yet.'
      : normalizedRules.length > 0
        ? `${normalizedRules.length} saved reviewer decision${normalizedRules.length === 1 ? '' : 's'} are shaping document-family ranking, label trust, and noise suppression.`
        : 'Explicit reviewer actions are overriding default evidence ranking even though no row-level map/ignore rules are stored yet.',
  };
}

export function buildReviewMemoryBundle({
  reviewOverrides = [],
  sourcePreferences = [],
  conceptSuppressions = [],
  timeBasisOverrides = [],
  entityResolutions = [],
} = {}) {
  const normalizedOverrides = normalizeReviewRules(reviewOverrides);
  const normalizedSourcePreferences = normalizeSourcePreferences(sourcePreferences);
  const normalizedConceptSuppressions = normalizeConceptSuppressions(conceptSuppressions);
  const normalizedTimeBasisOverrides = normalizeTimeBasisOverrides(timeBasisOverrides);
  const normalizedEntityResolutions = normalizeEntityResolutions(entityResolutions);
  const learnedAliasRules = deriveLearnedAliasRulesFromReviewOverrides(normalizedOverrides);
  const reviewerSignals = buildReviewerRankingSignals({
    reviewOverrides: normalizedOverrides,
    sourcePreferences: normalizedSourcePreferences,
    conceptSuppressions: normalizedConceptSuppressions,
    timeBasisOverrides: normalizedTimeBasisOverrides,
    entityResolutions: normalizedEntityResolutions,
  });
  const sourcePreferenceSummary = normalizedSourcePreferences.length > 0
    ? `${normalizedSourcePreferences.length} explicit source preference${normalizedSourcePreferences.length === 1 ? '' : 's'} are overriding default evidence ranking where needed.`
    : '';
  const suppressionSummary = normalizedConceptSuppressions.length > 0
    ? `${normalizedConceptSuppressions.length} source suppression${normalizedConceptSuppressions.length === 1 ? '' : 's'} are excluding low-trust evidence from specific concepts.`
    : '';
  const timeBasisSummary = normalizedTimeBasisOverrides.length > 0
    ? `${normalizedTimeBasisOverrides.length} reviewer time-basis override${normalizedTimeBasisOverrides.length === 1 ? '' : 's'} are correcting period interpretation.`
    : '';
  const entitySummary = normalizedEntityResolutions.length > 0
    ? `${normalizedEntityResolutions.length} reviewer entity resolution${normalizedEntityResolutions.length === 1 ? '' : 's'} are anchoring ambiguous aliases.`
    : '';

  return {
    reviewOverrides: normalizedOverrides,
    sourcePreferences: normalizedSourcePreferences,
    conceptSuppressions: normalizedConceptSuppressions,
    timeBasisOverrides: normalizedTimeBasisOverrides,
    entityResolutions: normalizedEntityResolutions,
    learnedAliasRules,
    reviewerSignals: {
      ...reviewerSignals,
      summary: [
        reviewerSignals.summary,
        sourcePreferenceSummary,
        suppressionSummary,
        timeBasisSummary,
        entitySummary,
      ].filter(Boolean).join(' '),
    },
  };
}

export function getStoredOverrideForRow(context, rules = loadReviewOverrides()) {
  const id = buildRuleId(context);
  return (Array.isArray(rules) ? rules : []).find((rule) => rule.id === id) || null;
}

export function getStoredSourcePreference(context, preferences = loadSourcePreferences()) {
  const normalized = normalizeSourcePreference(context);
  if (!normalized) return null;
  return (Array.isArray(preferences) ? preferences : []).find((entry) => (
    entry.conceptKey === normalized.conceptKey
    && entry.preferredDocType === normalized.preferredDocType
  )) || null;
}

export function saveSourcePreference(preferenceInput, options = {}) {
  const preferences = Array.isArray(options.preferences) ? options.preferences : loadSourcePreferences();
  const nextPreferences = upsertSourcePreference(preferences, preferenceInput);
  if (options.persist !== false) {
    persistSourcePreferences(nextPreferences);
  }
  return nextPreferences;
}

export function removeSourcePreference(preferenceInput, options = {}) {
  const preferences = Array.isArray(options.preferences) ? options.preferences : loadSourcePreferences();
  const nextPreferences = deleteSourcePreference(preferences, preferenceInput);
  if (options.persist !== false) {
    persistSourcePreferences(nextPreferences);
  }
  return nextPreferences;
}

export function getStoredConceptSuppression(context, suppressions = loadConceptSuppressions()) {
  const normalized = normalizeConceptSuppression(context);
  if (!normalized) return null;
  return (Array.isArray(suppressions) ? suppressions : []).find((entry) => (
    entry.conceptKey === normalized.conceptKey
    && entry.docType === normalized.docType
    && entry.sourceRefKey === normalized.sourceRefKey
  )) || null;
}

export function saveConceptSuppression(suppressionInput, options = {}) {
  const suppressions = Array.isArray(options.suppressions) ? options.suppressions : loadConceptSuppressions();
  const nextSuppressions = upsertConceptSuppression(suppressions, suppressionInput);
  if (options.persist !== false) {
    persistConceptSuppressions(nextSuppressions);
  }
  return nextSuppressions;
}

export function removeConceptSuppression(suppressionInput, options = {}) {
  const suppressions = Array.isArray(options.suppressions) ? options.suppressions : loadConceptSuppressions();
  const nextSuppressions = deleteConceptSuppression(suppressions, suppressionInput);
  if (options.persist !== false) {
    persistConceptSuppressions(nextSuppressions);
  }
  return nextSuppressions;
}

export function getStoredTimeBasisOverride(context, overrides = loadTimeBasisOverrides()) {
  const normalized = normalizeTimeBasisOverride(context);
  if (!normalized) return null;
  return (Array.isArray(overrides) ? overrides : []).find((entry) => (
    entry.docType === normalized.docType
    && entry.sourceRefKey === normalized.sourceRefKey
  )) || null;
}

export function saveTimeBasisOverride(overrideInput, options = {}) {
  const overrides = Array.isArray(options.overrides) ? options.overrides : loadTimeBasisOverrides();
  const nextOverrides = upsertTimeBasisOverride(overrides, overrideInput);
  if (options.persist !== false) {
    persistTimeBasisOverrides(nextOverrides);
  }
  return nextOverrides;
}

export function removeTimeBasisOverride(overrideInput, options = {}) {
  const overrides = Array.isArray(options.overrides) ? options.overrides : loadTimeBasisOverrides();
  const nextOverrides = deleteTimeBasisOverride(overrides, overrideInput);
  if (options.persist !== false) {
    persistTimeBasisOverrides(nextOverrides);
  }
  return nextOverrides;
}

export function getStoredEntityResolution(context, resolutions = loadEntityResolutions()) {
  const normalized = normalizeEntityResolution(context);
  if (!normalized) return null;
  return (Array.isArray(resolutions) ? resolutions : []).find((entry) => entry.id === normalized.id) || null;
}

export function saveEntityResolution(resolutionInput, options = {}) {
  const resolutions = Array.isArray(options.resolutions) ? options.resolutions : loadEntityResolutions();
  const nextResolutions = upsertEntityResolution(resolutions, resolutionInput);
  if (options.persist !== false) {
    persistEntityResolutions(nextResolutions);
  }
  return nextResolutions;
}

export function removeEntityResolution(resolutionInput, options = {}) {
  const resolutions = Array.isArray(options.resolutions) ? options.resolutions : loadEntityResolutions();
  const nextResolutions = deleteEntityResolution(resolutions, resolutionInput);
  if (options.persist !== false) {
    persistEntityResolutions(nextResolutions);
  }
  return nextResolutions;
}

export function getReviewOverrideFieldOptions(docType) {
  const schema = FIELD_SCHEMAS[docType];
  if (!schema) return [];

  return [
    ...(schema.requiredFields || []),
    ...(schema.optionalFields || []),
  ]
    .filter((field) => field.type !== 'array')
    .map((field) => ({
      value: field.name,
      label: humanizeFieldName(field.name),
      required: (schema.requiredFields || []).some((item) => item.name === field.name),
    }));
}

export function applyReviewOverridesToIngestionResponse(ingestionResponse, rules = loadReviewOverrides()) {
  const normalizedRules = (rules || []).map(normalizeRule);

  if (!ingestionResponse || !Array.isArray(ingestionResponse.files) || normalizedRules.length === 0) {
    return ingestionResponse;
  }

  const cloned = cloneValue(ingestionResponse);

  cloned.files.forEach((fileResult) => {
    const splitDocuments = Array.isArray(fileResult.splitDocuments) ? fileResult.splitDocuments : [];
    splitDocuments.forEach((splitDocument) => {
      const applicableRules = normalizedRules.filter((rule) => (
        rule.docType === splitDocument.docType
        && rule.sheetNameNormalized === normalizeLabel(splitDocument.sheetName)
      ));

      if (applicableRules.length === 0) return;

      const sourceSheet = fileResult?.parsing?.parsedDocument?.sheets?.find((sheet) => sheet.name === splitDocument.sheetName);
      if (!sourceSheet) return;

      const overrideContext = buildOverrideContext(sourceSheet, splitDocument.extraction?.periods || []);
      const appliedActions = [];

      applicableRules.forEach((rule) => {
        if (rule.action === 'ignore') {
          if (markIgnoredRow(splitDocument.extraction, rule)) {
            appliedActions.push(rule);
          }
          return;
        }

        if (applyMappedOverride(splitDocument.extraction, splitDocument.docType, overrideContext, rule)) {
          appliedActions.push(rule);
        }
      });

      if (appliedActions.length === 0) return;

      finalizeExtractionAfterOverrides(splitDocument, appliedActions);
    });
  });

  return cloned;
}

function finalizeExtractionAfterOverrides(splitDocument, appliedActions) {
  const extraction = splitDocument.extraction || {};
  const schema = FIELD_SCHEMAS[splitDocument.docType];
  const periods = extraction.periods || [];
  const sampleKey = periods[0] || '_single';
  const sampleData = extraction.data?.[sampleKey] || extraction.data?._single || {};
  const allFields = [
    ...(schema?.requiredFields || []),
    ...(schema?.optionalFields || []),
  ];
  const foundFields = allFields.filter((field) => sampleData[field.name] != null);
  const missingFields = allFields.filter((field) => sampleData[field.name] == null).map((field) => field.name);

  extraction.coverage = {
    total: allFields.length,
    found: foundFields.length,
    missing: missingFields,
    percentage: allFields.length === 0 ? 100 : round((foundFields.length / allFields.length) * 100),
  };
  extraction.missingFields = missingFields;
  extraction.usable = foundFields.length >= (schema?.minUsabilityFields || 0);
  extraction.interpretability = summarizeInterpretability(extraction.provenance);
  extraction.confidence = Math.min(0.95, round((extraction.confidence || 0) + appliedActions.filter((rule) => rule.action === 'map').length * 0.04));
  extraction.warnings = [
    ...(extraction.warnings || []).filter((warning) => !warning.startsWith('Manual override applied')),
    `Manual override applied: ${appliedActions.map((action) => `${humanizeFieldName(action.fieldName || action.action)} for "${action.rowLabel}"`).join('; ')}.`,
  ];

  splitDocument.normalization = {
    ...(splitDocument.normalization || {}),
    status: extraction.usable ? 'ready' : (splitDocument.normalization?.status || 'partial'),
    readyForPipeline: extraction.usable,
    normalizedDocument: extraction.usable
      ? { docType: splitDocument.docType, periods: extraction.periods, data: extraction.data }
      : splitDocument.normalization?.normalizedDocument || null,
    pipelineContent: extraction.usable
      ? { __parsed: true, periods: extraction.periods, data: extraction.data }
      : splitDocument.normalization?.pipelineContent || null,
  };
}

function applyMappedOverride(extraction, docType, overrideContext, rule) {
  const row = overrideContext.rowsByLabel.get(rule.rowLabelNormalized);
  if (!row) return false;

  const fieldSchema = getFieldSchema(docType, rule.fieldName);
  if (!fieldSchema || fieldSchema.type === 'array') return false;

  if (!extraction.data) extraction.data = {};
  if (!Array.isArray(extraction.periods) || extraction.periods.length === 0) {
    extraction.periods = ['_single'];
  }

  let applied = false;

  if (extraction.periods.includes('_single')) {
    if (!extraction.data._single) extraction.data._single = {};
    const value = getSingleValueForField(fieldSchema, row, overrideContext);
    if (value != null) {
      extraction.data._single[rule.fieldName] = value;
      applied = true;
      addOverrideMappedRow(extraction, rule, value, '_single');
    }
  } else {
    extraction.periods.forEach((period) => {
      const periodColumn = overrideContext.periodColumns.find((column) => column.periodKey === period);
      if (!periodColumn) return;

      const rawValue = row.values[periodColumn.columnKey];
      const value = coerceFieldValue(fieldSchema, rawValue, overrideContext.valueScale);
      if (value == null) return;

      if (!extraction.data[period]) extraction.data[period] = {};
      extraction.data[period][rule.fieldName] = value;
      applied = true;
      addOverrideMappedRow(extraction, rule, rawValue, period);
    });
  }

  if (!applied) return false;

  extraction.provenance = extraction.provenance || createEmptyProvenance();
  extraction.provenance.unmappedRows = (extraction.provenance.unmappedRows || []).filter((entry) => normalizeLabel(entry.rowLabel) !== rule.rowLabelNormalized);
  extraction.provenance.ambiguousRows = (extraction.provenance.ambiguousRows || []).filter((entry) => normalizeLabel(entry.rowLabel) !== rule.rowLabelNormalized);
  extraction.provenance.lowConfidenceRows = (extraction.provenance.lowConfidenceRows || []).filter((entry) => normalizeLabel(entry.rowLabel) !== rule.rowLabelNormalized);

  return true;
}

function markIgnoredRow(extraction, rule) {
  const provenance = extraction?.provenance;
  if (!provenance) return false;

  const beforeUnmapped = provenance.unmappedRows?.length || 0;
  const beforeAmbiguous = provenance.ambiguousRows?.length || 0;
  const beforeLowConfidence = provenance.lowConfidenceRows?.length || 0;
  provenance.unmappedRows = (provenance.unmappedRows || []).filter((entry) => normalizeLabel(entry.rowLabel) !== rule.rowLabelNormalized);
  provenance.ambiguousRows = (provenance.ambiguousRows || []).filter((entry) => normalizeLabel(entry.rowLabel) !== rule.rowLabelNormalized);
  provenance.lowConfidenceRows = (provenance.lowConfidenceRows || []).filter((entry) => normalizeLabel(entry.rowLabel) !== rule.rowLabelNormalized);

  return beforeUnmapped !== (provenance.unmappedRows?.length || 0)
    || beforeAmbiguous !== (provenance.ambiguousRows?.length || 0)
    || beforeLowConfidence !== (provenance.lowConfidenceRows?.length || 0);
}

function addOverrideMappedRow(extraction, rule, rawValue, period) {
  extraction.provenance = extraction.provenance || createEmptyProvenance();
  extraction.provenance.mappedRows = extraction.provenance.mappedRows || [];
  extraction.provenance.mappedRows.push({
    fieldName: rule.fieldName,
    rowLabel: rule.rowLabel,
    rowIndex: null,
    period,
    rawValue,
    sourceType: 'manual_override',
    matchAlias: 'manual override',
    matchType: 'manual',
    candidateFields: [rule.fieldName],
  });
}

function buildOverrideContext(sheet, targetPeriods) {
  const view = getMatrixSheetView(sheet);

  return {
    valueScale: sheet.valueScale || 1,
    periodColumns: dedupePeriodColumns(detectPeriodColumns(view.valueColumns))
      .filter((column) => targetPeriods.length === 0 || targetPeriods.includes(column.periodKey)),
    rowsByLabel: new Map(view.records.map((record) => [
      normalizeLabel(record.values[view.labelColumn]),
      record,
    ]).filter(([label]) => label)),
    records: view.records,
  };
}

function getSingleValueForField(fieldSchema, row, overrideContext) {
  const entries = Object.entries(row.values).filter(([columnKey]) => columnKey !== Object.keys(row.values)[0]);
  for (const [, rawValue] of entries) {
    const value = coerceFieldValue(fieldSchema, rawValue, overrideContext.valueScale);
    if (value != null) return value;
  }
  return null;
}

function getFieldSchema(docType, fieldName) {
  const schema = FIELD_SCHEMAS[docType];
  if (!schema) return null;

  return [
    ...(schema.requiredFields || []),
    ...(schema.optionalFields || []),
  ].find((field) => field.name === fieldName) || null;
}

function coerceFieldValue(fieldSchema, rawValue, valueScale = 1) {
  if (!fieldSchema) return null;
  if (fieldSchema.type === 'string') return rawValue == null ? null : String(rawValue);
  if (fieldSchema.type === 'date') return toDateValue(rawValue);
  if (fieldSchema.type === 'percentage') return toPercentageValue(rawValue);
  if (fieldSchema.type === 'integer') {
    const value = toNumericValue(rawValue);
    return value == null ? null : Math.round(value);
  }
  if (fieldSchema.type === 'currency') {
    const value = toNumericValue(rawValue);
    return value == null ? null : round(value * valueScale);
  }

  return toNumericValue(rawValue);
}

function dedupePeriodColumns(columns) {
  const seen = new Set();
  return columns.filter((column) => {
    if (seen.has(column.periodKey)) return false;
    seen.add(column.periodKey);
    return true;
  });
}

function normalizeRule(ruleInput) {
  return {
    id: buildRuleId(ruleInput),
    docType: ruleInput.docType,
    sheetName: ruleInput.sheetName || '',
    sheetNameNormalized: normalizeLabel(ruleInput.sheetName || ''),
    rowLabel: ruleInput.rowLabel || '',
    rowLabelNormalized: normalizeLabel(ruleInput.rowLabel || ''),
    action: ruleInput.action,
    fieldName: ruleInput.fieldName || '',
    createdAt: ruleInput.createdAt || new Date().toISOString(),
  };
}

export function normalizeReviewRule(ruleInput) {
  return normalizeRule(ruleInput);
}

export function normalizeReviewRules(rules = []) {
  if (!Array.isArray(rules)) return [];
  return rules
    .map(normalizeRule)
    .filter((rule) => rule.docType && rule.rowLabelNormalized && rule.action);
}

function buildRuleId(ruleInput) {
  return [
    ruleInput.docType || '',
    normalizeLabel(ruleInput.sheetName || ''),
    normalizeLabel(ruleInput.rowLabel || ''),
    ruleInput.action || '',
  ].join('::');
}

export function upsertReviewOverrideRule(rules = [], ruleInput) {
  const normalizedRule = normalizeRule(ruleInput);
  return [
    ...normalizeReviewRules(rules).filter((rule) => rule.id !== normalizedRule.id),
    normalizedRule,
  ];
}

export function deleteReviewOverrideRule(rules = [], ruleInput) {
  const id = buildRuleId(ruleInput);
  return normalizeReviewRules(rules).filter((rule) => rule.id !== id);
}

function normalizeLearnedAliasRule(ruleInput) {
  const alias = String(ruleInput.alias || ruleInput.rowLabel || '').trim();
  const fieldName = ruleInput.fieldName || '';
  const docType = ruleInput.docType || '';
  if (!alias || !fieldName || !docType) return null;

  return {
    docType,
    fieldName,
    alias,
    aliasNormalized: normalizeLabel(alias),
    source: ruleInput.source || 'manual_override',
    createdAt: ruleInput.createdAt || new Date().toISOString(),
  };
}

export function deriveLearnedAliasRulesFromReviewOverrides(rules = []) {
  return normalizeReviewRules(rules)
    .filter((rule) => rule.action === 'map' && rule.fieldName && rule.rowLabel)
    .map((rule) => normalizeLearnedAliasRule({
      docType: rule.docType,
      fieldName: rule.fieldName,
      alias: rule.rowLabel,
      source: 'manual_override',
      createdAt: rule.createdAt,
    }))
    .filter(Boolean);
}

function normalizeSourcePreference(preferenceInput) {
  const conceptKey = String(preferenceInput?.conceptKey || '').trim();
  const preferredDocType = String(preferenceInput?.preferredDocType || '').trim();
  if (!conceptKey || !preferredDocType) return null;

  return {
    conceptKey,
    preferredDocType,
    sourceRefKey: String(preferenceInput?.sourceRefKey || '').trim(),
    sourceRefLabel: String(preferenceInput?.sourceRefLabel || '').trim(),
    createdAt: preferenceInput?.createdAt || new Date().toISOString(),
  };
}

export function normalizeSourcePreferences(preferences = []) {
  if (!Array.isArray(preferences)) return [];
  const seen = new Set();
  return preferences
    .map(normalizeSourcePreference)
    .filter((entry) => {
      if (!entry) return false;
      if (seen.has(entry.conceptKey)) return false;
      seen.add(entry.conceptKey);
      return true;
    });
}

export function upsertSourcePreference(preferences = [], preferenceInput) {
  const normalized = normalizeSourcePreference(preferenceInput);
  if (!normalized) return normalizeSourcePreferences(preferences);
  return [
    ...normalizeSourcePreferences(preferences).filter((entry) => entry.conceptKey !== normalized.conceptKey),
    normalized,
  ];
}

export function deleteSourcePreference(preferences = [], preferenceInput) {
  const conceptKey = String(preferenceInput?.conceptKey || '').trim();
  if (!conceptKey) return normalizeSourcePreferences(preferences);
  return normalizeSourcePreferences(preferences).filter((entry) => entry.conceptKey !== conceptKey);
}

function normalizeConceptSuppression(suppressionInput) {
  const conceptKey = String(suppressionInput?.conceptKey || '').trim();
  const docType = String(suppressionInput?.docType || '').trim();
  if (!conceptKey || !docType) return null;

  return {
    conceptKey,
    docType,
    sourceRefKey: String(suppressionInput?.sourceRefKey || '').trim(),
    sourceRefLabel: String(suppressionInput?.sourceRefLabel || '').trim(),
    createdAt: suppressionInput?.createdAt || new Date().toISOString(),
  };
}

export function normalizeConceptSuppressions(suppressions = []) {
  if (!Array.isArray(suppressions)) return [];
  const seen = new Set();
  return suppressions
    .map(normalizeConceptSuppression)
    .filter((entry) => {
      if (!entry) return false;
      const id = buildConceptSuppressionId(entry);
      if (seen.has(id)) return false;
      seen.add(id);
      return true;
    });
}

export function upsertConceptSuppression(suppressions = [], suppressionInput) {
  const normalized = normalizeConceptSuppression(suppressionInput);
  if (!normalized) return normalizeConceptSuppressions(suppressions);
  const id = buildConceptSuppressionId(normalized);
  return [
    ...normalizeConceptSuppressions(suppressions).filter((entry) => buildConceptSuppressionId(entry) !== id),
    normalized,
  ];
}

export function deleteConceptSuppression(suppressions = [], suppressionInput) {
  const normalized = normalizeConceptSuppression(suppressionInput);
  if (!normalized) return normalizeConceptSuppressions(suppressions);
  const id = buildConceptSuppressionId(normalized);
  return normalizeConceptSuppressions(suppressions).filter((entry) => buildConceptSuppressionId(entry) !== id);
}

function normalizeTimeBasisOverride(overrideInput) {
  const docType = String(overrideInput?.docType || '').trim();
  const sourceRefKey = String(overrideInput?.sourceRefKey || '').trim();
  const basis = String(overrideInput?.basis || '').trim();
  if (!docType || !sourceRefKey || !basis) return null;

  return {
    docType,
    sourceRefKey,
    sourceRefLabel: String(overrideInput?.sourceRefLabel || '').trim(),
    basis,
    createdAt: overrideInput?.createdAt || new Date().toISOString(),
  };
}

export function normalizeTimeBasisOverrides(overrides = []) {
  if (!Array.isArray(overrides)) return [];
  const seen = new Set();
  return overrides
    .map(normalizeTimeBasisOverride)
    .filter((entry) => {
      if (!entry) return false;
      const id = buildTimeBasisOverrideId(entry);
      if (seen.has(id)) return false;
      seen.add(id);
      return true;
    });
}

export function upsertTimeBasisOverride(overrides = [], overrideInput) {
  const normalized = normalizeTimeBasisOverride(overrideInput);
  if (!normalized) return normalizeTimeBasisOverrides(overrides);
  const id = buildTimeBasisOverrideId(normalized);
  return [
    ...normalizeTimeBasisOverrides(overrides).filter((entry) => buildTimeBasisOverrideId(entry) !== id),
    normalized,
  ];
}

export function deleteTimeBasisOverride(overrides = [], overrideInput) {
  const normalized = normalizeTimeBasisOverride(overrideInput);
  if (!normalized) return normalizeTimeBasisOverrides(overrides);
  const id = buildTimeBasisOverrideId(normalized);
  return normalizeTimeBasisOverrides(overrides).filter((entry) => buildTimeBasisOverrideId(entry) !== id);
}

function normalizeEntityResolution(resolutionInput) {
  const kind = String(resolutionInput?.kind || '').trim();
  const aliases = [...new Set((resolutionInput?.aliases || [])
    .map((value) => String(value || '').trim())
    .filter(Boolean))];
  const canonicalName = String(resolutionInput?.canonicalName || aliases[0] || '').trim();
  if (!kind || aliases.length < 2 || !canonicalName) return null;

  const aliasKeys = aliases.map((value) => normalizeLabel(value)).filter(Boolean).sort();
  const id = buildEntityResolutionId({ kind, aliasKeys });

  return {
    id,
    kind,
    canonicalName,
    aliases,
    aliasKeys,
    createdAt: resolutionInput?.createdAt || new Date().toISOString(),
  };
}

export function normalizeEntityResolutions(resolutions = []) {
  if (!Array.isArray(resolutions)) return [];
  const seen = new Set();
  return resolutions
    .map(normalizeEntityResolution)
    .filter((entry) => {
      if (!entry) return false;
      if (seen.has(entry.id)) return false;
      seen.add(entry.id);
      return true;
    });
}

export function upsertEntityResolution(resolutions = [], resolutionInput) {
  const normalized = normalizeEntityResolution(resolutionInput);
  if (!normalized) return normalizeEntityResolutions(resolutions);
  return [
    ...normalizeEntityResolutions(resolutions).filter((entry) => entry.id !== normalized.id),
    normalized,
  ];
}

export function deleteEntityResolution(resolutions = [], resolutionInput) {
  const normalized = normalizeEntityResolution(resolutionInput);
  if (!normalized) return normalizeEntityResolutions(resolutions);
  return normalizeEntityResolutions(resolutions).filter((entry) => entry.id !== normalized.id);
}

function buildConceptSuppressionId(entry) {
  return [
    entry.conceptKey || '',
    entry.docType || '',
    entry.sourceRefKey || '',
  ].join('::');
}

function buildTimeBasisOverrideId(entry) {
  return [
    entry.docType || '',
    entry.sourceRefKey || '',
  ].join('::');
}

function buildEntityResolutionId(entry) {
  return [
    entry.kind || '',
    ...((entry.aliasKeys || []).slice().sort()),
  ].join('::');
}

function buildLearnedAliasId(ruleInput) {
  return [
    ruleInput.docType || '',
    ruleInput.fieldName || '',
    normalizeLabel(ruleInput.alias || ''),
  ].join('::');
}

function getOrCreateProfileEntry(map, key, factory) {
  if (!map.has(key)) map.set(key, factory());
  return map.get(key);
}

function persistRules(rules) {
  if (typeof localStorage === 'undefined') return;
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(rules));
  } catch (_error) {
    // Ignore storage quota/privacy failures so review actions do not crash the UI.
  }
}

function saveLearnedAliasRule(ruleInput) {
  const normalizedRule = normalizeLearnedAliasRule(ruleInput);
  if (!normalizedRule) return;

  const nextRules = [
    ...loadLearnedAliasRules().filter((rule) => buildLearnedAliasId(rule) !== buildLearnedAliasId(normalizedRule)),
    normalizedRule,
  ];
  persistLearnedAliasRules(nextRules);
}

function removeLearnedAliasRule(ruleInput) {
  const normalizedRule = normalizeLearnedAliasRule(ruleInput);
  if (!normalizedRule) return;

  const nextRules = loadLearnedAliasRules()
    .filter((rule) => buildLearnedAliasId(rule) !== buildLearnedAliasId(normalizedRule));
  persistLearnedAliasRules(nextRules);
}

function persistLearnedAliasRules(rules) {
  if (typeof localStorage === 'undefined') return;
  try {
    localStorage.setItem(ALIAS_STORAGE_KEY, JSON.stringify(rules));
  } catch (_error) {
    // Ignore storage quota/privacy failures so review actions do not crash the UI.
  }
}

function persistSourcePreferences(preferences) {
  if (typeof localStorage === 'undefined') return;
  try {
    localStorage.setItem(SOURCE_PREFERENCE_STORAGE_KEY, JSON.stringify(preferences));
  } catch (_error) {
    // Ignore storage quota/privacy failures so review actions do not crash the UI.
  }
}

function persistConceptSuppressions(suppressions) {
  if (typeof localStorage === 'undefined') return;
  try {
    localStorage.setItem(CONCEPT_SUPPRESSION_STORAGE_KEY, JSON.stringify(suppressions));
  } catch (_error) {
    // Ignore storage quota/privacy failures so review actions do not crash the UI.
  }
}

function persistTimeBasisOverrides(overrides) {
  if (typeof localStorage === 'undefined') return;
  try {
    localStorage.setItem(TIME_BASIS_OVERRIDE_STORAGE_KEY, JSON.stringify(overrides));
  } catch (_error) {
    // Ignore storage quota/privacy failures so review actions do not crash the UI.
  }
}

function persistEntityResolutions(resolutions) {
  if (typeof localStorage === 'undefined') return;
  try {
    localStorage.setItem(ENTITY_RESOLUTION_STORAGE_KEY, JSON.stringify(resolutions));
  } catch (_error) {
    // Ignore storage quota/privacy failures so review actions do not crash the UI.
  }
}

function cloneValue(value) {
  if (typeof structuredClone === 'function') return structuredClone(value);
  if (value === undefined) return undefined;
  return JSON.parse(JSON.stringify(value));
}

function createEmptyProvenance() {
  return {
    mappedRows: [],
    derivedFields: [],
    ambiguousRows: [],
    unmappedRows: [],
    lowConfidenceRows: [],
  };
}

function summarizeInterpretability(provenance) {
  const mappedRows = provenance?.mappedRows || [];
  const ambiguousCount = provenance?.ambiguousRows?.length || 0;
  const unmappedCount = provenance?.unmappedRows?.length || 0;
  const lowConfidenceCount = provenance?.lowConfidenceRows?.length || 0;
  const reviewLoad = ambiguousCount + unmappedCount + lowConfidenceCount;

  return {
    mappedCount: mappedRows.length,
    derivedCount: provenance?.derivedFields?.length || 0,
    ambiguousCount,
    unmappedCount,
    lowConfidenceCount,
    exactMatchCount: mappedRows.filter((entry) => entry.matchType === 'exact').length,
    heuristicMatchCount: mappedRows.filter((entry) => entry.sourceType !== 'manual_override' && entry.matchType && entry.matchType !== 'exact').length,
    manualMatchCount: mappedRows.filter((entry) => entry.sourceType === 'manual_override').length,
    needsReview: Boolean(reviewLoad),
    reviewPriority: reviewLoad >= 6 ? 'high' : reviewLoad >= 3 ? 'medium' : reviewLoad > 0 ? 'low' : 'none',
    recommendations: [
      ...(ambiguousCount > 0 ? ['Resolve competing row-to-field matches before relying on scored outputs.'] : []),
      ...(unmappedCount > 0 ? ['Review excluded source rows and map material line items into the schema.'] : []),
      ...(lowConfidenceCount > 0 ? ['Spot-check heuristic mappings that were accepted with weak label similarity.'] : []),
    ],
  };
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function getMatrixSheetView(sheet) {
  const labelColumn = sheet.header[0];
  const baseValueColumns = (sheet.columns || []).slice(1).map((column) => ({
    columnKey: column.header,
    header: column.header,
  }));
  const firstRecord = sheet.records?.[0];

  if (!firstRecord) {
    return {
      labelColumn,
      valueColumns: baseValueColumns,
      records: sheet.records || [],
    };
  }

  const candidateValueColumns = baseValueColumns.map((column) => ({
    columnKey: column.columnKey,
    header: firstRecord.values[column.columnKey] ?? column.header,
  }));
  const candidatePeriods = detectPeriodColumns(candidateValueColumns);

  if (candidatePeriods.length >= 2) {
    return {
      labelColumn,
      valueColumns: candidateValueColumns,
      records: (sheet.records || []).slice(1),
    };
  }

  return {
    labelColumn,
    valueColumns: baseValueColumns,
    records: sheet.records || [],
  };
}

function detectPeriodColumns(columns = []) {
  return columns
    .filter((column) => shouldTreatAsValuePeriodColumn(column.header))
    .map((column) => {
      const period = parsePeriodLabel(column.header);
      if (!period) return null;

      return {
        columnKey: column.columnKey ?? column.header,
        periodKey: period.periodKey,
        sortValue: period.sortValue,
      };
    })
    .filter(Boolean)
    .sort((left, right) => left.sortValue - right.sortValue);
}

function shouldTreatAsValuePeriodColumn(label) {
  const normalized = String(label || '').toLowerCase().replace(/\s+/g, ' ').trim();
  if (!normalized) return false;
  if (!/\b(19|20)\d{2}\b/.test(normalized) && !/\b(fy\s*)?(19|20)\d{2}\b/.test(normalized)) {
    return true;
  }

  return !/(^|\s)(yoy|margin|mix|growth|cagr|percent|%)(\s|$)/.test(normalized);
}

function parsePeriodLabel(label) {
  const raw = String(label || '').trim();
  if (!raw) return null;

  const normalized = raw.toLowerCase().replace(/\s+/g, ' ').trim();
  const yearMatch = normalized.match(/^(fy\s*)?((19|20)\d{2})(e)?$/);
  if (yearMatch) {
    const year = Number(yearMatch[2]);
    return { periodKey: String(year), sortValue: year * 100 };
  }

  const yearWithinLabel = normalized.match(/(?:^|[^a-z0-9])(?:fy\s*)?((19|20)\d{2})(e)?(?:[^a-z0-9]|$)/);
  if (yearWithinLabel) {
    const year = Number(yearWithinLabel[1]);
    return { periodKey: String(year), sortValue: year * 100 };
  }

  return null;
}

function normalizeLabel(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[_./\\\-]+/g, ' ')
    .replace(/[()]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function toNumericValue(value) {
  if (value == null || value === '') return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  if (typeof value === 'boolean') return value ? 1 : 0;

  const raw = String(value).trim();
  if (!raw) return null;

  const negative = /^\(.*\)$/.test(raw);
  const cleaned = raw
    .replace(/[,$]/g, '')
    .replace(/[%]/g, '')
    .replace(/[()]/g, '')
    .trim();

  const parsed = Number(cleaned);
  if (!Number.isFinite(parsed)) return null;
  return negative ? -parsed : parsed;
}

function toPercentageValue(value) {
  const numeric = toNumericValue(value);
  if (numeric == null) {
    const raw = String(value || '');
    const percentMatch = raw.match(/(-?\d+(?:\.\d+)?)\s*%/);
    if (!percentMatch) return null;
    return round(Number(percentMatch[1]));
  }
  return Math.abs(numeric) <= 1 ? round(numeric * 100) : round(numeric);
}

function toDateValue(value) {
  if (value == null || value === '') return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

function round(value) {
  return Math.round(value * 100) / 100;
}

function humanizeFieldName(value = '') {
  return String(value)
    .replace(/^__/, '')
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (match) => match.toUpperCase());
}
