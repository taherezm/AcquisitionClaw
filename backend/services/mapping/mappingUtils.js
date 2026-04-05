import { FIELD_SCHEMAS } from '../../../ingestion/schemas.js';

export function normalizeLabel(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[_./\\\-]+/g, ' ')
    .replace(/[()]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

export function matchFieldByAliases(label, aliasMap = {}) {
  const normalizedLabel = normalizeLabel(label);
  if (!normalizedLabel) return null;

  let bestMatch = null;

  for (const [fieldName, aliases] of Object.entries(aliasMap)) {
    for (const alias of aliases) {
      const normalizedAlias = normalizeLabel(alias);
      const exact = normalizedLabel === normalizedAlias;
      const includes = normalizedLabel.includes(normalizedAlias) || normalizedAlias.includes(normalizedLabel);
      if (!exact && !includes) continue;

      const score = exact ? 2 : 1;
      if (!bestMatch || score > bestMatch.score || normalizedAlias.length > bestMatch.alias.length) {
        bestMatch = {
          fieldName,
          alias: normalizedAlias,
          score,
        };
      }
    }
  }

  return bestMatch?.fieldName || null;
}

export function toNumericValue(value) {
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

export function toPercentageValue(value) {
  const numeric = toNumericValue(value);
  if (numeric == null) return null;
  return Math.abs(numeric) <= 1 ? round(numeric * 100) : round(numeric);
}

export function toDateValue(value) {
  if (value == null || value === '') return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

export function buildCoverage(docType, periods, data) {
  const schema = FIELD_SCHEMAS[docType];
  const allFields = [
    ...(schema?.requiredFields || []),
    ...(schema?.optionalFields || []),
  ];

  const samplePeriod = periods[0] || '_single';
  const sampleData = data[samplePeriod] || data._single || data;
  const found = allFields.filter((field) => sampleData[field.name] != null);
  const missing = allFields
    .filter((field) => sampleData[field.name] == null)
    .map((field) => field.name);

  return {
    total: allFields.length,
    found: found.length,
    missing,
    percentage: allFields.length === 0 ? 100 : round((found.length / allFields.length) * 100),
  };
}

export function computeMappingConfidence({
  docType,
  periods,
  data,
  directMatches = 0,
  derivedFields = 0,
}) {
  const schema = FIELD_SCHEMAS[docType];
  const requiredCount = schema?.requiredFields?.length || 0;
  const coverage = buildCoverage(docType, periods, data);
  const samplePeriod = periods[0] || '_single';
  const sampleData = data[samplePeriod] || data._single || data;
  const requiredFound = (schema?.requiredFields || []).filter((field) => sampleData[field.name] != null).length;
  const totalFields = Math.max(coverage.total, 1);
  const directCoverageScore = Math.min(directMatches / totalFields, 1);
  const requiredScore = requiredCount > 0 ? requiredFound / requiredCount : 1;
  const derivedPenalty = Math.min(derivedFields * 0.03, 0.15);

  return round(Math.max(
    0,
    Math.min((requiredScore * 0.55) + (directCoverageScore * 0.35) + ((coverage.found / totalFields) * 0.1) - derivedPenalty, 1),
  ));
}

export function finalizeExtraction({
  docType,
  periods,
  data,
  directMatches,
  derivedFields,
  warnings = [],
}) {
  const coverage = buildCoverage(docType, periods, data);
  const schema = FIELD_SCHEMAS[docType];
  const mappingConfidence = computeMappingConfidence({
    docType,
    periods,
    data,
    directMatches,
    derivedFields,
  });

  return {
    periods,
    data,
    coverage,
    missingFields: coverage.missing,
    mappingConfidence,
    usable: coverage.found >= (schema?.minUsabilityFields || 0),
    warnings: [...new Set(warnings)],
  };
}

export function listDataFields(periodData) {
  return Object.entries(periodData)
    .filter(([fieldName, value]) => !fieldName.startsWith('__') && value != null)
    .map(([fieldName]) => fieldName);
}

export function round(value) {
  return Math.round(value * 100) / 100;
}
