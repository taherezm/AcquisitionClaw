import { FIELD_SCHEMAS } from '../../../ingestion/schemas.js';

const NOISE_TOKENS = new Set([
  'and',
  'for',
  'from',
  'the',
  'this',
  'that',
  'with',
  'without',
  'ended',
  'ending',
  'period',
  'months',
  'month',
  'years',
  'year',
  'annual',
  'statement',
  'schedule',
  'report',
  'actual',
  'budget',
  'forecast',
  'projected',
  'estimated',
  'historical',
  'consolidated',
  'unaudited',
  'audited',
  'adjusted',
  'normalized',
  'usd',
  'dollars',
]);

export function normalizeLabel(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[\u2012-\u2015]/g, '-')
    .replace(/\ba\s*\/\s*r\b/g, 'accounts receivable')
    .replace(/\ba\s*\/\s*p\b/g, 'accounts payable')
    .replace(/\bd\s*[&/]\s*a\b/g, 'depreciation and amortization')
    .replace(/\bsg\s*[&/]\s*a\b/g, 'selling general administrative')
    .replace(/\bpp\s*[&/]\s*e\b/g, 'property plant equipment')
    .replace(/\bp\s*[&/]\s*l\b/g, 'profit and loss')
    .replace(/&/g, ' and ')
    .replace(/\((?:unaudited|audited|in thousands?|in millions?|000s?|mm|usd|us\\$)\)/g, ' ')
    .replace(/[_./\\\-]+/g, ' ')
    .replace(/[()]/g, ' ')
    .replace(/[^a-z0-9% ]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

export function matchFieldByAliases(label, aliasMap = {}) {
  return getFieldMatchCandidates(label, aliasMap)[0]?.fieldName || null;
}

export function getFieldMatchCandidates(label, aliasMap = {}) {
  const labelAnalysis = analyzeLabel(label);
  if (!labelAnalysis.normalized) return [];

  const bestMatchesByField = new Map();

  for (const [fieldName, aliases] of Object.entries(aliasMap)) {
    for (const alias of aliases) {
      const candidate = scoreAliasCandidate(labelAnalysis, alias);
      if (!candidate) continue;

      const enrichedCandidate = {
        fieldName,
        alias: candidate.alias,
        score: candidate.score,
        matchType: candidate.matchType,
        confidenceLabel: candidate.confidenceLabel,
      };

      const existing = bestMatchesByField.get(fieldName);
      if (!existing || enrichedCandidate.score > existing.score) {
        bestMatchesByField.set(fieldName, enrichedCandidate);
      }
    }
  }

  return [...bestMatchesByField.values()].sort((left, right) => {
    if (right.score !== left.score) return right.score - left.score;
    return right.alias.length - left.alias.length;
  });
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
  if (numeric == null) {
    const raw = String(value || '');
    const percentMatch = raw.match(/(-?\d+(?:\.\d+)?)\s*%/);
    if (!percentMatch) return null;
    return round(Number(percentMatch[1]));
  }
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
  provenance = null,
  sourceMetadata = null,
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
    provenance,
    sourceMetadata,
    interpretability: summarizeInterpretability(provenance),
    warnings: [...new Set(warnings)],
  };
}

export function createProvenanceTracker() {
  return {
    mappedRows: [],
    derivedFields: [],
    ambiguousRows: [],
    unmappedRows: [],
    lowConfidenceRows: [],
  };
}

export function summarizeInterpretability(provenance) {
  if (!provenance) {
    return {
      mappedCount: 0,
      derivedCount: 0,
      ambiguousCount: 0,
      unmappedCount: 0,
      lowConfidenceCount: 0,
      exactMatchCount: 0,
      heuristicMatchCount: 0,
      manualMatchCount: 0,
      needsReview: false,
      reviewPriority: 'none',
      recommendations: [],
    };
  }

  const mappedRows = provenance.mappedRows || [];
  const lowConfidenceCount = provenance.lowConfidenceRows?.length || 0;
  const ambiguousCount = provenance.ambiguousRows?.length || 0;
  const unmappedCount = provenance.unmappedRows?.length || 0;
  const exactMatchCount = mappedRows.filter((entry) => entry.matchType === 'exact').length;
  const manualMatchCount = mappedRows.filter((entry) => entry.sourceType === 'manual_override').length;
  const heuristicMatchCount = mappedRows.filter((entry) => (
    entry.sourceType !== 'manual_override'
    && entry.matchType
    && entry.matchType !== 'exact'
  )).length;
  const reviewLoad = ambiguousCount + unmappedCount + lowConfidenceCount;

  return {
    mappedCount: mappedRows.length,
    derivedCount: provenance.derivedFields.length,
    ambiguousCount,
    unmappedCount,
    lowConfidenceCount,
    exactMatchCount,
    heuristicMatchCount,
    manualMatchCount,
    needsReview: reviewLoad > 0,
    reviewPriority: reviewLoad >= 6 ? 'high' : reviewLoad >= 3 ? 'medium' : reviewLoad > 0 ? 'low' : 'none',
    recommendations: buildInterpretabilityRecommendations({
      ambiguousCount,
      unmappedCount,
      lowConfidenceCount,
    }),
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

function analyzeLabel(label) {
  const normalized = normalizeLabel(label);
  const stripped = stripLabelNoise(normalized);
  const compact = normalizeWhitespace(stripped);

  return {
    normalized,
    variants: [...new Set([normalized, stripped, compact].filter(Boolean))],
    tokens: tokenizeLabel(compact || normalized),
  };
}

function scoreAliasCandidate(labelAnalysis, alias) {
  const normalizedAlias = normalizeLabel(alias);
  if (!normalizedAlias) return null;

  const aliasCore = stripLabelNoise(normalizedAlias);
  const aliasTokens = tokenizeLabel(aliasCore || normalizedAlias);
  const labelVariants = labelAnalysis.variants;
  const labelTokenSet = new Set(labelAnalysis.tokens);

  if (labelVariants.includes(normalizedAlias) || labelVariants.includes(aliasCore)) {
    return {
      alias: normalizedAlias,
      score: 1,
      matchType: 'exact',
      confidenceLabel: 'high',
    };
  }

  if (aliasTokens.length === 0) return null;

  if (containsWholePhrase(labelVariants, normalizedAlias) || containsWholePhrase(labelVariants, aliasCore)) {
    return {
      alias: normalizedAlias,
      score: 0.9,
      matchType: 'phrase',
      confidenceLabel: 'high',
    };
  }

  if (aliasTokens.length === 1) {
    return null;
  }

  const sharedTokens = aliasTokens.filter((token) => labelTokenSet.has(token));
  const coverage = sharedTokens.length / aliasTokens.length;
  if (sharedTokens.length < 2 || coverage < 0.67) {
    return null;
  }

  const extraTokenPenalty = Math.min(Math.max(labelAnalysis.tokens.length - aliasTokens.length, 0) * 0.03, 0.12);
  const baseScore = coverage === 1
    ? 0.82
    : 0.68 + ((coverage - 0.67) * 0.35);
  const score = round(Math.max(0.55, Math.min(baseScore - extraTokenPenalty, 0.89)));

  return {
    alias: normalizedAlias,
    score,
    matchType: coverage === 1 ? 'token_cover' : 'token_overlap',
    confidenceLabel: score >= 0.8 ? 'high' : score >= 0.72 ? 'medium' : 'low',
  };
}

function stripLabelNoise(label) {
  return normalizeWhitespace(String(label || '')
    .replace(/\b(?:unaudited|audited|actual|budget|forecast|projected|estimated|historical|consolidated|continued)\b/g, ' ')
    .replace(/\b(?:for|the|year|years|month|months|ended|ending|period|as|of|at|in|on)\b/g, ' ')
    .replace(/\b(?:ltm|ttm|fy\d{2,4}|q[1-4]|ytd)\b/g, ' ')
    .replace(/\b(?:total company|company total|schedule|statement|report)\b/g, ' '));
}

function tokenizeLabel(label) {
  return normalizeWhitespace(label)
    .split(' ')
    .filter((token) => token && !NOISE_TOKENS.has(token) && token.length > 1);
}

function containsWholePhrase(variants, phrase) {
  if (!phrase) return false;
  const escaped = escapeForRegExp(phrase);
  const pattern = new RegExp(`(^|\\s)${escaped}(\\s|$)`);
  return variants.some((variant) => pattern.test(variant));
}

function buildInterpretabilityRecommendations({ ambiguousCount, unmappedCount, lowConfidenceCount }) {
  const recommendations = [];

  if (ambiguousCount > 0) {
    recommendations.push('Resolve competing row-to-field matches before relying on scored outputs.');
  }
  if (unmappedCount > 0) {
    recommendations.push('Review excluded source rows and map material line items into the schema.');
  }
  if (lowConfidenceCount > 0) {
    recommendations.push('Spot-check heuristic mappings that were accepted with weak label similarity.');
  }

  return recommendations;
}

function normalizeWhitespace(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function escapeForRegExp(value) {
  return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}
