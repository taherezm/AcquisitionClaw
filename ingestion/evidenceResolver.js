import { DOC_TYPES, DOC_TYPE_LABELS } from './schemas.js';

const DOC_BASE_PRIORS = Object.freeze({
  [DOC_TYPES.QOE_REPORT]: 0.97,
  [DOC_TYPES.DEBT_SCHEDULE]: 0.95,
  [DOC_TYPES.TAX_RETURN]: 0.94,
  [DOC_TYPES.INCOME_STATEMENT]: 0.92,
  [DOC_TYPES.BALANCE_SHEET]: 0.92,
  [DOC_TYPES.CASH_FLOW_STATEMENT]: 0.9,
  [DOC_TYPES.REVENUE_BREAKDOWN]: 0.88,
  [DOC_TYPES.AR_AGING]: 0.85,
  [DOC_TYPES.AP_AGING]: 0.85,
  [DOC_TYPES.PROJECTIONS]: 0.72,
  [DOC_TYPES.UNKNOWN]: 0.35,
});

const CONCEPT_DEFINITIONS = Object.freeze([
  {
    key: 'revenue',
    label: 'Revenue',
    format: 'currency',
    basis: 'flow',
    tolerance: 0.35,
    preferredDocTypes: [DOC_TYPES.INCOME_STATEMENT, DOC_TYPES.TAX_RETURN, DOC_TYPES.REVENUE_BREAKDOWN],
    sources: [
      { docType: DOC_TYPES.INCOME_STATEMENT, fieldName: 'revenue', fieldLabel: 'revenue' },
      { docType: DOC_TYPES.TAX_RETURN, fieldName: 'grossReceipts', fieldLabel: 'gross receipts' },
      { docType: DOC_TYPES.REVENUE_BREAKDOWN, fieldName: 'totalRevenue', fieldLabel: 'customer revenue total' },
    ],
  },
  {
    key: 'ebitda',
    label: 'EBITDA',
    format: 'currency',
    basis: 'flow',
    tolerance: 0.32,
    preferredDocTypes: [DOC_TYPES.QOE_REPORT, DOC_TYPES.INCOME_STATEMENT, DOC_TYPES.PROJECTIONS],
    sources: [
      { docType: DOC_TYPES.QOE_REPORT, fieldName: 'adjustedEbitda', fieldLabel: 'adjusted EBITDA' },
      { docType: DOC_TYPES.INCOME_STATEMENT, fieldName: 'ebitda', fieldLabel: 'EBITDA' },
      { docType: DOC_TYPES.PROJECTIONS, fieldName: 'projectedEbitda', fieldLabel: 'projected EBITDA' },
    ],
  },
  {
    key: 'cashflow',
    label: 'Free Cash Flow',
    format: 'currency',
    basis: 'flow',
    tolerance: 0.28,
    preferredDocTypes: [DOC_TYPES.CASH_FLOW_STATEMENT],
    sources: [
      { docType: DOC_TYPES.CASH_FLOW_STATEMENT, fieldName: 'freeCashFlow', fieldLabel: 'free cash flow' },
    ],
  },
  {
    key: 'totalDebt',
    label: 'Total Debt',
    format: 'currency',
    basis: 'point_in_time',
    tolerance: 0.28,
    preferredDocTypes: [DOC_TYPES.DEBT_SCHEDULE, DOC_TYPES.BALANCE_SHEET],
    sources: [
      { docType: DOC_TYPES.DEBT_SCHEDULE, fieldName: 'totalDebt', fieldLabel: 'total debt' },
      {
        docType: DOC_TYPES.BALANCE_SHEET,
        fieldName: 'longTermDebt',
        fieldLabel: 'balance sheet debt proxy',
        extractor: (result) => {
          const point = getSelectedPeriodData(result);
          if (!point) return null;
          const longTermDebt = toFiniteNumber(point.longTermDebt, null);
          const currentPortion = toFiniteNumber(point.currentPortionLTD, 0);
          if (longTermDebt == null && currentPortion == null) return null;
          return toFiniteNumber(longTermDebt, 0) + toFiniteNumber(currentPortion, 0);
        },
      },
    ],
  },
  {
    key: 'accountsReceivable',
    label: 'Accounts Receivable',
    format: 'currency',
    basis: 'point_in_time',
    tolerance: 0.22,
    preferredDocTypes: [DOC_TYPES.BALANCE_SHEET, DOC_TYPES.AR_AGING],
    sources: [
      { docType: DOC_TYPES.BALANCE_SHEET, fieldName: 'accountsReceivable', fieldLabel: 'balance sheet AR' },
      { docType: DOC_TYPES.AR_AGING, fieldName: 'totalAR', fieldLabel: 'aging total AR' },
    ],
  },
  {
    key: 'accountsPayable',
    label: 'Accounts Payable',
    format: 'currency',
    basis: 'point_in_time',
    tolerance: 0.22,
    preferredDocTypes: [DOC_TYPES.BALANCE_SHEET, DOC_TYPES.AP_AGING],
    sources: [
      { docType: DOC_TYPES.BALANCE_SHEET, fieldName: 'accountsPayable', fieldLabel: 'balance sheet AP' },
      { docType: DOC_TYPES.AP_AGING, fieldName: 'totalAP', fieldLabel: 'aging total AP' },
    ],
  },
  {
    key: 'topCustomerPct',
    label: 'Top Customer Concentration',
    format: 'percentage',
    basis: 'point_in_time',
    tolerance: 0.18,
    preferredDocTypes: [DOC_TYPES.REVENUE_BREAKDOWN, DOC_TYPES.AR_AGING],
    sources: [
      { docType: DOC_TYPES.REVENUE_BREAKDOWN, fieldName: 'topCustomerPct', fieldLabel: 'top customer % of revenue' },
      { docType: DOC_TYPES.AR_AGING, fieldName: 'concentrationTopCustomer', fieldLabel: 'top customer % of AR' },
    ],
  },
]);

const RECONCILIATION_CONCEPT_CODES = Object.freeze({
  revenue: ['revenue_vs_tax_receipts'],
  ebitda: ['ebitda_vs_qoe'],
  accountsReceivable: ['ar_aging_vs_balance'],
  accountsPayable: ['ap_aging_vs_balance'],
});

const ENTITY_SUFFIXES = new Set([
  'inc', 'llc', 'corp', 'co', 'company', 'ltd', 'lp', 'llp', 'pllc', 'holdings', 'group',
]);

export function buildEvidenceResolutionSummary(results = [], reconciliation = null, reviewerSignals = null) {
  const usableResults = results.filter((result) => result.usable);
  const byType = Object.fromEntries(usableResults.map((result) => [result.docType, result]));
  const timelineContext = buildTimelineContext(usableResults, reviewerSignals);
  const resolvedFields = [];
  const conflicts = [];

  CONCEPT_DEFINITIONS.forEach((concept) => {
    const candidates = concept.sources
      .map((source) => buildEvidenceCandidate(byType[source.docType], source, concept, reconciliation, timelineContext, reviewerSignals))
      .filter(Boolean)
      .sort((left, right) => {
        if (right.rankingScore !== left.rankingScore) return right.rankingScore - left.rankingScore;
        return right.documentPriority - left.documentPriority;
      });

    if (candidates.length === 0) return;

    const selected = candidates[0];
    const competingCandidates = candidates.slice(1);
    const conceptConflicts = competingCandidates
      .filter((candidate) => shouldFlagEvidenceConflict(selected, candidate, concept))
      .map((candidate, index) => buildEvidenceConflict(concept, selected, candidate, index));

    conflicts.push(...conceptConflicts);
    resolvedFields.push({
      key: concept.key,
      label: concept.label,
      format: concept.format,
      selected,
      candidates,
      competingCandidates: conceptConflicts.map((conflict) => conflict.comparedCandidate),
      confidencePct: Math.round((selected.rankingScore || 0) * 100),
      confidenceLabel: toConfidenceLabel(selected.rankingScore || 0),
      resolutionSummary: buildResolutionSummary(concept, selected, conceptConflicts),
    });
  });

  return {
    resolvedFields,
    conflicts: conflicts
      .sort((left, right) => evidencePriorityScore(left.severity) - evidencePriorityScore(right.severity))
      .slice(0, 12),
    summary: `${resolvedFields.length} resolved field${resolvedFields.length === 1 ? '' : 's'}, ${conflicts.length} evidence conflict${conflicts.length === 1 ? '' : 's'} surfaced.`,
  };
}

export function buildTemporalAlignmentSummary(results = [], evidenceResolution = null, reviewerSignals = null) {
  const usableResults = results.filter((result) => result.usable);
  const timelineContext = buildTimelineContext(usableResults, reviewerSignals);
  const documents = timelineContext.documents.map((timeline) => ({
    docType: timeline.docType,
    label: timeline.label,
    primaryPeriod: timeline.primaryPeriod,
    latestYear: timeline.latestYear,
    basis: timeline.basis,
    granularities: timeline.granularities,
    sourceRef: timeline.sourceRef,
    sourceRefKey: timeline.sourceRefKey,
    summary: buildTimelineSummary(timeline),
    alignmentLabel: timeline.alignmentLabel,
  }));

  const conflicts = [];
  for (let index = 0; index < documents.length; index += 1) {
    const current = documents[index];
    for (let compareIndex = index + 1; compareIndex < documents.length; compareIndex += 1) {
      const next = documents[compareIndex];
      if (!shouldCompareTimelinePair(current, next, evidenceResolution)) continue;
      const gap = Math.abs((current.latestYear || 0) - (next.latestYear || 0));
      if (current.latestYear && next.latestYear && gap > 1 && current.basis !== 'forecast' && next.basis !== 'forecast') {
        conflicts.push({
          id: `time-gap-${current.docType}-${next.docType}`,
          severity: gap > 2 ? 'high' : 'medium',
          label: `${current.label} vs ${next.label}`,
          summary: `${current.label} and ${next.label} appear ${gap} years apart, so they should not be treated as fully aligned evidence.`,
          docTypes: [current.docType, next.docType],
          recommendedAction: 'Upload documents for the same period set or explicitly confirm which date basis should drive underwriting.',
        });
      }

      if (current.basis !== next.basis && current.basis !== 'unknown' && next.basis !== 'unknown') {
        conflicts.push({
          id: `time-basis-${current.docType}-${next.docType}`,
          severity: 'medium',
          label: `${current.label} vs ${next.label}`,
          summary: `${current.label} is ${current.basis.replace(/_/g, ' ')} while ${next.label} is ${next.basis.replace(/_/g, ' ')}, so apparent value conflicts may be timing-based rather than factual.`,
          docTypes: [current.docType, next.docType],
          recommendedAction: 'Confirm whether the analysis should anchor on historical FY, LTM, point-in-time, or forecast periods before resolving the conflict.',
        });
      }
    }
  }

  (evidenceResolution?.conflicts || [])
    .filter((conflict) => conflict.timeBasisConflict)
    .forEach((conflict) => {
      conflicts.push({
        id: `evidence-time-${conflict.id}`,
        severity: conflict.severity,
        label: conflict.label,
        summary: conflict.summary,
        docTypes: [conflict.selected.docType, conflict.comparedCandidate.docType],
        recommendedAction: conflict.recommendedAction,
      });
    });

  return {
    documents,
    conflicts: dedupeById(conflicts).slice(0, 12),
    summary: `${documents.length} timeline${documents.length === 1 ? '' : 's'} normalized, ${conflicts.length} temporal alignment conflict${conflicts.length === 1 ? '' : 's'} surfaced.`,
  };
}

export function buildEntityResolutionSummary(results = [], reviewerSignals = null) {
  const mentions = collectEntityMentions(results);
  const clusters = clusterEntityMentions(mentions, reviewerSignals).map((cluster, index) => ({
    id: `entity-${index}`,
    resolutionId: cluster.resolutionId || null,
    kind: cluster.kind,
    canonicalName: cluster.canonicalName,
    aliases: [...cluster.aliases],
    mentionCount: cluster.mentions.length,
    docTypes: [...new Set(cluster.mentions.map((mention) => mention.docType))],
    confidencePct: Math.round(cluster.confidence * 100),
    confidenceLabel: toConfidenceLabel(cluster.confidence),
    summary: buildEntitySummary(cluster),
    reviewerConfirmed: Boolean(cluster.reviewerConfirmed),
  }));

  const ambiguousClusters = clusters
    .filter((cluster) => !cluster.reviewerConfirmed && (cluster.aliases.length > 1 || cluster.confidencePct < 80))
    .slice(0, 10);

  return {
    clusters: clusters.slice(0, 14),
    ambiguousClusters,
    summary: `${clusters.length} entity cluster${clusters.length === 1 ? '' : 's'} built from customer, debt, and add-back data. ${ambiguousClusters.length} cluster${ambiguousClusters.length === 1 ? '' : 's'} may need reviewer confirmation.`,
  };
}

export function buildAmbiguityWorkflowSummary(
  results = [],
  reconciliation = null,
  evidenceResolution = null,
  temporalAlignment = null,
  entityResolution = null,
) {
  const items = [];

  results.filter((result) => result.usable).forEach((result) => {
    const docLabel = DOC_TYPE_LABELS[result.docType] || result.docType;
    const sourceRef = formatSourceRef(result);
    const ambiguousCount = result.provenance?.ambiguousRows?.length || 0;
    const unmappedCount = result.provenance?.unmappedRows?.length || 0;
    const heuristicCount = result.provenance?.lowConfidenceRows?.length || 0;

    if (ambiguousCount > 0) {
      items.push({
        id: `${result.docType}-mapping-choice`,
        type: 'mapping_choice',
        priority: 'high',
        title: `Resolve competing row mappings in ${docLabel}`,
        detail: `${ambiguousCount} row${ambiguousCount === 1 ? '' : 's'} in ${sourceRef} mapped to multiple plausible schema fields.`,
        recommendedAction: 'Choose the intended schema field or ignore the row if it is subtotal noise.',
      });
    }

    if (unmappedCount > 0) {
      items.push({
        id: `${result.docType}-unmapped-materiality`,
        type: 'unmapped_materiality',
        priority: 'medium',
        title: `Review unmapped rows in ${docLabel}`,
        detail: `${unmappedCount} row${unmappedCount === 1 ? '' : 's'} were excluded from normalization in ${sourceRef}.`,
        recommendedAction: 'Map the row if it is economically material; otherwise mark it as non-operating noise.',
      });
    }

    if (heuristicCount > 0) {
      items.push({
        id: `${result.docType}-heuristic-confirmation`,
        type: 'heuristic_confirmation',
        priority: 'medium',
        title: `Confirm weak heuristic matches in ${docLabel}`,
        detail: `${heuristicCount} row${heuristicCount === 1 ? '' : 's'} were accepted through weak similarity in ${sourceRef}.`,
        recommendedAction: 'Promote the correct rows into explicit mappings or learned aliases so future uploads rank them deterministically.',
      });
    }

    if (result.sourceMetadata?.ocrApplied) {
      items.push({
        id: `${result.docType}-ocr-review`,
        type: 'ocr_verification',
        priority: 'medium',
        title: `Verify OCR-derived text in ${docLabel}`,
        detail: `${sourceRef} required OCR, which increases row-label and units ambiguity.`,
        recommendedAction: 'Replace the scan with a native export or confirm critical rows manually before relying on the output.',
      });
    }
  });

  (reconciliation?.findings || [])
    .filter((finding) => /scale|granularity|ocr|staleness/i.test(String(finding.code || finding.message || '')))
    .slice(0, 6)
    .forEach((finding, index) => {
      items.push({
        id: `reconciliation-workflow-${index}`,
        type: /scale/i.test(String(finding.code || finding.message || '')) ? 'unit_verification' : 'time_alignment',
        priority: finding.severity === 'hard_error' ? 'high' : 'medium',
        title: finding.label || 'Resolve reconciliation issue',
        detail: finding.message,
        recommendedAction: /scale/i.test(String(finding.code || finding.message || ''))
          ? 'Confirm the source units and upload the file with explicit unit headers if available.'
          : 'Upload aligned periods or confirm which date basis should control the analysis.',
      });
    });

  (evidenceResolution?.conflicts || []).slice(0, 8).forEach((conflict) => {
    items.push({
      id: `source-conflict-${conflict.id}`,
      type: 'source_conflict',
      priority: conflict.severity === 'high' ? 'high' : 'medium',
      title: conflict.label,
      detail: conflict.summary,
      recommendedAction: conflict.recommendedAction,
    });
  });

  (temporalAlignment?.conflicts || []).slice(0, 6).forEach((conflict) => {
    items.push({
      id: `temporal-${conflict.id}`,
      type: 'time_alignment',
      priority: conflict.severity === 'high' ? 'high' : 'medium',
      title: conflict.label,
      detail: conflict.summary,
      recommendedAction: conflict.recommendedAction,
    });
  });

  (entityResolution?.ambiguousClusters || []).slice(0, 6).forEach((cluster) => {
    items.push({
      id: `entity-${cluster.id}`,
      type: 'entity_resolution',
      priority: 'medium',
      title: `Confirm entity aliases for ${cluster.canonicalName}`,
      detail: `${cluster.aliases.length} aliases were grouped into one ${cluster.kind.replace(/_/g, ' ')} cluster across ${cluster.docTypes.length} document type${cluster.docTypes.length === 1 ? '' : 's'}.`,
      recommendedAction: 'Confirm whether these names refer to the same real-world entity before relying on concentration or debt conclusions.',
    });
  });

  return {
    items: dedupeById(items)
      .sort((left, right) => workflowPriorityScore(left.priority) - workflowPriorityScore(right.priority))
      .slice(0, 16),
    summary: `${items.length} ambiguity workflow${items.length === 1 ? '' : 's'} generated across row mapping, source conflicts, timing, units, OCR, and entity resolution.`,
  };
}

export function buildDocumentConfidenceDecomposition(result, reconciliation = null, reviewerSignals = null) {
  const sourceQuality = computeSourceQualityScore(result);
  const labelConfidence = computeDocumentLabelConfidence(result);
  const extractionConfidence = clamp(result.confidence || 0, 0, 1);
  const temporalAlignment = getDocumentTemporalScore(result, reviewerSignals);
  const reconciliationSupport = getDocumentReconciliationScore(result, reconciliation);
  const reviewerSupport = computeReviewerDocumentSupportScore(result, reviewerSignals);
  const derivedPenalty = Math.min((result.provenance?.derivedFields?.length || 0) * 0.02, 0.12);
  const totalScore = clamp(
    (extractionConfidence * 0.31)
    + (labelConfidence * 0.2)
    + (sourceQuality * 0.18)
    + (temporalAlignment * 0.12)
    + (reconciliationSupport * 0.08)
    + (reviewerSupport * 0.11)
    - derivedPenalty,
    0,
    1,
  );

  return {
    totalScore,
    totalPct: Math.round(totalScore * 100),
    label: toConfidenceLabel(totalScore),
    factors: [
      { key: 'extraction', label: 'Extraction strength', score: extractionConfidence, note: `${Math.round(extractionConfidence * 100)}% extraction confidence from normalized coverage and mapper output.` },
      { key: 'label', label: 'Label evidence', score: labelConfidence, note: `${Math.round(labelConfidence * 100)}% based on exact, heuristic, and manual row mapping support.` },
      { key: 'source', label: 'Source quality', score: sourceQuality, note: `${Math.round(sourceQuality * 100)}% after OCR, warning, and review penalties.` },
      { key: 'timing', label: 'Time alignment', score: temporalAlignment, note: `${Math.round(temporalAlignment * 100)}% based on period basis clarity and granularity consistency.` },
      { key: 'reconciliation', label: 'Cross-doc support', score: reconciliationSupport, note: `${Math.round(reconciliationSupport * 100)}% based on whether the document participates in reconciled or disputed comparisons.` },
      { key: 'reviewer', label: 'Reviewer memory', score: reviewerSupport, note: `${Math.round(reviewerSupport * 100)}% from persisted reviewer decisions about trustworthy document families, noisy labels, and corrected rows.` },
    ],
    penalties: derivedPenalty > 0 ? [
      { key: 'derived', amount: round(derivedPenalty), note: `${result.provenance?.derivedFields?.length || 0} derived field${(result.provenance?.derivedFields?.length || 0) === 1 ? '' : 's'} reduce confidence slightly.` },
    ] : [],
  };
}

function buildEvidenceCandidate(result, source, concept, reconciliation, timelineContext, reviewerSignals = null) {
  if (!result || !result.usable) return null;
  if (isConceptSuppressedForResult(reviewerSignals, concept.key, result)) return null;

  const selection = selectFieldValue(result, source, reviewerSignals);
  if (!selection || !Number.isFinite(selection.value)) return null;

  const fieldConfidence = buildFieldConfidenceDecomposition(
    result,
    source.fieldName,
    selection.periodKey,
    concept,
    reconciliation,
    timelineContext,
    reviewerSignals,
  );

  return {
    docType: result.docType,
    docLabel: DOC_TYPE_LABELS[result.docType] || result.docType,
    fieldName: source.fieldName,
    fieldLabel: source.fieldLabel || humanizeFieldName(source.fieldName),
    value: selection.value,
    format: concept.format,
    periodKey: selection.periodKey,
    latestYear: selection.latestYear,
    granularity: selection.granularity,
    basis: selection.basis,
    sourceRef: formatSourceRef(result),
    sourceRefKey: buildResultSourceRefKey(result),
    synthetic: Boolean(result.synthetic),
    ocrApplied: Boolean(result.sourceMetadata?.ocrApplied),
    rankingScore: fieldConfidence.totalScore,
    confidencePct: Math.round(fieldConfidence.totalScore * 100),
    confidenceLabel: toConfidenceLabel(fieldConfidence.totalScore),
    decomposition: fieldConfidence,
    documentPriority: getConceptDocumentPriority(concept, result, reviewerSignals),
    sourcePreferenceApplied: Boolean(
      reviewerSignals?.sourcePreferences?.some((entry) => (
        entry.conceptKey === concept.key && entry.preferredDocType === result.docType
      )),
    ),
  };
}

function selectFieldValue(result, source, reviewerSignals = null) {
  const periodKey = getSelectedPeriodKey(result);
  const point = getSelectedPeriodData(result);
  if (!point) return null;

  const rawValue = typeof source.extractor === 'function'
    ? source.extractor(result)
    : point?.[source.fieldName];
  const value = toFiniteNumber(rawValue, null);
  if (!Number.isFinite(value)) return null;

  const periodMeta = findPeriodMetadata(result, periodKey);
  return {
    value,
    periodKey,
    granularity: periodMeta?.granularity || inferGranularity(periodKey),
    latestYear: extractYear(periodKey),
    basis: inferDocumentBasis(result, periodKey, periodMeta, reviewerSignals),
  };
}

function buildFieldConfidenceDecomposition(result, fieldName, periodKey, concept, reconciliation, timelineContext, reviewerSignals = null) {
  const documentPriority = getConceptDocumentPriority(concept, result, reviewerSignals);
  const labelConfidence = computeFieldLabelConfidence(result, fieldName, periodKey);
  const extractionConfidence = clamp(result.confidence || 0, 0, 1);
  const sourceQuality = computeSourceQualityScore(result);
  const temporalAlignment = computeTemporalAlignmentScore(result, concept, timelineContext, periodKey, reviewerSignals);
  const reconciliationSupport = computeConceptReconciliationScore(concept.key, result.docType, reconciliation);
  const reviewerSupport = computeReviewerFieldSupportScore(result, fieldName, reviewerSignals);
  const derivedSupport = isDerivedField(result, fieldName, periodKey)
    ? computeDerivedFieldSupportScore(result, fieldName)
    : null;
  const derivedPenalty = derivedSupport != null
    ? (derivedSupport >= 0.88 ? 0.02 : clamp(0.16 - (derivedSupport * 0.12), 0.02, 0.12))
    : 0;

  const totalScore = clamp(
    (documentPriority * 0.22)
    + (labelConfidence * 0.18)
    + (extractionConfidence * 0.16)
    + (sourceQuality * 0.15)
    + (temporalAlignment * 0.12)
    + (reconciliationSupport * 0.07)
    + (reviewerSupport * 0.1)
    - derivedPenalty,
    0,
    1,
  );

  return {
    totalScore,
    factors: [
      { key: 'document_priority', label: 'Document-family prior', score: documentPriority, note: `${DOC_TYPE_LABELS[result.docType] || result.docType} is ${describeDocumentPriority(documentPriority, concept.label)} for ${concept.label}.` },
      { key: 'label', label: 'Label evidence', score: labelConfidence, note: `${Math.round(labelConfidence * 100)}% based on exact, manual, phrase, or heuristic row matching for ${humanizeFieldName(fieldName)}.` },
      { key: 'extraction', label: 'Extraction strength', score: extractionConfidence, note: `${Math.round(extractionConfidence * 100)}% extraction confidence from the selected normalized document.` },
      { key: 'source', label: 'Source quality', score: sourceQuality, note: `${Math.round(sourceQuality * 100)}% after OCR, warning, and review penalties.` },
      { key: 'timing', label: 'Time alignment', score: temporalAlignment, note: `${Math.round(temporalAlignment * 100)}% based on whether the selected period basis fits a ${concept.basis.replace(/_/g, ' ')} concept.` },
      { key: 'reconciliation', label: 'Cross-doc support', score: reconciliationSupport, note: `${Math.round(reconciliationSupport * 100)}% from reconciled vs disputed cross-document comparisons.` },
      { key: 'reviewer', label: 'Reviewer memory', score: reviewerSupport, note: `${Math.round(reviewerSupport * 100)}% from persisted reviewer confirmations and ignored-noise patterns for this document family and field.` },
    ],
    penalties: derivedPenalty > 0 ? [
      { key: 'derived', amount: derivedPenalty, note: `${humanizeFieldName(fieldName)} is derived rather than directly mapped in the selected document.` },
    ] : [],
  };
}

function buildTimelineContext(results, reviewerSignals = null) {
  const documents = results.map((result) => {
    const primaryPeriod = getSelectedPeriodKey(result);
    const granularities = [...new Set((result.sourceMetadata?.periodMetadata || [])
      .map((entry) => entry?.granularity)
      .filter(Boolean))];
    const latestYear = extractYear(primaryPeriod);
    const basis = inferDocumentBasis(result, primaryPeriod, findPeriodMetadata(result, primaryPeriod), reviewerSignals);

    return {
      docType: result.docType,
      label: DOC_TYPE_LABELS[result.docType] || result.docType,
      primaryPeriod,
      latestYear,
      basis,
      granularities,
      sourceRefKey: buildResultSourceRefKey(result),
      overrideApplied: Boolean(getTimeBasisOverrideForResult(reviewerSignals, result)),
      alignmentLabel: basis === 'forecast'
        ? 'forward'
        : basis === 'ltm'
          ? 'trailing'
          : basis === 'point_in_time'
            ? 'snapshot'
            : 'historical',
      sourceRef: formatSourceRef(result),
    };
  });

  const referenceHistoricalYear = mode(documents
    .filter((doc) => doc.latestYear && doc.basis !== 'forecast')
    .map((doc) => doc.latestYear));

  return {
    documents,
    referenceHistoricalYear,
  };
}

function collectEntityMentions(results) {
  const mentions = [];

  results.filter((result) => result.usable).forEach((result) => {
    const point = getSelectedPeriodData(result) || {};
    const sourceRef = formatSourceRef(result);

    if (Array.isArray(point.customers)) {
      point.customers.forEach((customer) => {
        if (!customer?.name) return;
        mentions.push({
          kind: 'customer',
          name: customer.name,
          docType: result.docType,
          sourceRef,
          metrics: [`${toFiniteNumber(customer.percentage, 0)}% share`],
        });
      });
    }

    if (Array.isArray(point.instruments)) {
      point.instruments.forEach((instrument) => {
        if (!instrument?.name) return;
        mentions.push({
          kind: 'debt_instrument',
          name: instrument.name,
          docType: result.docType,
          sourceRef,
          metrics: [
            instrument.principal != null ? `${toCurrencyString(toFiniteNumber(instrument.principal, 0))} principal` : null,
            instrument.rate != null ? `${toFiniteNumber(instrument.rate, 0)}% rate` : null,
          ].filter(Boolean),
        });
      });
    }

    if (Array.isArray(point.addBacks)) {
      point.addBacks.forEach((item) => {
        const itemName = item?.item || item?.name || item?.label;
        if (!itemName) return;
        mentions.push({
          kind: 'add_back',
          name: itemName,
          docType: result.docType,
          sourceRef,
          metrics: [item.amount != null ? `${toCurrencyString(toFiniteNumber(item.amount, 0))} adjustment` : null].filter(Boolean),
        });
      });
    }
  });

  return mentions;
}

function clusterEntityMentions(mentions = [], reviewerSignals = null) {
  const clusters = [];

  mentions.forEach((mention) => {
    const normalized = normalizeEntityName(mention.name);
    if (!normalized) return;

    const explicitResolution = getEntityResolutionForAlias(reviewerSignals, mention.kind, normalized);
    if (explicitResolution) {
      const existingResolvedCluster = clusters.find((cluster) => cluster.resolutionId === explicitResolution.id);
      if (existingResolvedCluster) {
        existingResolvedCluster.mentions.push(mention);
        existingResolvedCluster.aliases.add(mention.name);
        existingResolvedCluster.similarities.push(1);
        return;
      }

      clusters.push({
        kind: mention.kind,
        canonicalName: explicitResolution.canonicalName,
        normalizedName: normalizeEntityName(explicitResolution.canonicalName) || normalized,
        aliases: new Set(explicitResolution.aliases),
        mentions: [mention],
        similarities: [1],
        resolutionId: explicitResolution.id,
        reviewerConfirmed: true,
      });
      return;
    }

    const existing = clusters.find((cluster) => (
      !cluster.resolutionId
      && (
      cluster.kind === mention.kind
      && entitySimilarity(normalized, cluster.normalizedName) >= 0.74
      )
    ));

    if (existing) {
      existing.mentions.push(mention);
      existing.aliases.add(mention.name);
      existing.similarities.push(entitySimilarity(normalized, existing.normalizedName));
      return;
    }

    clusters.push({
      kind: mention.kind,
      canonicalName: mention.name,
      normalizedName: normalized,
      aliases: new Set([mention.name]),
      mentions: [mention],
      similarities: [1],
    });
  });

  return clusters.map((cluster) => ({
    ...cluster,
    confidence: clamp(
      0.58
      + (Math.min(cluster.mentions.length, 4) * 0.07)
      + (average(cluster.similarities) * 0.18)
      + (cluster.reviewerConfirmed ? 0.08 : 0),
      0,
      1,
    ),
  }));
}

function shouldFlagEvidenceConflict(selected, candidate, concept) {
  const rankingGap = Math.abs((selected.rankingScore || 0) - (candidate.rankingScore || 0));
  const variance = relativeDifference(selected.value, candidate.value);
  const timeBasisConflict = selected.basis !== candidate.basis || selected.periodKey !== candidate.periodKey;
  return variance > concept.tolerance || rankingGap < 0.08 || timeBasisConflict;
}

function buildEvidenceConflict(concept, selected, candidate, index) {
  const variance = relativeDifference(selected.value, candidate.value);
  const rankingGap = Math.abs((selected.rankingScore || 0) - (candidate.rankingScore || 0));
  const basisConflict = selected.basis !== candidate.basis;
  const periodConflict = selected.periodKey !== candidate.periodKey;
  const timeBasisConflict = basisConflict || periodConflict;
  const severity = variance > concept.tolerance * 1.8
    ? 'high'
    : variance > concept.tolerance || timeBasisConflict
      ? 'medium'
      : 'low';

  return {
    id: `${concept.key}-${selected.docType}-${candidate.docType}-${index}`,
    key: concept.key,
    label: `${concept.label} conflict`,
    severity,
    summary: `${concept.label} resolves to ${selected.docLabel}${selected.periodKey ? ` (${selected.periodKey})` : ''}, but ${candidate.docLabel}${candidate.periodKey ? ` (${candidate.periodKey})` : ''} provides a competing value with ${formatPercent(variance)} variance${basisConflict ? ' and a different time basis' : periodConflict ? ' and a different period' : ''}.`,
    rankingGap: round(rankingGap),
    variance: round(variance),
    basisConflict,
    periodConflict,
    timeBasisConflict,
    selected,
    comparedCandidate: candidate,
    recommendedAction: timeBasisConflict
      ? 'Confirm which period basis should control this concept and upload aligned support if the documents are mixing forecast, LTM, and historical periods.'
      : 'Review the competing source documents and choose which source family should control this concept for underwriting.',
  };
}

function buildResolutionSummary(concept, selected, conflicts) {
  const majorConflict = conflicts[0];
  const conflictText = majorConflict
    ? ` A competing ${majorConflict.comparedCandidate.docLabel} value remains under review.`
    : '';
  const reviewerText = selected.decomposition?.factors?.find((factor) => factor.key === 'reviewer' && (factor.score || 0) >= 0.72)
    ? ' Reviewer history also supports this source family.'
    : '';
  const explicitPreference = Boolean(selected.sourcePreferenceApplied);
  const preferenceText = explicitPreference ? ' An explicit reviewer source preference is reinforcing that choice.' : '';
  return `${concept.label} currently resolves to ${selected.docLabel}${selected.periodKey ? ` (${selected.periodKey})` : ''} at ${formatValue(selected.value, concept.format)} because it ranks highest on document-family priority, source quality, and timing fit.${reviewerText}${preferenceText}${conflictText}`;
}

function buildTimelineSummary(timeline) {
  const periodText = timeline.primaryPeriod ? `Primary period ${timeline.primaryPeriod}. ` : '';
  const granularityText = timeline.granularities.length > 0
    ? `Granularity: ${timeline.granularities.join(', ')}. `
    : '';
  const overrideText = timeline.overrideApplied ? ' Reviewer memory is explicitly overriding the default time basis. ' : ' ';
  return `${periodText}${timeline.label} is treated as ${timeline.basis.replace(/_/g, ' ')} evidence.${overrideText}${granularityText}${timeline.sourceRef ? `Source: ${timeline.sourceRef}.` : ''}`.trim();
}

function buildEntitySummary(cluster) {
  const aliases = [...cluster.aliases];
  const aliasText = aliases.length > 1 ? `Aliases: ${aliases.slice(0, 4).join(', ')}.` : 'Single visible alias.';
  const docLabels = [...new Set(cluster.mentions.map((mention) => DOC_TYPE_LABELS[mention.docType] || mention.docType))];
  const reviewerText = cluster.reviewerConfirmed ? ' Reviewer confirmed this alias set.' : '';
  return `${cluster.kind.replace(/_/g, ' ')} cluster seen ${cluster.mentions.length} time${cluster.mentions.length === 1 ? '' : 's'} across ${docLabels.join(', ')}. ${aliasText}${reviewerText}`;
}

function getConceptDocumentPriority(concept, resultOrDocType, reviewerSignals = null) {
  const docType = typeof resultOrDocType === 'string' ? resultOrDocType : resultOrDocType?.docType;
  const preferredIndex = concept.preferredDocTypes.indexOf(docType);
  let score;
  if (preferredIndex === 0) score = 1;
  else if (preferredIndex === 1) score = 0.78;
  else if (preferredIndex === 2) score = 0.7;
  else score = DOC_BASE_PRIORS[docType] || 0.5;

  const adjustment = getReviewerDocTypeAdjustment(reviewerSignals, docType);
  if (adjustment > 0) {
    const positiveCap = preferredIndex === 0 ? 0 : preferredIndex === 1 ? 0.03 : preferredIndex === 2 ? 0.025 : 0.02;
    score += Math.min(adjustment, positiveCap);
  } else {
    score += Math.max(adjustment, -0.08);
  }

  const explicitSourcePreference = getSourcePreferenceForConcept(reviewerSignals, concept.key);
  if (explicitSourcePreference) {
    if (explicitSourcePreference.preferredDocType === docType) {
      score += 0.16;
    } else {
      score -= 0.12;
    }
  }

  if (typeof resultOrDocType === 'object' && resultOrDocType) {
    const suppression = getConceptSuppressionForResult(reviewerSignals, concept.key, resultOrDocType);
    if (suppression) {
      score -= suppression.sourceRefKey ? 0.32 : 0.22;
    }
  }
  return clamp(score, 0.35, 1);
}

function describeDocumentPriority(score, label) {
  if (score >= 0.98) return `the strongest default source family`;
  if (score >= 0.9) return `a preferred source family`;
  if (score >= 0.8) return `an acceptable secondary source family`;
  return `a fallback source family`;
}

function computeFieldLabelConfidence(result, fieldName, periodKey) {
  const mappedRows = (result.provenance?.mappedRows || []).filter((entry) => (
    entry.fieldName === fieldName
    && (!periodKey || entry.period === periodKey || entry.period == null)
  ));

  if (mappedRows.length === 0) {
    return isDerivedField(result, fieldName, periodKey)
      ? computeDerivedFieldSupportScore(result, fieldName)
      : clamp((result.confidence || 0) - 0.08, 0.45, 0.92);
  }

  const scores = mappedRows.map((entry) => {
    if (entry.sourceType === 'manual_override') return 0.99;
    if (typeof entry.matchScore === 'number') return clamp(entry.matchScore, 0, 1);
    if (entry.matchType === 'exact') return 0.96;
    if (entry.matchType === 'phrase') return 0.9;
    if (entry.matchType === 'token_cover') return 0.82;
    if (entry.matchType === 'token_overlap') return 0.74;
    return 0.68;
  });

  return clamp(average(scores), 0, 1);
}

function computeDocumentLabelConfidence(result) {
  const mappedRows = result.provenance?.mappedRows || [];
  if (mappedRows.length === 0) return clamp((result.confidence || 0) - 0.12, 0.4, 0.9);

  const scores = mappedRows.slice(0, 20).map((entry) => {
    if (entry.sourceType === 'manual_override') return 0.99;
    if (typeof entry.matchScore === 'number') return clamp(entry.matchScore, 0, 1);
    if (entry.matchType === 'exact') return 0.96;
    if (entry.matchType === 'phrase') return 0.9;
    if (entry.matchType === 'token_cover') return 0.82;
    if (entry.matchType === 'token_overlap') return 0.74;
    return 0.68;
  });

  return clamp(average(scores), 0, 1);
}

function computeSourceQualityScore(result) {
  let score = result.synthetic ? 0.42 : 0.92;
  if (result.sourceMetadata?.ocrApplied) score -= 0.16;
  score -= Math.min((result.warnings || []).length * 0.02, 0.12);
  score -= Math.min((result.provenance?.lowConfidenceRows?.length || 0) * 0.015, 0.08);
  score -= Math.min((result.provenance?.ambiguousRows?.length || 0) * 0.02, 0.08);
  if ((result.provenance?.mappedRows || []).some((entry) => entry.sourceType === 'manual_override')) score += 0.05;
  return clamp(score, 0.2, 1);
}

function computeReviewerFieldSupportScore(result, fieldName, reviewerSignals = null) {
  if (!reviewerSignals) return 0.56;

  let score = 0.56 + getReviewerDocTypeAdjustment(reviewerSignals, result.docType);
  const profileField = (reviewerSignals.fields || []).find((entry) => (
    entry.docType === result.docType
    && entry.fieldName === fieldName
  ));
  if (profileField) {
    score += Math.min(profileField.confidenceBoost || 0, 0.1);
  }

  if (result.docType === DOC_TYPES.DEBT_SCHEDULE && fieldName === 'totalDebt') {
    const instrumentProfile = (reviewerSignals.fields || []).find((entry) => (
      entry.docType === result.docType
      && entry.fieldName === 'instruments'
    ));
    if (instrumentProfile) {
      score += Math.min((instrumentProfile.confidenceBoost || 0) + 0.03, 0.12);
    }
  }

  if (result.docType === DOC_TYPES.REVENUE_BREAKDOWN && ['topCustomerPct', 'top3Pct', 'top5Pct'].includes(fieldName)) {
    const customerProfile = (reviewerSignals.fields || []).find((entry) => (
      entry.docType === result.docType
      && entry.fieldName === 'customers'
    ));
    if (customerProfile) {
      score += Math.min((customerProfile.confidenceBoost || 0) + 0.03, 0.1);
    }
  }

  const mappedRowLabels = (result.provenance?.mappedRows || [])
    .filter((entry) => entry.fieldName === fieldName)
    .map((entry) => normalizeLabel(entry.rowLabel));

  mappedRowLabels.forEach((label) => {
    if (!label) return;
    const labelProfile = (reviewerSignals.labelMappings || []).find((entry) => (
      entry.docType === result.docType
      && entry.rowLabelNormalized === label
      && (entry.mappedFields || []).includes(fieldName)
    ));
    if (labelProfile) {
      score += Math.min(labelProfile.confidenceBoost || 0, 0.14);
    }

    const noisyLabel = (reviewerSignals.noisyLabels || []).find((entry) => (
      entry.docType === result.docType
      && entry.rowLabelNormalized === label
    ));
    if (noisyLabel && !(noisyLabel.mappedFields || []).includes(fieldName)) {
      score -= Math.min((noisyLabel.noiseScore || 0) * 0.18, 0.18);
    }
  });

  return clamp(score, 0.2, 1);
}

function computeReviewerDocumentSupportScore(result, reviewerSignals = null) {
  if (!reviewerSignals) return 0.54;

  let score = 0.54 + getReviewerDocTypeAdjustment(reviewerSignals, result.docType);
  const docProfile = (reviewerSignals.docTypes || []).find((entry) => entry.docType === result.docType);
  if (docProfile) {
    score += Math.min((docProfile.mapCount || 0) * 0.025, 0.14);
    score -= Math.min((docProfile.noiseRatio || 0) * 0.18, 0.16);
  }

  const noisyLabelHits = (result.provenance?.mappedRows || [])
    .map((entry) => normalizeLabel(entry.rowLabel))
    .filter((label) => (reviewerSignals.noisyLabels || []).some((profile) => (
      profile.docType === result.docType
      && profile.rowLabelNormalized === label
    )))
    .length;

  score -= Math.min(noisyLabelHits * 0.03, 0.12);
  return clamp(score, 0.2, 1);
}

function getReviewerDocTypeAdjustment(reviewerSignals, docType) {
  if (!reviewerSignals) return 0;
  const docProfile = (reviewerSignals.docTypes || []).find((entry) => entry.docType === docType);
  if (!docProfile) return 0;
  return clamp(docProfile.trustAdjustment || 0, -0.12, 0.12);
}

function getSourcePreferenceForConcept(reviewerSignals, conceptKey) {
  if (!reviewerSignals || !Array.isArray(reviewerSignals.sourcePreferences)) return null;
  return reviewerSignals.sourcePreferences.find((entry) => entry.conceptKey === conceptKey) || null;
}

function computeTemporalAlignmentScore(result, concept, timelineContext, periodKey, reviewerSignals = null) {
  const periodMeta = findPeriodMetadata(result, periodKey);
  const basis = inferDocumentBasis(result, periodKey, periodMeta, reviewerSignals);
  const latestYear = extractYear(periodKey);
  const referenceYear = timelineContext.referenceHistoricalYear;
  let score = 0.62;

  if (concept.basis === 'point_in_time' && periodKey === '_single') {
    score = result.docType === DOC_TYPES.DEBT_SCHEDULE || result.docType === DOC_TYPES.REVENUE_BREAKDOWN
      ? 1
      : 0.88;
  }

  if (concept.basis === 'flow') {
    if (basis === 'historical') score = 1;
    else if (basis === 'ltm') score = 0.88;
    else if (basis === 'forecast') score = 0.48;
    else if (basis === 'point_in_time') score = 0.58;
  } else {
    if (basis === 'point_in_time' || result.docType === DOC_TYPES.BALANCE_SHEET) score = 1;
    else if (basis === 'historical') score = 0.8;
    else if (basis === 'ltm') score = 0.56;
    else if (basis === 'forecast') score = 0.38;
  }

  if (referenceYear && latestYear) {
    const gap = Math.abs(referenceYear - latestYear);
    if (gap === 0) score += 0.05;
    else if (gap > 1) score -= Math.min(0.04 * gap, 0.14);
  }

  return clamp(score, 0, 1);
}

function getDocumentTemporalScore(result, reviewerSignals = null) {
  const periodKey = getSelectedPeriodKey(result);
  return computeTemporalAlignmentScore(
    result,
    { basis: inferConceptBasisForDoc(result.docType) },
    buildTimelineContext([result], reviewerSignals),
    periodKey,
    reviewerSignals,
  );
}

function computeDerivedFieldSupportScore(result, fieldName) {
  const mappedRows = result.provenance?.mappedRows || [];

  if (result.docType === DOC_TYPES.DEBT_SCHEDULE && fieldName === 'totalDebt') {
    return mappedRows.some((entry) => entry.fieldName === 'instruments') ? 0.9 : 0.68;
  }

  if (result.docType === DOC_TYPES.REVENUE_BREAKDOWN && ['topCustomerPct', 'top3Pct', 'top5Pct'].includes(fieldName)) {
    return mappedRows.some((entry) => entry.fieldName === 'customers') ? 0.88 : 0.66;
  }

  if (result.docType === DOC_TYPES.AR_AGING && fieldName === 'concentrationTopCustomer') {
    return mappedRows.some((entry) => entry.fieldName === 'totalAR' || entry.fieldName === 'current') ? 0.82 : 0.64;
  }

  return 0.55;
}

function computeConceptReconciliationScore(conceptKey, docType, reconciliation) {
  const codes = RECONCILIATION_CONCEPT_CODES[conceptKey] || [];
  if (codes.length === 0) return 0.7;

  const matchingFinding = (reconciliation?.findings || []).find((finding) => (
    codes.includes(finding.code)
    && (finding.docType === docType || finding.relatedDocType === docType)
  ));
  if (matchingFinding) {
    if (matchingFinding.severity === 'hard_error') return 0.2;
    if (matchingFinding.severity === 'warning') return 0.42;
    return 0.62;
  }

  const matchingReconciliation = (reconciliation?.matches || []).find((match) => codes.includes(match.code));
  if (matchingReconciliation) return 0.92;

  return 0.68;
}

function getDocumentReconciliationScore(result, reconciliation) {
  const findings = (reconciliation?.findings || []).filter((finding) => (
    finding.docType === result.docType || finding.relatedDocType === result.docType
  ));
  if (findings.some((finding) => finding.severity === 'hard_error')) return 0.25;
  if (findings.some((finding) => finding.severity === 'warning')) return 0.48;
  if (findings.some((finding) => finding.severity === 'note')) return 0.62;
  return 0.78;
}

function getSelectedPeriodKey(result) {
  if (!Array.isArray(result.periods) || result.periods.length === 0) return '_single';
  return result.periods.includes('_single')
    ? '_single'
    : result.periods[result.periods.length - 1];
}

function getSelectedPeriodData(result) {
  const periodKey = getSelectedPeriodKey(result);
  return periodKey === '_single'
    ? result.data?._single || null
    : result.data?.[periodKey] || null;
}

function findPeriodMetadata(result, periodKey) {
  return (result.sourceMetadata?.periodMetadata || []).find((entry) => entry.periodKey === periodKey) || null;
}

function inferDocumentBasis(result, periodKey, periodMeta = null, reviewerSignals = null) {
  const override = getTimeBasisOverrideForResult(reviewerSignals, result);
  if (override?.basis) {
    return override.basis;
  }
  if (result.docType === DOC_TYPES.PROJECTIONS) return 'forecast';
  if (periodKey === 'LTM' || periodMeta?.granularity === 'ltm') return 'ltm';
  if ([DOC_TYPES.BALANCE_SHEET, DOC_TYPES.AR_AGING, DOC_TYPES.AP_AGING, DOC_TYPES.DEBT_SCHEDULE, DOC_TYPES.REVENUE_BREAKDOWN].includes(result.docType)) {
    return 'point_in_time';
  }
  if (periodMeta?.granularity === 'point_in_time') return 'point_in_time';
  if (periodMeta?.granularity === 'year' || periodMeta?.granularity === 'quarter' || periodMeta?.granularity === 'month') return 'historical';
  return 'unknown';
}

function inferConceptBasisForDoc(docType) {
  return [DOC_TYPES.BALANCE_SHEET, DOC_TYPES.AR_AGING, DOC_TYPES.AP_AGING, DOC_TYPES.DEBT_SCHEDULE, DOC_TYPES.REVENUE_BREAKDOWN].includes(docType)
    ? 'point_in_time'
    : 'flow';
}

function inferGranularity(periodKey) {
  if (periodKey === '_single') return 'point_in_time';
  if (periodKey === 'LTM') return 'ltm';
  if (/Q[1-4]/.test(String(periodKey))) return 'quarter';
  if (/\d{4}-\d{2}/.test(String(periodKey))) return 'month';
  if (/\d{4}/.test(String(periodKey))) return 'year';
  return 'unknown';
}

function isDerivedField(result, fieldName, periodKey) {
  return (result.provenance?.derivedFields || []).some((entry) => (
    entry.fieldName === fieldName
    && (!periodKey || entry.period === periodKey || entry.period == null)
  ));
}

function normalizeEntityName(value) {
  const tokens = String(value || '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9 ]+/g, ' ')
    .split(/\s+/)
    .filter(Boolean)
    .filter((token) => !ENTITY_SUFFIXES.has(token));

  return tokens.join(' ').trim();
}

function entitySimilarity(left, right) {
  if (!left || !right) return 0;
  if (left === right) return 1;
  const leftTokens = new Set(left.split(/\s+/));
  const rightTokens = new Set(right.split(/\s+/));
  const shared = [...leftTokens].filter((token) => rightTokens.has(token)).length;
  const denominator = Math.max(leftTokens.size, rightTokens.size, 1);
  return shared / denominator;
}

function evidencePriorityScore(severity) {
  if (severity === 'high') return 0;
  if (severity === 'medium') return 1;
  return 2;
}

function shouldCompareTimelinePair(left, right, evidenceResolution) {
  const flowDocs = new Set([
    DOC_TYPES.INCOME_STATEMENT,
    DOC_TYPES.CASH_FLOW_STATEMENT,
    DOC_TYPES.TAX_RETURN,
    DOC_TYPES.QOE_REPORT,
    DOC_TYPES.PROJECTIONS,
  ]);
  const pointInTimeDocs = new Set([
    DOC_TYPES.BALANCE_SHEET,
    DOC_TYPES.AR_AGING,
    DOC_TYPES.AP_AGING,
    DOC_TYPES.DEBT_SCHEDULE,
    DOC_TYPES.REVENUE_BREAKDOWN,
  ]);

  const sameFamily = (flowDocs.has(left.docType) && flowDocs.has(right.docType))
    || (pointInTimeDocs.has(left.docType) && pointInTimeDocs.has(right.docType));
  if (sameFamily) return true;

  return (evidenceResolution?.conflicts || []).some((conflict) => (
    [conflict.selected?.docType, conflict.comparedCandidate?.docType].includes(left.docType)
    && [conflict.selected?.docType, conflict.comparedCandidate?.docType].includes(right.docType)
  ));
}

function workflowPriorityScore(priority) {
  if (priority === 'high') return 0;
  if (priority === 'medium') return 1;
  return 2;
}

function formatSourceRef(result) {
  const meta = result.sourceMetadata || {};
  if (meta.pageNumber) return `page ${meta.pageNumber}`;
  if (meta.segmentLabel) return meta.segmentLabel;
  if (result.sourceSheetName) return result.sourceSheetName;
  if (result.sourceFileName) return result.sourceFileName;
  return DOC_TYPE_LABELS[result.docType] || result.docType;
}

function buildResultSourceRefKey(result) {
  const meta = result.sourceMetadata || {};
  return [
    result.docType || '',
    result.sourceSheetName || meta.sheetName || '',
    meta.segmentLabel || '',
    meta.pageNumber != null ? `page-${meta.pageNumber}` : '',
  ]
    .join('::')
    .replace(/\s+/g, ' ')
    .trim();
}

function isConceptSuppressedForResult(reviewerSignals, conceptKey, result) {
  return Boolean(getConceptSuppressionForResult(reviewerSignals, conceptKey, result));
}

function getConceptSuppressionForResult(reviewerSignals, conceptKey, result) {
  if (!reviewerSignals || !Array.isArray(reviewerSignals.conceptSuppressions)) return null;
  const sourceRefKey = buildResultSourceRefKey(result);
  return reviewerSignals.conceptSuppressions.find((entry) => (
    entry.conceptKey === conceptKey
    && entry.docType === result.docType
    && (!entry.sourceRefKey || entry.sourceRefKey === sourceRefKey)
  )) || null;
}

function getTimeBasisOverrideForResult(reviewerSignals, result) {
  if (!reviewerSignals || !Array.isArray(reviewerSignals.timeBasisOverrides)) return null;
  const sourceRefKey = buildResultSourceRefKey(result);
  return reviewerSignals.timeBasisOverrides.find((entry) => (
    entry.docType === result.docType
    && entry.sourceRefKey === sourceRefKey
  )) || null;
}

function getEntityResolutionForAlias(reviewerSignals, kind, normalizedAlias) {
  if (!reviewerSignals || !Array.isArray(reviewerSignals.entityResolutions)) return null;
  return reviewerSignals.entityResolutions.find((entry) => (
    entry.kind === kind
    && Array.isArray(entry.aliasKeys)
    && entry.aliasKeys.includes(normalizedAlias)
  )) || null;
}

function toFiniteNumber(value, fallback = 0) {
  const numeric = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function relativeDifference(leftValue, rightValue) {
  const baseline = Math.max(Math.abs(leftValue), Math.abs(rightValue), 1);
  return Math.abs(leftValue - rightValue) / baseline;
}

function clamp(value, min, max) {
  return Math.min(Math.max(toFiniteNumber(value, min), min), max);
}

function round(value) {
  return Math.round(value * 100) / 100;
}

function average(values = []) {
  if (values.length === 0) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function mode(values = []) {
  if (values.length === 0) return null;
  const counts = new Map();
  values.forEach((value) => {
    counts.set(value, (counts.get(value) || 0) + 1);
  });

  return [...counts.entries()].sort((left, right) => {
    if (right[1] !== left[1]) return right[1] - left[1];
    return right[0] - left[0];
  })[0]?.[0] ?? null;
}

function extractYear(periodKey) {
  const match = String(periodKey || '').match(/\b(19|20)\d{2}\b/);
  return match ? Number(match[0]) : null;
}

function humanizeFieldName(value = '') {
  return String(value)
    .replace(/^__/, '')
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function normalizeLabel(value = '') {
  return String(value)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function toConfidenceLabel(value) {
  if (value >= 0.8) return 'high';
  if (value >= 0.55) return 'medium';
  if (value > 0) return 'low';
  return 'none';
}

function formatPercent(value) {
  return `${Math.round(value * 100)}%`;
}

function toCurrencyString(value) {
  return `$${Math.round(value).toLocaleString()}`;
}

function formatValue(value, format) {
  if (!Number.isFinite(value)) return 'N/A';
  if (format === 'percentage') return `${round(value, 1)}%`;
  return toCurrencyString(value);
}

function dedupeById(items = []) {
  const seen = new Set();
  return items.filter((item) => {
    if (seen.has(item.id)) return false;
    seen.add(item.id);
    return true;
  });
}
