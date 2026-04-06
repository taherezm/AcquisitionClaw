export async function ingestFiles(files, companyContext = {}) {
  const formData = new FormData();
  const reviewMemory = companyContext.reviewMemory || null;

  files.forEach((file) => {
    formData.append('files', file, file.name);
  });

  formData.append('companyName', companyContext.companyName || '');
  formData.append('dealName', companyContext.dealName || '');
  formData.append('reviewerId', companyContext.reviewerId || '');
  formData.append('industry', companyContext.industry || '');
  formData.append('ebitdaRange', companyContext.ebitdaRange || '');
  formData.append('reviewAliasRules', JSON.stringify(companyContext.reviewAliasRules || []));
  formData.append('reviewRankingSignals', JSON.stringify(companyContext.reviewRankingSignals || null));
  formData.append('reviewOverrideRules', JSON.stringify(reviewMemory?.reviewOverrides || []));
  formData.append('reviewSourcePreferences', JSON.stringify(reviewMemory?.sourcePreferences || []));
  formData.append('reviewConceptSuppressions', JSON.stringify(reviewMemory?.conceptSuppressions || []));
  formData.append('reviewTimeBasisOverrides', JSON.stringify(reviewMemory?.timeBasisOverrides || []));
  formData.append('reviewEntityResolutions', JSON.stringify(reviewMemory?.entityResolutions || []));

  let response;
  try {
    response = await fetch('/api/ingest', {
      method: 'POST',
      body: formData,
    });
  } catch (_error) {
    throw new Error('Unable to reach the ingestion service. Start the backend and try again.');
  }

  const payload = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(payload?.error || 'Backend ingestion failed.');
  }

  return payload;
}

export async function fetchReviewMemory(scopeInput) {
  const scope = normalizeReviewScopeInput(scopeInput);
  if (!scope.companyName) {
    return createEmptyReviewMemory(scope);
  }

  let response;
  try {
    response = await fetch(`/api/review-memory?companyName=${encodeURIComponent(scope.companyName)}&dealName=${encodeURIComponent(scope.dealName)}&reviewerId=${encodeURIComponent(scope.reviewerId)}`);
  } catch (_error) {
    throw new Error('Unable to load persisted review memory from the backend.');
  }

  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    throw new Error(payload?.error || 'Failed to load persisted review memory.');
  }
  return normalizeReviewMemoryPayload(payload, scope);
}

export async function saveReviewMemory(scopeInput, reviewMemory) {
  const scope = normalizeReviewScopeInput(scopeInput);
  if (!scope.companyName) {
    throw new Error('Company name is required before persisting review memory.');
  }

  let response;
  try {
    response = await fetch('/api/review-memory', {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        companyName: scope.companyName,
        dealName: scope.dealName,
        reviewerId: scope.reviewerId,
        expectedRevision: reviewMemory?.revision ?? 0,
        reviewOverrides: reviewMemory?.reviewOverrides || [],
        sourcePreferences: reviewMemory?.sourcePreferences || [],
        conceptSuppressions: reviewMemory?.conceptSuppressions || [],
        timeBasisOverrides: reviewMemory?.timeBasisOverrides || [],
        entityResolutions: reviewMemory?.entityResolutions || [],
      }),
    });
  } catch (_error) {
    throw new Error('Unable to persist review memory to the backend.');
  }

  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    throw new Error(payload?.error || 'Failed to persist review memory.');
  }
  return normalizeReviewMemoryPayload(payload, scope);
}

export function buildPipelineFileDescriptors(ingestionResponse) {
  // The scoring pipeline now consumes backend-validated file results directly.
  // Each descriptor carries the structured ingestion payload so classification
  // and extraction can use server-side results before any local fallback logic.
  return (ingestionResponse.files || [])
    .filter((fileResult) => fileResult.validation?.accepted)
    .flatMap((fileResult) => {
      const splitDocuments = Array.isArray(fileResult.splitDocuments)
        ? fileResult.splitDocuments
        : [];

      if (splitDocuments.length > 0) {
        return splitDocuments.map((splitDocument) => ({
          name: `${fileResult.file.originalName} :: ${splitDocument.sheetName || splitDocument.docType}`,
          type: fileResult.file.mimeType,
          size: fileResult.file.size,
          content: {
            __backendIngestion: true,
            backendFileResult: {
              ...fileResult,
              file: {
                ...fileResult.file,
                originalName: `${fileResult.file.originalName} :: ${splitDocument.sheetName || splitDocument.docType}`,
              },
              classification: splitDocument.classification,
              extraction: splitDocument.extraction,
              normalization: splitDocument.normalization,
              splitDocument,
            },
          },
        }));
      }

      return [{
        name: fileResult.file.originalName,
        type: fileResult.file.mimeType,
        size: fileResult.file.size,
        content: {
          __backendIngestion: true,
          backendFileResult: fileResult,
        },
      }];
    });
}

function normalizeReviewMemoryPayload(payload, scopeInput = {}) {
  const scope = normalizeReviewScopeInput(scopeInput);
  return {
    version: payload?.version || 2,
    revision: Number(payload?.revision || 0),
    companyName: payload?.companyName || scope.companyName,
    companyKey: payload?.companyKey || '',
    dealName: payload?.dealName || scope.dealName,
    dealKey: payload?.dealKey || '',
    reviewerId: payload?.reviewerId || scope.reviewerId,
    reviewOverrides: Array.isArray(payload?.reviewOverrides) ? payload.reviewOverrides : [],
    sourcePreferences: Array.isArray(payload?.sourcePreferences) ? payload.sourcePreferences : [],
    conceptSuppressions: Array.isArray(payload?.conceptSuppressions) ? payload.conceptSuppressions : [],
    timeBasisOverrides: Array.isArray(payload?.timeBasisOverrides) ? payload.timeBasisOverrides : [],
    entityResolutions: Array.isArray(payload?.entityResolutions) ? payload.entityResolutions : [],
    learnedAliasRules: Array.isArray(payload?.learnedAliasRules) ? payload.learnedAliasRules : [],
    reviewerSignals: payload?.reviewerSignals || null,
    recentHistory: Array.isArray(payload?.recentHistory) ? payload.recentHistory : [],
    updatedAt: payload?.updatedAt || null,
    updatedBy: payload?.updatedBy || null,
    loadError: payload?.loadError || null,
  };
}

export function createEmptyReviewMemory(scopeInput = {}) {
  const scope = normalizeReviewScopeInput(scopeInput);
  return normalizeReviewMemoryPayload({
    companyName: scope.companyName,
    dealName: scope.dealName,
    reviewerId: scope.reviewerId,
    reviewOverrides: [],
    sourcePreferences: [],
    conceptSuppressions: [],
    timeBasisOverrides: [],
    entityResolutions: [],
    learnedAliasRules: [],
    reviewerSignals: null,
    recentHistory: [],
    updatedAt: null,
  }, scope);
}

function normalizeReviewScopeInput(scopeInput = {}) {
  if (typeof scopeInput === 'string') {
    return {
      companyName: String(scopeInput || '').trim(),
      dealName: 'primary-deal',
      reviewerId: 'anonymous-reviewer',
    };
  }

  return {
    companyName: String(scopeInput?.companyName || '').trim(),
    dealName: String(scopeInput?.dealName || '').trim() || 'primary-deal',
    reviewerId: String(scopeInput?.reviewerId || '').trim() || 'anonymous-reviewer',
  };
}
