const DEFAULT_BACKEND_ORIGIN = 'http://localhost:8080';

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
    response = await fetch(buildApiUrl('/api/ingest'), {
      method: 'POST',
      body: formData,
    });
  } catch (_error) {
    throw new Error(`Unable to reach the ingestion service at ${getApiOriginLabel()}. Start the backend and try again.`);
  }

  const { payload, text } = await readApiResponse(response);

  if (!response.ok) {
    throw new Error(buildApiErrorMessage(response, payload, text, 'Backend ingestion failed.'));
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
    response = await fetch(buildApiUrl(`/api/review-memory?companyName=${encodeURIComponent(scope.companyName)}&dealName=${encodeURIComponent(scope.dealName)}&reviewerId=${encodeURIComponent(scope.reviewerId)}`));
  } catch (_error) {
    throw new Error(`Unable to load persisted review memory from ${getApiOriginLabel()}.`);
  }

  const { payload, text } = await readApiResponse(response);
  if (!response.ok) {
    throw new Error(buildApiErrorMessage(response, payload, text, 'Failed to load persisted review memory.'));
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
    response = await fetch(buildApiUrl('/api/review-memory'), {
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
    throw new Error(`Unable to persist review memory to ${getApiOriginLabel()}.`);
  }

  const { payload, text } = await readApiResponse(response);
  if (!response.ok) {
    throw new Error(buildApiErrorMessage(response, payload, text, 'Failed to persist review memory.'));
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

function buildApiUrl(pathname) {
  const origin = resolveApiOrigin();
  if (!origin) return pathname;
  return `${origin}${pathname}`;
}

function getApiOriginLabel() {
  return resolveApiOrigin() || 'this page';
}

function resolveApiOrigin() {
  if (typeof window === 'undefined' || !window.location) {
    return '';
  }

  const configuredOrigin = safeReadLocalStorage('acquisitionclaw.apiOrigin');
  if (configuredOrigin) {
    return configuredOrigin.replace(/\/+$/, '');
  }

  const { protocol, hostname, port } = window.location;

  if (protocol === 'file:') {
    return DEFAULT_BACKEND_ORIGIN;
  }

  if (hostname === 'localhost' || hostname === '127.0.0.1') {
    if (port === '8080' || port === '8888') {
      return '';
    }

    return DEFAULT_BACKEND_ORIGIN;
  }

  return '';
}

async function readApiResponse(response) {
  const text = await response.text().catch(() => '');
  if (!text) {
    return { payload: null, text: '' };
  }

  try {
    return {
      payload: JSON.parse(text),
      text,
    };
  } catch (_error) {
    return {
      payload: null,
      text,
    };
  }
}

function buildApiErrorMessage(response, payload, text, fallbackMessage) {
  if (payload?.error) {
    return payload.error;
  }

  if (response.status === 404) {
    return `The backend API was not found at ${getApiOriginLabel()}. Open the app from http://localhost:8080 or point the frontend at the running backend.`;
  }

  if (response.status >= 500) {
    return `The backend returned ${response.status}. Check the server terminal for the underlying error.`;
  }

  if (text && !looksLikeHtml(text)) {
    return text.slice(0, 240);
  }

  return `${fallbackMessage} HTTP ${response.status}.`;
}

function looksLikeHtml(text) {
  return /<!doctype html>|<html[\s>]/i.test(text || '');
}

function safeReadLocalStorage(key) {
  try {
    return String(window.localStorage?.getItem(key) || '').trim();
  } catch (_error) {
    return '';
  }
}
