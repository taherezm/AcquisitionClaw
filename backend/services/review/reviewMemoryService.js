import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { buildReviewMemoryBundle } from '../../../ingestion/reviewOverrides.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '../../..');
const REVIEW_MEMORY_DIR = path.join(projectRoot, '.data', 'review-memory');
const STATE_FILENAME = 'state.json';
const HISTORY_FILENAME = 'history.json';
const HISTORY_LIMIT = 40;

export class ReviewMemoryConflictError extends Error {
  constructor(message, meta = {}) {
    super(message);
    this.name = 'ReviewMemoryConflictError';
    this.statusCode = 409;
    this.code = 'review_memory_conflict';
    this.meta = meta;
  }
}

export async function loadPersistedReviewMemory(scopeInput = {}) {
  const scope = normalizeReviewMemoryScope(scopeInput);
  const scopeDir = getReviewMemoryScopeDir(scope);

  await ensureScopeDir(scopeDir);

  try {
    const [storedState, storedHistory] = await Promise.all([
      readJson(path.join(scopeDir, STATE_FILENAME), {}),
      readJson(path.join(scopeDir, HISTORY_FILENAME), []),
    ]);
    return finalizePersistedMemory(scope, storedState, storedHistory);
  } catch (error) {
    return finalizePersistedMemory(scope, {
      loadError: error.message,
    }, []);
  }
}

export async function savePersistedReviewMemory(scopeInput = {}, payload = {}) {
  const scope = normalizeReviewMemoryScope(scopeInput);
  const scopeDir = getReviewMemoryScopeDir(scope);
  const statePath = path.join(scopeDir, STATE_FILENAME);
  const historyPath = path.join(scopeDir, HISTORY_FILENAME);

  await ensureScopeDir(scopeDir);

  const [storedState, storedHistory] = await Promise.all([
    readJson(statePath, {}),
    readJson(historyPath, []),
  ]);
  const currentMemory = finalizePersistedMemory(scope, storedState, storedHistory);

  if (
    payload.expectedRevision != null
    && Number.isFinite(Number(payload.expectedRevision))
    && Number(payload.expectedRevision) !== Number(currentMemory.revision || 0)
  ) {
    throw new ReviewMemoryConflictError(
      `Reviewer memory changed on the server. Expected revision ${payload.expectedRevision}, found ${currentMemory.revision || 0}. Reload and merge before saving again.`,
      {
        expectedRevision: Number(payload.expectedRevision),
        currentRevision: Number(currentMemory.revision || 0),
      },
    );
  }

  const nextInput = finalizePersistInput(scope, payload, currentMemory);
  if (isSameReviewMemoryPayload(currentMemory, nextInput)) {
    return currentMemory;
  }

  const revision = Number(currentMemory.revision || 0) + 1;
  const updatedAt = new Date().toISOString();
  const updatedBy = scope.reviewerId || payload.updatedBy || currentMemory.updatedBy || 'anonymous-reviewer';
  const historyEntry = buildHistoryEntry(scope, currentMemory, nextInput, {
    revision,
    updatedAt,
    updatedBy,
  });
  const nextHistory = historyEntry
    ? [historyEntry, ...(Array.isArray(storedHistory) ? storedHistory : [])].slice(0, HISTORY_LIMIT)
    : (Array.isArray(storedHistory) ? storedHistory : []).slice(0, HISTORY_LIMIT);

  const nextMemory = finalizePersistedMemory(scope, {
    ...nextInput,
    revision,
    updatedAt,
    updatedBy,
  }, nextHistory);

  await Promise.all([
    atomicWriteJson(statePath, serializePersistedMemory(nextMemory)),
    atomicWriteJson(historyPath, nextHistory),
  ]);

  return nextMemory;
}

function finalizePersistedMemory(scope, payload = {}, recentHistory = []) {
  const bundle = buildReviewMemoryBundle({
    reviewOverrides: payload.reviewOverrides || [],
    sourcePreferences: payload.sourcePreferences || [],
    conceptSuppressions: payload.conceptSuppressions || [],
    timeBasisOverrides: payload.timeBasisOverrides || [],
    entityResolutions: payload.entityResolutions || [],
  });

  return {
    version: 2,
    revision: Number(payload.revision || 0),
    companyName: scope.companyName,
    companyKey: scope.companyKey,
    dealName: scope.dealName,
    dealKey: scope.dealKey,
    reviewerId: scope.reviewerId,
    reviewOverrides: bundle.reviewOverrides,
    sourcePreferences: bundle.sourcePreferences,
    conceptSuppressions: bundle.conceptSuppressions,
    timeBasisOverrides: bundle.timeBasisOverrides,
    entityResolutions: bundle.entityResolutions,
    learnedAliasRules: bundle.learnedAliasRules,
    reviewerSignals: bundle.reviewerSignals,
    recentHistory: Array.isArray(recentHistory) ? recentHistory.slice(0, HISTORY_LIMIT) : [],
    updatedAt: payload.updatedAt || null,
    updatedBy: payload.updatedBy || null,
    loadError: payload.loadError || null,
  };
}

function finalizePersistInput(scope, payload = {}, currentMemory = {}) {
  return {
    companyName: scope.companyName,
    dealName: scope.dealName,
    reviewerId: scope.reviewerId,
    reviewOverrides: payload.reviewOverrides || currentMemory.reviewOverrides || [],
    sourcePreferences: payload.sourcePreferences || currentMemory.sourcePreferences || [],
    conceptSuppressions: payload.conceptSuppressions || currentMemory.conceptSuppressions || [],
    timeBasisOverrides: payload.timeBasisOverrides || currentMemory.timeBasisOverrides || [],
    entityResolutions: payload.entityResolutions || currentMemory.entityResolutions || [],
  };
}

function serializePersistedMemory(memory) {
  return {
    version: memory.version,
    revision: memory.revision,
    companyName: memory.companyName,
    dealName: memory.dealName,
    reviewOverrides: memory.reviewOverrides,
    sourcePreferences: memory.sourcePreferences,
    conceptSuppressions: memory.conceptSuppressions,
    timeBasisOverrides: memory.timeBasisOverrides,
    entityResolutions: memory.entityResolutions,
    updatedAt: memory.updatedAt,
    updatedBy: memory.updatedBy,
  };
}

function normalizeReviewMemoryScope(scopeInput = {}) {
  const companyName = String(scopeInput.companyName || '').trim();
  const dealName = String(scopeInput.dealName || '').trim() || 'primary-deal';
  const reviewerId = String(scopeInput.reviewerId || '').trim() || 'anonymous-reviewer';

  return {
    companyName,
    companyKey: buildScopeKey(companyName || 'default-company'),
    dealName,
    dealKey: buildScopeKey(dealName),
    reviewerId,
  };
}

function buildScopeKey(value = '') {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'default';
}

function getReviewMemoryScopeDir(scope) {
  return path.join(REVIEW_MEMORY_DIR, scope.companyKey, scope.dealKey);
}

async function ensureScopeDir(scopeDir) {
  await fs.mkdir(scopeDir, { recursive: true });
}

async function readJson(filePath, fallback) {
  try {
    const raw = await fs.readFile(filePath, 'utf8');
    return JSON.parse(raw);
  } catch (error) {
    if (error?.code === 'ENOENT') return fallback;
    throw error;
  }
}

async function atomicWriteJson(filePath, payload) {
  const tempPath = `${filePath}.tmp`;
  await fs.writeFile(tempPath, JSON.stringify(payload, null, 2), 'utf8');
  await fs.rename(tempPath, filePath);
}

function isSameReviewMemoryPayload(currentMemory, nextInput) {
  const currentSignature = JSON.stringify({
    reviewOverrides: currentMemory.reviewOverrides || [],
    sourcePreferences: currentMemory.sourcePreferences || [],
    conceptSuppressions: currentMemory.conceptSuppressions || [],
    timeBasisOverrides: currentMemory.timeBasisOverrides || [],
    entityResolutions: currentMemory.entityResolutions || [],
  });
  const nextSignature = JSON.stringify({
    reviewOverrides: nextInput.reviewOverrides || [],
    sourcePreferences: nextInput.sourcePreferences || [],
    conceptSuppressions: nextInput.conceptSuppressions || [],
    timeBasisOverrides: nextInput.timeBasisOverrides || [],
    entityResolutions: nextInput.entityResolutions || [],
  });

  return currentSignature === nextSignature;
}

function buildHistoryEntry(scope, currentMemory, nextInput, meta) {
  const changes = [
    summarizeCollectionDelta('review_overrides', currentMemory.reviewOverrides, nextInput.reviewOverrides, (entry) => entry.id),
    summarizeCollectionDelta('source_preferences', currentMemory.sourcePreferences, nextInput.sourcePreferences, (entry) => entry.conceptKey),
    summarizeCollectionDelta('concept_suppressions', currentMemory.conceptSuppressions, nextInput.conceptSuppressions, (entry) => [entry.conceptKey, entry.docType, entry.sourceRefKey || ''].join('::')),
    summarizeCollectionDelta('time_basis_overrides', currentMemory.timeBasisOverrides, nextInput.timeBasisOverrides, (entry) => [entry.docType, entry.sourceRefKey || ''].join('::')),
    summarizeCollectionDelta('entity_resolutions', currentMemory.entityResolutions, nextInput.entityResolutions, (entry) => entry.id),
  ].filter(Boolean);

  if (changes.length === 0) return null;

  const summary = changes
    .map((entry) => `${entry.label}: +${entry.addedCount}${entry.removedCount > 0 ? ` / -${entry.removedCount}` : ''}`)
    .join(', ');

  return {
    id: `${meta.revision}-${meta.updatedAt}`,
    revision: meta.revision,
    updatedAt: meta.updatedAt,
    updatedBy: meta.updatedBy,
    companyName: scope.companyName,
    dealName: scope.dealName,
    summary,
    changes,
  };
}

function summarizeCollectionDelta(label, previousItems = [], nextItems = [], buildKey) {
  const previous = new Map((previousItems || []).map((entry) => [buildKey(entry), entry]));
  const next = new Map((nextItems || []).map((entry) => [buildKey(entry), entry]));
  const added = [...next.keys()].filter((key) => !previous.has(key));
  const removed = [...previous.keys()].filter((key) => !next.has(key));

  if (added.length === 0 && removed.length === 0) return null;

  return {
    label,
    addedCount: added.length,
    removedCount: removed.length,
  };
}
