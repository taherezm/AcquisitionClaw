import { ReviewMemoryConflictError, loadPersistedReviewMemory, savePersistedReviewMemory } from '../services/review/reviewMemoryService.js';

export async function getReviewMemory(req, res) {
  const companyName = String(req.query.companyName || '').trim();
  const dealName = String(req.query.dealName || '').trim();
  const reviewerId = String(req.query.reviewerId || '').trim();
  if (!companyName) {
    return res.status(400).json({
      error: 'companyName is required to load persisted review memory.',
    });
  }

  const memory = await loadPersistedReviewMemory({
    companyName,
    dealName,
    reviewerId,
  });
  return res.status(200).json(memory);
}

export async function putReviewMemory(req, res) {
  const companyName = String(req.body?.companyName || '').trim();
  const dealName = String(req.body?.dealName || '').trim();
  const reviewerId = String(req.body?.reviewerId || '').trim();
  if (!companyName) {
    return res.status(400).json({
      error: 'companyName is required to persist review memory.',
    });
  }

  try {
    const memory = await savePersistedReviewMemory({
      companyName,
      dealName,
      reviewerId,
    }, {
      expectedRevision: req.body?.expectedRevision,
      reviewOverrides: req.body?.reviewOverrides || [],
      sourcePreferences: req.body?.sourcePreferences || [],
      conceptSuppressions: req.body?.conceptSuppressions || [],
      timeBasisOverrides: req.body?.timeBasisOverrides || [],
      entityResolutions: req.body?.entityResolutions || [],
    });

    return res.status(200).json(memory);
  } catch (error) {
    if (error instanceof ReviewMemoryConflictError) {
      return res.status(error.statusCode || 409).json({
        error: error.message,
        code: error.code,
        meta: error.meta || null,
      });
    }
    throw error;
  }
}
