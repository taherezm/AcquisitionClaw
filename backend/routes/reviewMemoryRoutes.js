import { Router } from 'express';

import { getReviewMemory, putReviewMemory } from '../controllers/reviewMemoryController.js';

const router = Router();

router.get('/', getReviewMemory);
router.put('/', putReviewMemory);

export default router;
