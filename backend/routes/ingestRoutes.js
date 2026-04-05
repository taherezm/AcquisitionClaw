import { Router } from 'express';

import { ingestUploadedFiles } from '../controllers/ingestController.js';
import { uploadFiles } from '../middleware/uploadMiddleware.js';

const router = Router();

router.post('/', uploadFiles.array('files'), ingestUploadedFiles);

export default router;
