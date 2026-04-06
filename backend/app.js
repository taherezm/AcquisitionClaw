import express from 'express';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import ingestRoutes from './routes/ingestRoutes.js';
import reviewMemoryRoutes from './routes/reviewMemoryRoutes.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '..');

const app = express();

app.use(express.json());
app.use('/api/ingest', ingestRoutes);
app.use('/api/review-memory', reviewMemoryRoutes);
app.use(express.static(projectRoot));

app.get('/', (_req, res) => {
  res.sendFile(path.join(projectRoot, 'index.html'));
});

app.use((err, _req, res, _next) => {
  console.error('Unhandled backend error:', err);
  res.status(err.statusCode || 500).json({
    error: err.message || 'Unexpected backend error.',
  });
});

export default app;
