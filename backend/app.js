import express from 'express';
import path from 'node:path';

import ingestRoutes from './routes/ingestRoutes.js';
import reviewMemoryRoutes from './routes/reviewMemoryRoutes.js';

const projectRoot = path.resolve(process.cwd());

const app = express();

app.use((req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    return res.sendStatus(204);
  }

  return next();
});

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
