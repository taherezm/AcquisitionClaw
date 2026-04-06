import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '..');
const outputRoot = path.join(projectRoot, '.netlify', 'public');

const rootFiles = [
  'index.html',
  'app.js',
  'api.js',
  'charts.js',
  'styles.css',
];

const directoriesToCopy = [
  'ingestion',
  'scoring',
];

await fs.rm(outputRoot, { recursive: true, force: true });
await fs.mkdir(outputRoot, { recursive: true });

for (const relativePath of rootFiles) {
  await copyEntry(relativePath);
}

for (const relativePath of directoriesToCopy) {
  await copyEntry(relativePath);
}

async function copyEntry(relativePath) {
  const sourcePath = path.join(projectRoot, relativePath);
  const targetPath = path.join(outputRoot, relativePath);
  await fs.cp(sourcePath, targetPath, { recursive: true });
}
