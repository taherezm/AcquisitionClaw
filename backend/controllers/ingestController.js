import { randomUUID } from 'node:crypto';

import { classifyUploadedFile } from '../services/classification/classificationService.js';
import { extractStructuredData } from '../services/extraction/extractionService.js';
import { normalizeExtractionResult } from '../services/normalization/normalizationService.js';
import { parseUploadedFile } from '../services/parsing/parsingService.js';
import { summarizeValidationResults, validateUploadedFile } from '../services/validation/validationService.js';
import { toUploadedFileSummary } from '../utils/fileUtils.js';

export async function ingestUploadedFiles(req, res) {
  const uploadedFiles = req.files || [];

  if (uploadedFiles.length === 0) {
    return res.status(400).json({
      error: 'No files were uploaded. Submit multipart/form-data with one or more "files" fields.',
    });
  }

  const results = uploadedFiles.map((file) => {
    const validation = validateUploadedFile(file);
    const parsing = parseUploadedFile(file, validation);
    const classification = classifyUploadedFile(file, parsing);
    const extraction = extractStructuredData({
      file,
      validation,
      parsing,
      classification,
      companyContext: req.body || {},
    });
    const normalization = normalizeExtractionResult({
      validation,
      extraction,
      classification,
    });

    const { contentText: _contentText, ...publicParsing } = parsing;

    return {
      file: toUploadedFileSummary(file),
      validation,
      parsing: publicParsing,
      classification,
      extraction,
      normalization,
    };
  });

  return res.status(200).json({
    requestId: randomUUID(),
    receivedAt: new Date().toISOString(),
    companyContext: {
      companyName: req.body.companyName || '',
      industry: req.body.industry || '',
      ebitdaRange: req.body.ebitdaRange || '',
    },
    summary: summarizeValidationResults(results),
    files: results,
  });
}
