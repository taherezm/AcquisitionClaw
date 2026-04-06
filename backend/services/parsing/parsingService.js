import { parseCsvFile } from './csvParser.js';
import { parsePdfFile } from './pdfParser.js';
import { parseXlsxFile } from './xlsxParser.js';
import { getFileExtension } from '../../utils/fileUtils.js';

export async function parseUploadedFile(file, validation) {
  if (!validation.accepted) {
    return {
      status: 'skipped',
      parser: null,
      warnings: [validation.reason],
      tables: [],
      workbook: null,
      textPreview: '',
      contentText: '',
      parsedDocument: null,
    };
  }

  const extension = getFileExtension(file.originalname);

  try {
    if (extension === '.csv') {
      return parseCsvFile(file);
    }

    if (extension === '.xlsx') {
      return parseXlsxFile(file);
    }

     if (extension === '.pdf') {
      return await parsePdfFile(file);
    }

    return {
      status: 'skipped',
      parser: null,
      warnings: [`No parser is registered for ${extension}.`],
      tables: [],
      workbook: null,
      textPreview: '',
      contentText: '',
      parsedDocument: null,
    };
  } catch (error) {
    return {
      status: 'error',
      parser: extension.replace('.', '') || 'unknown',
      warnings: [`Failed to parse ${file.originalname}: ${error.message}`],
      tables: [],
      workbook: null,
      textPreview: '',
      contentText: '',
      parsedDocument: null,
    };
  }
}
