import xlsx from 'xlsx';

import { buildParsedDocument, buildParsingResponse } from './parsingUtils.js';

export function parseXlsxFile(file) {
  const workbook = xlsx.read(file.buffer, {
    type: 'buffer',
    cellDates: true,
    raw: true,
  });

  const parsedDocument = buildParsedDocument({
    sourceType: 'xlsx',
    sheets: workbook.SheetNames.map((sheetName) => {
      const worksheet = workbook.Sheets[sheetName];
      const rows = xlsx.utils.sheet_to_json(worksheet, {
        header: 1,
        raw: true,
        defval: null,
        blankrows: false,
      });

      return {
        name: sheetName,
        header: rows[0] || [],
        rows,
      };
    }),
  });

  return buildParsingResponse({
    parser: 'xlsx',
    parsedDocument,
  });
}
