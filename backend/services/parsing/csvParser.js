import path from 'node:path';

import { buildParsedDocument, buildParsingResponse } from './parsingUtils.js';

export function parseCsvFile(file) {
  const rawText = file.buffer.toString('utf8').replace(/^\uFEFF/, '');
  const rows = parseCsvRows(rawText);
  const sheetName = path.basename(file.originalname, path.extname(file.originalname)) || 'CSV';

  const parsedDocument = buildParsedDocument({
    sourceType: 'csv',
    sheets: [
      {
        name: sheetName,
        header: rows[0] || [],
        rows,
      },
    ],
  });

  return buildParsingResponse({
    parser: 'csv',
    parsedDocument,
  });
}

function parseCsvRows(text) {
  const rows = [];
  let row = [];
  let cell = '';
  let inQuotes = false;

  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    const nextChar = text[index + 1];

    if (char === '"') {
      if (inQuotes && nextChar === '"') {
        cell += '"';
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === ',' && !inQuotes) {
      row.push(cell);
      cell = '';
      continue;
    }

    if ((char === '\n' || char === '\r') && !inQuotes) {
      if (char === '\r' && nextChar === '\n') {
        index += 1;
      }
      row.push(cell);
      rows.push(row);
      row = [];
      cell = '';
      continue;
    }

    cell += char;
  }

  if (cell.length > 0 || row.length > 0) {
    row.push(cell);
    rows.push(row);
  }

  return rows;
}
