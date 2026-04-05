const TEXT_PREVIEW_LIMIT = 4000;
const PREVIEW_RECORD_LIMIT = 5;

export function buildParsedDocument({
  sourceType,
  sheets,
}) {
  const normalizedSheets = sheets.map((sheet, index) => normalizeSheet(sheet, index));
  const sheetNames = normalizedSheets.map((sheet) => sheet.name);

  return {
    sourceType,
    sheetNames,
    sheetCount: normalizedSheets.length,
    sheets: normalizedSheets,
  };
}

export function buildParsingResponse({
  parser,
  parsedDocument,
  warnings = [],
}) {
  return {
    status: 'parsed',
    parser,
    warnings,
    workbook: parser === 'xlsx'
      ? {
        sheetCount: parsedDocument.sheetCount,
        sheetNames: parsedDocument.sheetNames,
      }
      : null,
    tables: parsedDocument.sheets.map((sheet) => ({
      name: sheet.name,
      header: sheet.header,
      rowCount: sheet.recordCount,
      columnCount: sheet.columnCount,
      previewRows: sheet.records.slice(0, PREVIEW_RECORD_LIMIT),
    })),
    parsedDocument,
    textPreview: buildContentText(parsedDocument).slice(0, TEXT_PREVIEW_LIMIT),
    contentText: buildContentText(parsedDocument),
  };
}

function normalizeSheet(sheet, index) {
  const rows = normalizeRows(sheet.rows || []);
  const columnCount = rows.reduce((max, row) => Math.max(max, row.length), 0);
  const columns = buildColumns(sheet.header, columnCount);
  const header = columns.map((column) => column.header);
  const records = rows.slice(1).map((row, rowIndex) => buildRecord(columns, row, rowIndex + 1));

  return {
    name: sheet.name || `Sheet ${index + 1}`,
    index,
    headerRowIndex: 0,
    header,
    columnCount,
    rowCount: rows.length,
    recordCount: records.length,
    columns,
    rows: rows.map((row, rowIndex) => ({
      rowIndex,
      values: columns.map((_, columnIndex) => row[columnIndex] ?? null),
    })),
    records,
  };
}

function normalizeRows(rows) {
  return rows
    .map((row) => Array.isArray(row) ? row.map(normalizeCellValue) : [])
    .filter((row) => row.some((value) => value !== null && value !== ''));
}

function buildColumns(headerRow = [], columnCount) {
  const columns = [];

  for (let index = 0; index < columnCount; index += 1) {
    const key = `column_${index + 1}`;
    const headerValue = normalizeHeaderValue(headerRow[index], key);
    columns.push({
      index,
      key,
      header: headerValue,
    });
  }

  return columns;
}

function buildRecord(columns, row, sourceRowIndex) {
  const values = {};

  columns.forEach((column, index) => {
    values[column.header] = row[index] ?? null;
  });

  return {
    rowIndex: sourceRowIndex,
    values,
  };
}

function normalizeHeaderValue(value, fallback) {
  const normalized = normalizeCellValue(value);

  if (typeof normalized === 'string' && normalized.trim().length > 0) {
    return normalized.trim();
  }

  if (typeof normalized === 'number' || typeof normalized === 'boolean') {
    return String(normalized);
  }

  return fallback;
}

function normalizeCellValue(value) {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  if (typeof value === 'string') {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  }

  if (typeof value === 'number' || typeof value === 'boolean') {
    return value;
  }

  return String(value);
}

function buildContentText(parsedDocument) {
  return parsedDocument.sheets
    .flatMap((sheet) => [
      sheet.name,
      ...sheet.rows.map((row) => row.values.filter((value) => value !== null).join(' ')),
    ])
    .filter((chunk) => typeof chunk === 'string' && chunk.trim().length > 0)
    .join('\n')
    .slice(0, TEXT_PREVIEW_LIMIT);
}
