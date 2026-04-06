const TEXT_PREVIEW_LIMIT = 4000;
const PREVIEW_RECORD_LIMIT = 5;
const HEADER_SCAN_LIMIT = 8;
const PERIOD_HEADER_PATTERN = /\b(19|20)\d{2}\b|ltm|ttm|fy\d{2,4}|\bq[1-4]\b/i;

export function buildParsedDocument({
  sourceType,
  sheets,
}) {
  const normalizedSheets = sheets.map((sheet, index) => buildNormalizedSheet(sheet, index));
  const sheetNames = normalizedSheets.map((sheet) => sheet.name);

  return {
    sourceType,
    sheetNames,
    sheetCount: normalizedSheets.length,
    sheets: normalizedSheets,
  };
}

export function buildNormalizedSheet(sheet, index = 0) {
  return normalizeSheet(sheet, index);
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
  const headerRowIndex = detectHeaderRowIndex(rows);
  const columnCount = rows.reduce((max, row) => Math.max(max, row.length), 0);
  const columns = buildColumns(rows[headerRowIndex] || sheet.header, columnCount);
  const header = columns.map((column) => column.header);
  const records = rows
    .slice(headerRowIndex + 1)
    .map((row, rowIndex) => buildRecord(columns, row, headerRowIndex + rowIndex + 2));
  const valueScale = detectValueScale(sheet.name, rows);
  const title = detectSheetTitle(rows, headerRowIndex);

  return {
    name: sheet.name || `Sheet ${index + 1}`,
    index,
    title,
    valueScale,
    headerRowIndex,
    sourceKind: sheet.sourceKind || 'tabular-sheet',
    pageNumber: sheet.pageNumber ?? null,
    pageRange: sheet.pageRange || null,
    ocrApplied: Boolean(sheet.ocrApplied),
    ocrEngine: sheet.ocrEngine || null,
    extractionMode: sheet.extractionMode || 'tabular',
    parentSheetName: sheet.parentSheetName || null,
    parentSheetIndex: sheet.parentSheetIndex ?? null,
    segmentIndex: sheet.segmentIndex ?? null,
    segmentLabel: sheet.segmentLabel || null,
    segmentationReason: sheet.segmentationReason || null,
    header,
    columnCount,
    rowCount: rows.length,
    recordCount: records.length,
    columns,
    rows: rows.map((row, rowIndex) => ({
      rowIndex: rowIndex + 1,
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

function detectHeaderRowIndex(rows) {
  if (rows.length === 0) return 0;

  const maxIndex = Math.min(rows.length, HEADER_SCAN_LIMIT);
  let bestCandidate = { index: 0, score: Number.NEGATIVE_INFINITY };

  for (let index = 0; index < maxIndex; index += 1) {
    const score = scoreHeaderRowCandidate(rows, index);
    if (score > bestCandidate.score) {
      bestCandidate = { index, score };
    }
  }

  return bestCandidate.index;
}

function scoreHeaderRowCandidate(rows, index) {
  const row = rows[index] || [];
  const populatedCells = row.filter(hasMeaningfulValue);
  if (populatedCells.length === 0) return Number.NEGATIVE_INFINITY;

  const stringCells = populatedCells.filter((value) => typeof value === 'string');
  const numericCells = populatedCells.filter(isLikelyNumeric);
  const periodCells = populatedCells.filter((value) => PERIOD_HEADER_PATTERN.test(String(value || '')));
  const nextRows = rows.slice(index + 1, index + 4);

  let score = 0;
  score += populatedCells.length >= 2 ? 1.5 : -2;
  score += Math.min(stringCells.length * 0.55, 2.2);
  score += Math.min(periodCells.length * 0.75, 2.25);
  score -= Math.min(numericCells.length * 0.35, 1.4);

  if (typeof row[0] === 'string' && row[0].trim()) {
    score += 0.75;
  }

  const supportedColumns = row.reduce((count, cell, columnIndex) => {
    if (!hasMeaningfulValue(cell)) return count;
    const hasDataBelow = nextRows.some((candidateRow) => hasMeaningfulValue(candidateRow?.[columnIndex]));
    return count + (hasDataBelow ? 1 : 0);
  }, 0);
  score += Math.min(supportedColumns * 0.35, 1.75);

  const numericSupport = row.reduce((count, _cell, columnIndex) => {
    const hasNumericBelow = nextRows.some((candidateRow) => isLikelyNumeric(candidateRow?.[columnIndex]));
    return count + (hasNumericBelow ? 1 : 0);
  }, 0);
  score += Math.min(numericSupport * 0.18, 0.9);

  if (looksLikeTitleRow(row)) {
    score -= 1.75;
  }

  return score;
}

function detectSheetTitle(rows, headerRowIndex) {
  const titleRows = rows.slice(0, headerRowIndex + 1);

  for (const row of titleRows) {
    const textCells = row.filter((value) => typeof value === 'string' && value.trim().length > 0);
    if (textCells.length === 1 && !PERIOD_HEADER_PATTERN.test(textCells[0])) {
      return textCells[0];
    }
  }

  return rows[headerRowIndex]?.find((value) => typeof value === 'string' && value.trim().length > 0) || null;
}

function looksLikeTitleRow(row) {
  const populatedCells = row.filter(hasMeaningfulValue);
  if (populatedCells.length <= 1) return true;
  if (populatedCells.length === 2 && populatedCells.every((value) => typeof value === 'string') && !populatedCells.some((value) => PERIOD_HEADER_PATTERN.test(String(value)))) {
    return true;
  }
  return false;
}

function hasMeaningfulValue(value) {
  return value !== null && value !== undefined && value !== '';
}

function isLikelyNumeric(value) {
  if (typeof value === 'number') return Number.isFinite(value);
  if (typeof value !== 'string') return false;

  const cleaned = value.trim().replace(/[,$()%]/g, '');
  if (!cleaned) return false;
  return Number.isFinite(Number(cleaned));
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

function detectValueScale(sheetName, rows) {
  const samples = [
    sheetName,
    ...(rows || []).slice(0, 3).flatMap((row) => row),
  ]
    .filter((value) => typeof value === 'string')
    .join(' ')
    .toLowerCase();

  if (!samples) return 1;
  if (/\(\$?\s*000s?\)|\$000s?\b|\bin thousands?\b|\b000s\b/.test(samples)) return 1000;
  if (/\(\$?\s*mm\)|\(\$?\s*millions?\)|\bin millions?\b|\bmm\b/.test(samples)) return 1000000;
  return 1;
}
