import { buildNormalizedSheet } from '../parsing/parsingUtils.js';

const MIN_ROWS_PER_SECTION = 3;
const HEADER_SCAN_LOOKAHEAD = 3;

export function segmentParsedSheet(sheet) {
  if (!sheet || !Array.isArray(sheet.rows) || sheet.rows.length === 0) {
    return [];
  }

  const rows = sheet.rows;
  const boundaryStarts = [{ headerRowIndex: sheet.headerRowIndex ?? 0, title: sheet.title || null, reason: 'base_header' }];
  let pendingTitle = null;

  for (let index = (sheet.headerRowIndex ?? 0) + 1; index < rows.length - 1; index += 1) {
    const currentRow = rows[index];
    const nextRow = rows[index + 1];

    if (isTitleRow(currentRow) && isHeaderLikeRow(nextRow, rows, index + 1)) {
      pendingTitle = getRowLabel(currentRow);
      continue;
    }

    if (!isHeaderLikeRow(currentRow, rows, index)) continue;

    const currentStart = boundaryStarts[boundaryStarts.length - 1];
    if (index - currentStart.headerRowIndex < MIN_ROWS_PER_SECTION) continue;

    const similarToCurrentHeader = areRowsStructurallySimilar(
      rows[currentStart.headerRowIndex],
      currentRow,
    );
    const promotedByTitle = Boolean(pendingTitle);
    if (!similarToCurrentHeader && !promotedByTitle) continue;

    boundaryStarts.push({
      headerRowIndex: index,
      title: pendingTitle,
      reason: promotedByTitle ? 'title_then_header' : 'repeated_header',
    });
    pendingTitle = null;
  }

  if (boundaryStarts.length === 1) {
    return [buildSegmentSheet(sheet, boundaryStarts[0], rows.length, 0)];
  }

  return boundaryStarts
    .map((boundary, index) => {
      const nextBoundary = boundaryStarts[index + 1];
      return buildSegmentSheet(
        sheet,
        boundary,
        nextBoundary?.headerRowIndex ?? rows.length,
        index,
      );
    })
    .filter((segment) => segment.recordCount >= 1);
}

function buildSegmentSheet(sheet, boundary, endExclusive, segmentIndex) {
  const rawRows = [];

  if (boundary.title) {
    rawRows.push([boundary.title]);
  }

  rawRows.push(
    ...sheet.rows
      .slice(boundary.headerRowIndex, endExclusive)
      .map((row) => row.values),
  );

  return buildNormalizedSheet({
    name: boundary.title
      ? `${sheet.name} :: ${boundary.title}`
      : `${sheet.name} :: section ${segmentIndex + 1}`,
    rows: rawRows,
    sourceKind: 'sheet-section',
    pageNumber: sheet.pageNumber ?? null,
    pageRange: sheet.pageRange || null,
    parentSheetName: sheet.name,
    parentSheetIndex: sheet.index,
    segmentIndex,
    segmentLabel: boundary.title || `Section ${segmentIndex + 1}`,
    segmentationReason: boundary.reason,
  }, segmentIndex);
}

function isHeaderLikeRow(row, rows, index) {
  if (!row) return false;
  const populated = getPopulatedValues(row);
  if (populated.length < 2) return false;

  const stringCount = populated.filter((value) => typeof value === 'string').length;
  const periodCount = populated.filter((value) => /\b(19|20)\d{2}\b|ltm|ttm|fy\d{2,4}|\bq[1-4]\b/i.test(String(value || ''))).length;
  const numericCount = populated.filter((value) => isNumericLike(value)).length;
  if (stringCount === 0) return false;
  if (numericCount >= populated.length && periodCount === 0) return false;

  const nextRows = rows.slice(index + 1, index + 1 + HEADER_SCAN_LOOKAHEAD);
  const supportedColumns = row.values.reduce((count, _value, columnIndex) => {
    const hasDataBelow = nextRows.some((candidate) => hasMeaningfulValue(candidate?.values?.[columnIndex]));
    return count + (hasDataBelow ? 1 : 0);
  }, 0);

  return supportedColumns >= Math.max(2, Math.min(populated.length, 3));
}

function areRowsStructurallySimilar(leftRow, rightRow) {
  const left = new Set(getPopulatedValues(leftRow).map((value) => normalizeToken(value)));
  const right = new Set(getPopulatedValues(rightRow).map((value) => normalizeToken(value)));
  if (left.size === 0 || right.size === 0) return false;

  let overlaps = 0;
  for (const token of left) {
    if (right.has(token)) overlaps += 1;
  }

  return overlaps >= Math.min(left.size, right.size, 2);
}

function isTitleRow(row) {
  const populated = getPopulatedValues(row);
  if (populated.length !== 1) return false;
  return typeof populated[0] === 'string' && !isNumericLike(populated[0]);
}

function getRowLabel(row) {
  return getPopulatedValues(row).find((value) => typeof value === 'string') || null;
}

function getPopulatedValues(row) {
  if (!row) return [];
  const values = Array.isArray(row.values) ? row.values : Object.values(row.values || {});
  return values.filter(hasMeaningfulValue);
}

function hasMeaningfulValue(value) {
  return value !== null && value !== undefined && value !== '';
}

function isNumericLike(value) {
  if (typeof value === 'number') return Number.isFinite(value);
  if (typeof value !== 'string') return false;
  const cleaned = value.trim().replace(/[,$()%]/g, '');
  if (!cleaned) return false;
  return Number.isFinite(Number(cleaned));
}

function normalizeToken(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}
