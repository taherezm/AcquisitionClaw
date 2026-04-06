const MONTH_LOOKUP = Object.freeze({
  january: '01',
  jan: '01',
  february: '02',
  feb: '02',
  march: '03',
  mar: '03',
  april: '04',
  apr: '04',
  may: '05',
  june: '06',
  jun: '06',
  july: '07',
  jul: '07',
  august: '08',
  aug: '08',
  september: '09',
  sept: '09',
  sep: '09',
  october: '10',
  oct: '10',
  november: '11',
  nov: '11',
  december: '12',
  dec: '12',
});

export function detectPeriodColumns(columns = []) {
  return columns
    .filter((column) => shouldTreatAsValuePeriodColumn(column.header))
    .map((column) => {
      const period = parsePeriodLabel(column.header);
      if (!period) return null;

      return {
        columnKey: column.columnKey ?? column.header,
        periodKey: period.periodKey,
        granularity: period.granularity,
        sortValue: period.sortValue,
      };
    })
    .filter(Boolean)
    .sort((left, right) => left.sortValue - right.sortValue);
}

function shouldTreatAsValuePeriodColumn(label) {
  const normalized = String(label || '').toLowerCase().replace(/\s+/g, ' ').trim();
  if (!normalized) return false;
  if (!/\b(19|20)\d{2}\b/.test(normalized) && !/\b(fy\s*)?(19|20)\d{2}\b/.test(normalized)) {
    return true;
  }

  return !/(^|\s)(yoy|margin|mix|growth|cagr|percent|%)(\s|$)/.test(normalized);
}

export function sortPeriodKeys(periodKeys = []) {
  return [...new Set(periodKeys)].sort((left, right) => getPeriodSortValue(left) - getPeriodSortValue(right));
}

export function inferFallbackPeriod(sheetName = '', fileName = '') {
  return parsePeriodLabel(sheetName)?.periodKey
    || parsePeriodLabel(fileName)?.periodKey
    || '_single';
}

export function describePeriodKey(periodKey) {
  if (periodKey === '_single') {
    return { periodKey, granularity: 'point_in_time', sortValue: Number.MAX_SAFE_INTEGER };
  }

  return parsePeriodLabel(periodKey) || {
    periodKey,
    granularity: 'unknown',
    sortValue: Number.MAX_SAFE_INTEGER,
  };
}

function parsePeriodLabel(label) {
  const raw = String(label || '').trim();
  if (!raw) return null;

  const normalized = raw.toLowerCase().replace(/\s+/g, ' ').trim();

  if (/^(ltm|ttm)$/.test(normalized)) {
    return { periodKey: 'LTM', granularity: 'ltm', sortValue: 999999 };
  }

  const yearMatch = normalized.match(/^(fy\s*)?((19|20)\d{2})(e)?$/);
  if (yearMatch) {
    const year = Number(yearMatch[2]);
    return { periodKey: String(year), granularity: 'year', sortValue: year * 100 };
  }

  const yearWithinLabel = normalized.match(/(?:^|[^a-z0-9])(?:fy\s*)?((19|20)\d{2})(e)?(?:[^a-z0-9]|$)/);
  if (yearWithinLabel) {
    const year = Number(yearWithinLabel[1]);
    return { periodKey: String(year), granularity: 'year', sortValue: year * 100 };
  }

  const quarterMatch = normalized.match(/^(q([1-4]))[\s\-\/]*((19|20)?\d{2,4})$/)
    || normalized.match(/^((19|20)?\d{2,4})[\s\-\/]*(q([1-4]))$/);
  if (quarterMatch) {
    const quarter = Number(quarterMatch[2] || quarterMatch[4]);
    const yearToken = quarterMatch[3] || quarterMatch[1];
    const year = normalizeYear(yearToken);
    if (year) {
      return {
        periodKey: `${year}-Q${quarter}`,
        granularity: 'quarter',
        sortValue: year * 100 + quarter,
      };
    }
  }

  const isoMonthMatch = normalized.match(/^((19|20)\d{2})[\-\/]((0?[1-9])|(1[0-2]))$/);
  if (isoMonthMatch) {
    const year = Number(isoMonthMatch[1]);
    const month = isoMonthMatch[3].padStart(2, '0');
    return {
      periodKey: `${year}-${month}`,
      granularity: 'month',
      sortValue: year * 100 + Number(month),
    };
  }

  const namedMonthMatch = normalized.match(/^([a-z]+)[\s\-\/,]+((19|20)\d{2})$/);
  if (namedMonthMatch && MONTH_LOOKUP[namedMonthMatch[1]]) {
    const year = Number(namedMonthMatch[2]);
    const month = MONTH_LOOKUP[namedMonthMatch[1]];
    return {
      periodKey: `${year}-${month}`,
      granularity: 'month',
      sortValue: year * 100 + Number(month),
    };
  }

  return null;
}

function normalizeYear(value) {
  const raw = String(value || '').replace(/\D/g, '');
  if (raw.length === 4) return Number(raw);
  if (raw.length === 2) return Number(`20${raw}`);
  return null;
}

function getPeriodSortValue(periodKey) {
  if (periodKey === 'LTM') return 999999;
  const parsed = parsePeriodLabel(periodKey);
  return parsed?.sortValue || Number.MAX_SAFE_INTEGER;
}
