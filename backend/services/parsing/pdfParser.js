import { buildParsedDocument, buildParsingResponse } from './parsingUtils.js';

const Y_TOLERANCE = 3;
const GAP_AS_NEW_CELL = 24;
const OCR_RENDER_SCALE = 2;
let pdfjsLibPromise = null;
let ocrDependenciesPromise = null;

export async function parsePdfFile(file) {
  const pdfjsLib = await getPdfJsLib();
  const loadingTask = pdfjsLib.getDocument({
    data: new Uint8Array(file.buffer),
    useSystemFonts: true,
    isEvalSupported: false,
  });

  const pdfDocument = await loadingTask.promise;
  const sheets = [];
  const warnings = [];
  let ocrWorker = null;

  try {
    for (let pageNumber = 1; pageNumber <= pdfDocument.numPages; pageNumber += 1) {
      const page = await pdfDocument.getPage(pageNumber);
      const textContent = await page.getTextContent();
      const viewport = page.getViewport({ scale: 1 });
      const layout = extractPdfLayout(textContent.items || [], viewport);
      let rows = normalizePdfRows(layout.rows);
      let ocrApplied = false;
      let extractionMode = 'native_pdf_text';
      let layoutMetadata = layout.metadata;

      if (rows.length === 0) {
        try {
          ocrWorker = ocrWorker || await createOcrWorker();
          const ocrText = await recognizePdfPageText(page, ocrWorker);
          const ocrRows = parseOcrTextIntoRows(ocrText);
          rows = normalizePdfRows(ocrRows);
          ocrApplied = rows.length > 0;
          extractionMode = ocrApplied ? 'ocr_text' : 'ocr_unreadable';
          layoutMetadata = {
            columnCount: 1,
            footnotes: detectFootnotes(ocrRows),
            sectionTitles: detectSectionTitles(ocrRows),
            readingOrder: 'ocr-linear',
            tableCount: estimateTableCount(ocrRows),
          };

          if (ocrApplied) {
            warnings.push(`PDF page ${pageNumber} required OCR. OCR-derived rows are lower-confidence than native PDF text.`);
          } else {
            warnings.push(`PDF page ${pageNumber} appears image-based and OCR could not recover usable text.`);
            rows.push([`OCR required on page ${pageNumber}`]);
          }
        } catch (error) {
          warnings.push(`PDF page ${pageNumber} appears image-based and OCR failed: ${error.message}`);
          rows.push([`OCR required on page ${pageNumber}`]);
          extractionMode = 'ocr_failed';
        }
      }

      sheets.push({
        name: `Page ${pageNumber}`,
        rows,
        sourceKind: 'pdf-page',
        pageNumber,
        pageRange: { start: pageNumber, end: pageNumber },
        ocrApplied,
        ocrEngine: ocrApplied ? 'tesseract.js' : null,
        extractionMode,
        layoutMetadata,
      });
    }
  } finally {
    if (ocrWorker) {
      try {
        await ocrWorker.terminate();
      } catch (_error) {
        warnings.push('OCR worker cleanup failed after PDF parsing completed.');
      }
    }
    await loadingTask.destroy().catch(() => {});
  }

  const parsedDocument = buildParsedDocument({
    sourceType: 'pdf',
    sheets,
  });

  return buildParsingResponse({
    parser: 'pdf',
    parsedDocument,
    warnings,
  });
}

function extractPdfLayout(items, viewport) {
  const normalizedItems = items
    .map((item) => ({
      text: String(item?.str || '').trim(),
      x: Number(item?.transform?.[4] || 0),
      y: Number(item?.transform?.[5] || 0),
    }))
    .filter((item) => item.text);
  const columnBands = detectColumnBands(normalizedItems, Number(viewport?.width || 0));
  const rows = columnBands.flatMap((band, columnIndex) => extractRowsFromTextItems(
    normalizedItems.filter((item) => item.x >= band.minX && item.x <= band.maxX),
    columnIndex,
  ));
  const normalizedRows = rows.filter((row) => row.some((value) => value && value.trim().length > 0));
  const footnotes = detectFootnotes(normalizedRows);
  const sectionTitles = detectSectionTitles(normalizedRows);

  return {
    rows: normalizedRows,
    metadata: {
      columnCount: columnBands.length,
      footnotes,
      sectionTitles,
      readingOrder: columnBands.length > 1 ? 'column-major-left-to-right' : 'single-column',
      tableCount: estimateTableCount(normalizedRows),
    },
  };
}

function extractRowsFromTextItems(items, columnIndex = 0) {
  const groupedLines = [];

  for (const item of items) {
    const text = String(item?.text || '').trim();
    if (!text) continue;

    const x = Number(item?.x || 0);
    const y = Number(item?.y || 0);
    const existingLine = groupedLines.find((line) => Math.abs(line.y - y) <= Y_TOLERANCE);

    if (existingLine) {
      existingLine.items.push({ text, x });
      existingLine.y = average(existingLine.y, y);
      continue;
    }

    groupedLines.push({ y, items: [{ text, x }] });
  }

  return groupedLines
    .sort((left, right) => right.y - left.y)
    .map((line) => line.items.sort((left, right) => left.x - right.x))
    .map(splitLineIntoCells)
    .filter((row) => row.some((value) => value && value.trim().length > 0));
}

function detectColumnBands(items, viewportWidth) {
  if (!Array.isArray(items) || items.length < 24 || !Number.isFinite(viewportWidth) || viewportWidth <= 0) {
    return [{ minX: -Infinity, maxX: Infinity }];
  }

  const xPositions = items
    .map((item) => item.x)
    .filter(Number.isFinite)
    .sort((left, right) => left - right);
  if (xPositions.length < 24) {
    return [{ minX: -Infinity, maxX: Infinity }];
  }

  let bestGap = 0;
  let splitPoint = null;
  for (let index = 1; index < xPositions.length; index += 1) {
    const gap = xPositions[index] - xPositions[index - 1];
    if (gap > bestGap) {
      bestGap = gap;
      splitPoint = xPositions[index - 1] + (gap / 2);
    }
  }

  if (bestGap < Math.max(96, viewportWidth * 0.16) || splitPoint == null) {
    return [{ minX: -Infinity, maxX: Infinity }];
  }

  const leftCount = items.filter((item) => item.x <= splitPoint).length;
  const rightCount = items.filter((item) => item.x > splitPoint).length;
  if (leftCount < 10 || rightCount < 10) {
    return [{ minX: -Infinity, maxX: Infinity }];
  }

  return [
    { minX: -Infinity, maxX: splitPoint },
    { minX: splitPoint, maxX: Infinity },
  ];
}

function detectFootnotes(rows = []) {
  return rows
    .map((row) => row.filter(Boolean).join(' ').trim())
    .filter(Boolean)
    .filter((line) => /^(note|notes|footnote|\*|\(\d+\)|\[\d+\])/i.test(line))
    .slice(0, 6);
}

function detectSectionTitles(rows = []) {
  return rows
    .filter((row) => row.length === 1)
    .map((row) => String(row[0] || '').trim())
    .filter(Boolean)
    .filter((line) => !/^-?\$?\d/.test(line))
    .filter((line) => line.length <= 80)
    .slice(0, 8);
}

function estimateTableCount(rows = []) {
  let tableCount = 0;
  let inTabularBlock = false;

  rows.forEach((row) => {
    const tabular = Array.isArray(row) && row.length >= 2;
    if (tabular && !inTabularBlock) {
      tableCount += 1;
      inTabularBlock = true;
      return;
    }
    if (!tabular) {
      inTabularBlock = false;
    }
  });

  return tableCount || (rows.length > 0 ? 1 : 0);
}

function normalizePdfRows(rows) {
  if (!Array.isArray(rows) || rows.length === 0) return [];
  const [firstRow, ...remainingRows] = rows;
  const looksLikeTitle = Array.isArray(firstRow) && firstRow.length === 1 && String(firstRow[0] || '').trim().length > 0;
  const looksLikeKeyValueBody = remainingRows.length >= 2 && remainingRows.every((row) => Array.isArray(row) && row.length <= 2);

  if (looksLikeTitle && looksLikeKeyValueBody) {
    return [
      firstRow,
      ['Metric', 'Value'],
      ...remainingRows.map((row) => row.length === 1 ? [row[0], null] : row),
    ];
  }

  return rows;
}

function splitLineIntoCells(items) {
  const cells = [];
  let currentCell = '';
  let previousX = null;

  for (const item of items) {
    if (previousX != null && (item.x - previousX) >= GAP_AS_NEW_CELL) {
      cells.push(currentCell.trim() || null);
      currentCell = item.text;
    } else {
      currentCell = currentCell ? `${currentCell} ${item.text}` : item.text;
    }
    previousX = item.x + (item.text.length * 4);
  }

  if (currentCell) {
    cells.push(currentCell.trim());
  }

  return cells.length > 0 ? cells : [items.map((item) => item.text).join(' ')];
}

function average(left, right) {
  return (left + right) / 2;
}

async function recognizePdfPageText(page, worker) {
  const { createCanvas } = await getOcrDependencies();
  const viewport = page.getViewport({ scale: OCR_RENDER_SCALE });
  const canvas = createCanvas(Math.ceil(viewport.width), Math.ceil(viewport.height));
  const context = canvas.getContext('2d');
  await page.render({ canvasContext: context, viewport }).promise;
  const imageBuffer = canvas.toBuffer('image/png');
  const result = await worker.recognize(imageBuffer);
  return result?.data?.text || '';
}

async function getOcrDependencies() {
  if (!ocrDependenciesPromise) {
    ocrDependenciesPromise = Promise.all([
      import('@napi-rs/canvas'),
      import('tesseract.js'),
    ])
      .then(([canvasModule, tesseractModule]) => ({
        createCanvas: canvasModule.createCanvas,
        createWorker: tesseractModule.createWorker,
      }))
      .catch((error) => {
        ocrDependenciesPromise = null;
        throw new Error(`OCR dependencies are unavailable: ${error.message}`);
      });
  }

  return ocrDependenciesPromise;
}

async function getPdfJsLib() {
  if (!pdfjsLibPromise) {
    pdfjsLibPromise = import('pdfjs-dist/legacy/build/pdf.mjs')
      .catch((error) => {
        pdfjsLibPromise = null;
        throw new Error(`PDF parsing dependency failed to load: ${error.message}`);
      });
  }

  return pdfjsLibPromise;
}

async function createOcrWorker() {
  const { createWorker } = await getOcrDependencies();
  return createWorker('eng');
}

function parseOcrTextIntoRows(text) {
  return String(text || '')
    .split(/\n+/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => normalizeOcrRow(line))
    .filter((row) => row.length > 0);
}

function normalizeOcrRow(line) {
  const explicitCells = line.split(/\s{2,}|\t+/).map((cell) => cell.trim()).filter(Boolean);
  if (explicitCells.length > 1) return explicitCells;

  const trailingNumericMatch = line.match(/^(.*?)(-?\(?\$?\d[\d,]*(?:\.\d+)?%?\)?|[A-Za-z]{3,9}\s+\d{4})$/);
  if (trailingNumericMatch) {
    const label = trailingNumericMatch[1].trim();
    const value = trailingNumericMatch[2].trim();
    if (label && value) {
      return [label, value];
    }
  }

  return [line];
}
