import { runPipeline, getDocTypeLabel } from './ingestion/pipeline.js';
import { classifyDocument } from './ingestion/classifier.js';
import { DOC_TYPES } from './ingestion/schemas.js';
import { renderAllCharts } from './charts.js';
import {
  buildPipelineFileDescriptors,
  createEmptyReviewMemory,
  fetchReviewMemory,
  ingestFiles,
  saveReviewMemory,
} from './api.js';
import {
  applyReviewOverridesToIngestionResponse,
  buildReviewMemoryBundle,
  getStoredConceptSuppression,
  getStoredEntityResolution,
  getReviewOverrideFieldOptions,
  getStoredOverrideForRow,
  getStoredSourcePreference,
  getStoredTimeBasisOverride,
  loadConceptSuppressions,
  loadEntityResolutions,
  loadReviewOverrides,
  loadSourcePreferences,
  loadTimeBasisOverrides,
  removeConceptSuppression,
  removeEntityResolution,
  removeSourcePreference,
  removeTimeBasisOverride,
  removeReviewOverride,
  saveConceptSuppression,
  saveEntityResolution,
  saveSourcePreference,
  saveTimeBasisOverride,
  saveReviewOverride,
} from './ingestion/reviewOverrides.js';

const REVIEWER_ID_STORAGE_KEY = 'acquisitionclaw.reviewer-id.v1';
const DEFAULT_REVIEWER_ID = loadOrCreateReviewerId();

// ===== STATE =====
const state = {
  files: [],
  companyName: '',
  dealName: '',
  industry: '',
  ebitdaRange: '',
  reviewerId: DEFAULT_REVIEWER_ID,
  ingestionResult: null,
  isAnalyzing: false,
  activeReviewDocType: null,
  reviewMemory: createEmptyReviewMemory({ reviewerId: DEFAULT_REVIEWER_ID }),
};

// ===== DOM REFS =====
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

const dropZone = $('#drop-zone');
const fileInput = $('#file-input');
const browseBtn = $('#browse-btn');
const fileList = $('#file-list');
const uploadMessage = $('#upload-message');
const analyzeBtn = $('#analyze-btn');
const companyInput = $('#company-name');
const dealInput = $('#deal-name');
const reviewerInput = $('#reviewer-id');
const industrySelect = $('#industry');
const ebitdaSelect = $('#ebitda-range');
const analyzeBtnDefaultMarkup = analyzeBtn.innerHTML;

if (reviewerInput && !reviewerInput.value.trim()) {
  reviewerInput.value = DEFAULT_REVIEWER_ID;
}

const SAFE_STATUS_CLASSES = ['high', 'medium', 'low', 'validated', 'review', 'warning', 'hard_error', 'hard-error', 'note', 'critical', 'amber', 'green', 'blue', 'red'];
const SAFE_CONFIDENCE_CLASSES = ['high', 'medium', 'low'];

function escapeHtml(value = '') {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function escapeAttr(value = '') {
  return escapeHtml(value).replace(/`/g, '&#96;');
}

function safeClass(value, allowed = [], fallback = '') {
  const normalized = String(value || '').toLowerCase().trim();
  return allowed.includes(normalized) ? normalized : fallback;
}

function toFiniteNumber(value, fallback = 0) {
  const numeric = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(numeric) ? numeric : fallback;
}

function clampNumber(value, min, max) {
  return Math.min(Math.max(toFiniteNumber(value, min), min), max);
}

function sanitizeRichText(html = '') {
  if (typeof document === 'undefined') {
    return escapeHtml(html).replace(/\n/g, '<br>');
  }

  const template = document.createElement('template');
  template.innerHTML = String(html || '');
  const allowedTags = new Set(['BR', 'STRONG', 'EM', 'B', 'I', 'P', 'UL', 'OL', 'LI']);

  const sanitizeNode = (node) => {
    if (node.nodeType === Node.TEXT_NODE) return;
    if (node.nodeType !== Node.ELEMENT_NODE) {
      node.remove();
      return;
    }

    if (!allowedTags.has(node.tagName)) {
      const textNode = document.createTextNode(node.textContent || '');
      node.replaceWith(textNode);
      return;
    }

    [...node.attributes].forEach((attribute) => node.removeAttribute(attribute.name));
    [...node.childNodes].forEach(sanitizeNode);
  };

  [...template.content.childNodes].forEach(sanitizeNode);
  return template.innerHTML;
}

function loadOrCreateReviewerId() {
  if (typeof localStorage === 'undefined') return 'anonymous-reviewer';

  try {
    const existing = String(localStorage.getItem(REVIEWER_ID_STORAGE_KEY) || '').trim();
    if (existing) return existing;
    const generated = `reviewer-${Math.random().toString(36).slice(2, 8)}`;
    localStorage.setItem(REVIEWER_ID_STORAGE_KEY, generated);
    return generated;
  } catch (_error) {
    return 'anonymous-reviewer';
  }
}

function persistReviewerId(value) {
  if (typeof localStorage === 'undefined') return;
  try {
    localStorage.setItem(REVIEWER_ID_STORAGE_KEY, String(value || '').trim());
  } catch (_error) {
    // Ignore storage failures and keep the in-memory reviewer id.
  }
}

// ===== NAVIGATION =====
$$('.nav-btn').forEach(btn => {
  btn.addEventListener('click', () => {
    if (btn.disabled) return;
    showView(btn.dataset.view);
  });
});

function showView(name) {
  $$('.view').forEach(v => v.classList.remove('active'));
  $(`#view-${name}`).classList.add('active');
  $$('.nav-btn').forEach(b => b.classList.toggle('active', b.dataset.view === name));
}

// ===== FILE UPLOAD =====
function detectDocType(filename) {
  const result = classifyDocument({ name: filename, type: '', size: 0 });
  if (result.docType === DOC_TYPES.UNKNOWN) return null;
  return getDocTypeLabel(result.docType);
}

function getFileExt(name) {
  return name.split('.').pop().toUpperCase();
}

function addFiles(fileArray) {
  if (state.isAnalyzing) return;

  setUploadMessage('');
  state.ingestionResult = null;
  state.activeReviewDocType = null;
  for (const f of fileArray) {
    if (state.files.some(existing => existing.name === f.name && existing.size === f.size)) continue;
    state.files.push(f);
  }
  renderFileList();
  updateAnalyzeBtn();
}

function removeFile(index) {
  if (state.isAnalyzing) return;

  state.ingestionResult = null;
  state.activeReviewDocType = null;
  state.files.splice(index, 1);
  renderFileList();
  updateAnalyzeBtn();
}

function renderFileList() {
  fileList.innerHTML = state.files.map((f, i) => {
    const docType = detectDocType(f.name);
    return `<div class="file-item">
      <div class="file-info">
        <span class="file-type-badge">${escapeHtml(getFileExt(f.name))}</span>
        <span class="file-name">${escapeHtml(f.name)}</span>
        ${docType ? `<span class="file-doc-type">&mdash; ${escapeHtml(docType)}</span>` : ''}
      </div>
      <button class="file-remove" data-index="${i}" ${state.isAnalyzing ? 'disabled' : ''}>&times;</button>
    </div>`;
  }).join('');

  fileList.querySelectorAll('.file-remove').forEach(btn => {
    btn.addEventListener('click', () => removeFile(parseInt(btn.dataset.index)));
  });
}

function updateAnalyzeBtn() {
  analyzeBtn.disabled = state.isAnalyzing || state.files.length === 0 || !companyInput.value.trim();
}

function setUploadMessage(message, tone = 'error') {
  if (!uploadMessage) return;

  uploadMessage.hidden = !message;
  uploadMessage.textContent = message || '';
  uploadMessage.className = `upload-message ${tone}`;
}

function resetProcessingSteps() {
  $$('#processing-steps .step').forEach(step => {
    step.classList.remove('active', 'done');
  });
}

function setAnalyzeLoading(isLoading) {
  state.isAnalyzing = isLoading;
  analyzeBtn.classList.toggle('is-loading', isLoading);
  analyzeBtn.setAttribute('aria-busy', isLoading ? 'true' : 'false');
  analyzeBtn.innerHTML = isLoading
    ? '<span class="btn-spinner" aria-hidden="true"></span><span>Uploading files...</span>'
    : analyzeBtnDefaultMarkup;
  dropZone.classList.toggle('disabled', isLoading);
  browseBtn.disabled = isLoading;
  renderFileList();
  updateAnalyzeBtn();
}

// Events
dropZone.addEventListener('click', () => {
  if (state.isAnalyzing) return;
  fileInput.click();
});
browseBtn.addEventListener('click', (e) => {
  e.stopPropagation();
  if (state.isAnalyzing) return;
  fileInput.click();
});
fileInput.addEventListener('change', () => {
  addFiles(Array.from(fileInput.files));
  fileInput.value = '';
});

dropZone.addEventListener('dragover', (e) => {
  e.preventDefault();
  if (state.isAnalyzing) return;
  dropZone.classList.add('drag-over');
});
dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag-over'));
dropZone.addEventListener('drop', (e) => {
  e.preventDefault();
  dropZone.classList.remove('drag-over');
  if (state.isAnalyzing) return;
  addFiles(Array.from(e.dataTransfer.files));
});

companyInput.addEventListener('input', updateAnalyzeBtn);

// ===== ANALYZE =====
analyzeBtn.addEventListener('click', startAnalysis);

async function startAnalysis() {
  if (state.isAnalyzing) return;

  setUploadMessage('');
  state.companyName = companyInput.value.trim();
  state.dealName = dealInput?.value.trim() || '';
  state.industry = industrySelect.options[industrySelect.selectedIndex].text;
  state.ebitdaRange = ebitdaSelect.options[ebitdaSelect.selectedIndex].text;
  state.reviewerId = reviewerInput?.value.trim() || DEFAULT_REVIEWER_ID;
  persistReviewerId(state.reviewerId);
  state.ebitdaRangeValue = ebitdaSelect.value; // raw value like "1m-3m"
  if (state.industry === 'Select industry...') state.industry = '';
  if (state.ebitdaRange === 'Select range...') state.ebitdaRange = '';

  showView('processing');
  $('#processing-company').textContent = state.companyName;

  // Animate processing steps while running pipeline
  const steps = $$('#processing-steps .step');
  resetProcessingSteps();
  setAnalyzeLoading(true);

  let ingestResponse = null;
  let pipelineResult = null;

  try {
    for (let i = 0; i < steps.length; i++) {
      steps[i].classList.add('active');

      if (i === 0) {
        await hydrateReviewMemoryFromBackend();
        await delay(250);
      } else if (i === 1) {
        const companyContext = buildCompanyContext();
        ingestResponse = await ingestFiles(state.files, companyContext);
        state.ingestionResult = ingestResponse;

        if (!ingestResponse.summary?.acceptedFiles) {
          throw new Error('No supported files were accepted. Upload CSV, XLSX, or text-readable PDF files.');
        }

        await delay(250);
      } else if (i === 2) {
        const effectiveIngestion = applyReviewOverridesToIngestionResponse(ingestResponse, getEffectiveReviewMemory().reviewOverrides);
        const fileDescriptors = buildPipelineFileDescriptors(effectiveIngestion);
        pipelineResult = runPipeline(fileDescriptors, buildCompanyContext());
        await delay(300);
      } else {
        await delay(500 + Math.random() * 400);
      }

      steps[i].classList.remove('active');
      steps[i].classList.add('done');
    }

    state.lastIngestion = ingestResponse;
    state.lastDiagnostics = {
      ...(pipelineResult?.diagnostics || {}),
      backendIngestion: applyReviewOverridesToIngestionResponse(ingestResponse, getEffectiveReviewMemory().reviewOverrides),
    };

    await delay(300);
    showDashboard(pipelineResult?.dashboardData);
  } catch (error) {
    console.error(error);
    state.ingestionResult = null;
    resetProcessingSteps();
    showView('upload');
    setUploadMessage(error.message || 'File ingestion failed. Check the backend and try again.');
  } finally {
    setAnalyzeLoading(false);
  }
}

function buildReviewScope() {
  return {
    companyName: state.companyName,
    dealName: state.dealName || dealInput?.value.trim() || 'primary-deal',
    reviewerId: state.reviewerId || reviewerInput?.value.trim() || DEFAULT_REVIEWER_ID,
  };
}

function buildCompanyContext() {
  const reviewMemory = getEffectiveReviewMemory();
  return {
    companyName: state.companyName,
    dealName: buildReviewScope().dealName,
    reviewerId: buildReviewScope().reviewerId,
    industry: state.industry,
    ebitdaRange: state.ebitdaRangeValue || '1m-3m',
    allowDemoFallback: false,
    reviewMemory,
    reviewAliasRules: reviewMemory.learnedAliasRules || [],
    reviewRankingSignals: reviewMemory.reviewerSignals || null,
  };
}

function getEffectiveReviewMemory() {
  const scope = buildReviewScope();
  if (state.reviewMemory && typeof state.reviewMemory === 'object') {
    return {
      ...createEmptyReviewMemory(scope),
      ...state.reviewMemory,
      companyName: state.reviewMemory.companyName || scope.companyName,
      dealName: state.reviewMemory.dealName || scope.dealName,
      reviewerId: state.reviewMemory.reviewerId || scope.reviewerId,
      reviewOverrides: Array.isArray(state.reviewMemory.reviewOverrides) ? state.reviewMemory.reviewOverrides : [],
      sourcePreferences: Array.isArray(state.reviewMemory.sourcePreferences) ? state.reviewMemory.sourcePreferences : [],
      conceptSuppressions: Array.isArray(state.reviewMemory.conceptSuppressions) ? state.reviewMemory.conceptSuppressions : [],
      timeBasisOverrides: Array.isArray(state.reviewMemory.timeBasisOverrides) ? state.reviewMemory.timeBasisOverrides : [],
      entityResolutions: Array.isArray(state.reviewMemory.entityResolutions) ? state.reviewMemory.entityResolutions : [],
      learnedAliasRules: Array.isArray(state.reviewMemory.learnedAliasRules) ? state.reviewMemory.learnedAliasRules : [],
      reviewerSignals: state.reviewMemory.reviewerSignals || null,
      recentHistory: Array.isArray(state.reviewMemory.recentHistory) ? state.reviewMemory.recentHistory : [],
    };
  }
  return createEmptyReviewMemory(scope);
}

async function hydrateReviewMemoryFromBackend() {
  if (!state.companyName) {
    state.reviewMemory = createEmptyReviewMemory(buildReviewScope());
    return state.reviewMemory;
  }

  try {
    state.reviewMemory = await fetchReviewMemory(buildReviewScope());
  } catch (error) {
    console.warn('Review memory load failed, falling back to local browser state:', error);
    const fallbackBundle = buildReviewMemoryBundle({
      reviewOverrides: loadReviewOverrides(),
      sourcePreferences: loadSourcePreferences(),
      conceptSuppressions: loadConceptSuppressions(),
      timeBasisOverrides: loadTimeBasisOverrides(),
      entityResolutions: loadEntityResolutions(),
    });
    state.reviewMemory = {
      ...createEmptyReviewMemory(buildReviewScope()),
      ...fallbackBundle,
      companyName: buildReviewScope().companyName,
      dealName: buildReviewScope().dealName,
      reviewerId: buildReviewScope().reviewerId,
      loadError: error.message || 'Review memory could not be loaded from the backend.',
    };
  }

  return getEffectiveReviewMemory();
}

async function persistReviewMemoryState() {
  if (!state.companyName) return getEffectiveReviewMemory();

  const scope = buildReviewScope();
  const currentMemory = getEffectiveReviewMemory();

  try {
    state.reviewMemory = await saveReviewMemory(scope, currentMemory);
  } catch (error) {
    if (/changed on the server/i.test(String(error?.message || ''))) {
      try {
        const latestMemory = await fetchReviewMemory(scope);
        const mergedMemory = mergeReviewMemoryStates(latestMemory, currentMemory);
        state.reviewMemory = await saveReviewMemory(scope, mergedMemory);
        return getEffectiveReviewMemory();
      } catch (mergeError) {
        console.error('Failed to merge reviewer memory after a revision conflict:', mergeError);
      }
    }

    console.error('Failed to persist review memory:', error);
    state.reviewMemory = {
      ...currentMemory,
      loadError: error.message || 'Review memory could not be persisted to the backend.',
    };
  }

  return getEffectiveReviewMemory();
}

function mergeReviewMemoryStates(remoteMemory, localMemory) {
  const mergedBundle = buildReviewMemoryBundle({
    reviewOverrides: mergeUniqueByKey(remoteMemory.reviewOverrides, localMemory.reviewOverrides, (entry) => entry.id),
    sourcePreferences: mergeUniqueByKey(remoteMemory.sourcePreferences, localMemory.sourcePreferences, (entry) => entry.conceptKey),
    conceptSuppressions: mergeUniqueByKey(remoteMemory.conceptSuppressions, localMemory.conceptSuppressions, (entry) => `${entry.conceptKey}::${entry.docType}::${entry.sourceRefKey || ''}`),
    timeBasisOverrides: mergeUniqueByKey(remoteMemory.timeBasisOverrides, localMemory.timeBasisOverrides, (entry) => `${entry.docType}::${entry.sourceRefKey || ''}`),
    entityResolutions: mergeUniqueByKey(remoteMemory.entityResolutions, localMemory.entityResolutions, (entry) => entry.id),
  });

  return {
    ...remoteMemory,
    ...mergedBundle,
    companyName: localMemory.companyName || remoteMemory.companyName,
    dealName: localMemory.dealName || remoteMemory.dealName,
    reviewerId: localMemory.reviewerId || remoteMemory.reviewerId,
    revision: remoteMemory.revision || 0,
    recentHistory: remoteMemory.recentHistory || [],
  };
}

function mergeUniqueByKey(leftItems = [], rightItems = [], buildKey) {
  const merged = new Map();
  [...(leftItems || []), ...(rightItems || [])].forEach((entry) => {
    const key = buildKey(entry);
    if (!key) return;
    merged.set(key, entry);
  });
  return [...merged.values()];
}

function delay(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// ===== MOCK DATA =====
// Structured so real extracted data can replace this object directly.
function getMockData() {
  return {
    overallScore: 74,
    verdict: 'Moderately Healthy — Suitable for Acquisition with Conditions',
    description: 'The target demonstrates solid top-line growth and acceptable margins, but carries elevated leverage and notable customer concentration risk. Earnings quality is adequate with some adjustments required for owner compensation normalization. A structured earnout and thorough QoE are recommended before proceeding.',
    subScores: [
      { key: 'profitability', label: 'Profitability', score: 81, note: 'Gross margins trending up; EBITDA margin 18.2%', confidence: 'high',
        explanation: 'EBITDA margin of 18.2% vs. 14% industry median, with an improving trend. Margins support healthy acquisition economics.',
        metrics: [{ name: 'Gross Margin', value: '45.8%' }, { name: 'EBITDA Margin', value: '18.2%' }, { name: 'Net Margin', value: '10.1%' }],
        logic: 'Scores EBITDA margin level relative to industry benchmark, adjusted for trend direction and gross margin adequacy.' },
      { key: 'revenueStability', label: 'Revenue Stability', score: 76, note: 'Revenue CAGR of ~8% with consistent growth', confidence: 'high',
        explanation: 'Revenue CAGR of 8% over 5 periods. No revenue declines observed. Growth volatility is low. Top-line trajectory supports acquisition valuation.',
        metrics: [{ name: 'Revenue CAGR', value: '8.0%' }, { name: 'Latest Revenue', value: '$12.0M' }, { name: 'Periods Observed', value: '5' }],
        logic: 'Evaluates revenue CAGR, consistency of growth across periods, and YoY volatility.' },
      { key: 'liquidity', label: 'Liquidity', score: 72, note: 'Current ratio 1.4x; working capital adequate', confidence: 'high',
        explanation: 'Current ratio of 1.4x indicates adequate working capital. Liquidity supports normal operations and debt service.',
        metrics: [{ name: 'Current Ratio', value: '1.4x' }, { name: 'Cash', value: '$1.2M' }],
        logic: 'Scores current ratio on a tiered scale (2.0x+ = strong, <1.0x = weak), adjusted for trend direction and cash adequacy.' },
      { key: 'leverage', label: 'Leverage', score: 55, note: 'Debt/EBITDA 3.8x — above target range', confidence: 'high',
        explanation: 'Leverage is elevated at 3.8x Debt/EBITDA. Interest coverage of 3.9x may constrain debt service. Leverage presents refinancing and debt capacity risk.',
        metrics: [{ name: 'Debt / EBITDA', value: '3.8x' }, { name: 'Interest Coverage', value: '3.9x' }],
        logic: 'Tiered scoring on Debt/EBITDA (<1.5x = strong, >4.5x = weak), adjusted for interest coverage adequacy and leverage trend.' },
      { key: 'cashConversion', label: 'Cash Conversion', score: 78, note: 'OCF/EBITDA conversion 89%; low capex intensity', confidence: 'high',
        explanation: 'Operating cash flow converts at 89% of EBITDA. Strong conversion indicates high-quality, cash-generative earnings.',
        metrics: [{ name: 'OCF / EBITDA', value: '89%' }, { name: 'Operating Cash Flow', value: '$2.18M' }],
        logic: 'Scores OCF/EBITDA conversion ratio on a tiered scale (95%+ = excellent, <50% = weak), with penalty for high capital intensity.' },
      { key: 'concentration', label: 'Customer Concentration', score: 58, note: 'Top 3 customers = 47% of revenue', confidence: 'high',
        explanation: 'Customer base is highly concentrated with the top customer at 22% of revenue and top 3 at 47%. Concentration risk should be addressed via earnout or holdback mechanisms in deal structure.',
        metrics: [{ name: 'Top Customer', value: '22%' }, { name: 'Top 3 Customers', value: '47%' }, { name: 'Total Customers', value: '40' }],
        logic: 'Tiered scoring on top customer revenue share (<8% = excellent, >35% = critical). Penalized further when top 3 exceed 50%.' },
      { key: 'earningsQuality', label: 'Earnings Quality', score: 76, note: 'Low one-time adjustments; stable add-backs', confidence: 'high',
        explanation: 'EBITDA add-backs represent 8% of adjusted EBITDA. Moderate add-backs — each line item should be validated in QoE.',
        metrics: [{ name: 'Add-backs / Adj. EBITDA', value: '8%' }, { name: 'Owner Comp Above Market', value: '$180K' }],
        logic: 'Scores total add-backs as a percentage of adjusted EBITDA (<5% = excellent, >25% = poor). Flags aggressive owner compensation.' },
      { key: 'forecastCredibility', label: 'Forecast Credibility', score: 69, note: 'Projections assume 12% growth vs 8% historical', confidence: 'high',
        explanation: 'Management projects 12% growth vs. a trailing CAGR of 8.1% (3.9pp gap). Moderate projection gap — validate key growth assumptions.',
        metrics: [{ name: 'Projected Growth', value: '12%' }, { name: 'Historical CAGR', value: '8.1%' }, { name: 'Projection Gap', value: '3.9pp' }],
        logic: 'Scores the gap between projected growth rate and historical CAGR (<2pp = excellent, >12pp = poor).' },
    ],
    overallConfidence: 'high',
    strengths: [
      { text: 'EBITDA margin of 18.2% vs. 14% industry median, with an improving trend.', dimension: 'Profitability', score: 81 },
      { text: 'Operating cash flow converts at 89% of EBITDA.', dimension: 'Cash Conversion', score: 78 },
    ],
    risks: [
      { severity: 'high', text: 'Top customer at 22% of revenue — loss would materially impair economics', dimension: 'Customer Concentration' },
      { severity: 'high', text: 'Debt/EBITDA of 3.8x exceeds typical acquisition financing threshold', dimension: 'Leverage' },
      { severity: 'medium', text: 'Owner compensation exceeds market rate by ~$180K — requires normalization', dimension: 'Earnings Quality' },
      { severity: 'medium', text: 'Projections assume 12% growth vs. 8.1% historical — 3.9pp gap', dimension: 'Forecast Credibility' },
    ],
    missingItems: [],
    riskFlags: [
      { severity: 'high', text: 'Customer concentration: top client represents 22% of trailing revenue. Loss would materially impair cash flow coverage ratios.' },
      { severity: 'high', text: 'Leverage is elevated at 3.8x Debt/EBITDA. Refinancing risk in a rising rate environment.' },
      { severity: 'medium', text: 'Owner compensation above market by ~$180K. Requires normalization in QoE analysis.' },
      { severity: 'medium', text: 'Forecast assumes 12% YoY growth but trailing 3-year CAGR is 8.1%. Gap needs management substantiation.' },
      { severity: 'low', text: 'Minor working capital seasonality — Q4 AR build-up typically resolves in Q1.' },
    ],
    revenue: {
      labels: ['2021', '2022', '2023', '2024', 'LTM'],
      revenue: [8.2, 9.4, 10.1, 11.3, 12.0],
      ebitda: [1.3, 1.6, 1.7, 2.0, 2.18],
    },
    margins: {
      labels: ['2021', '2022', '2023', '2024', 'LTM'],
      gross: [42, 43.5, 44.1, 45.2, 45.8],
      ebitda: [15.9, 17.0, 16.8, 17.7, 18.2],
      net: [8.1, 9.2, 8.5, 9.8, 10.1],
    },
    leverage: {
      labels: ['2021', '2022', '2023', '2024', 'LTM'],
      debtToEbitda: [2.9, 3.1, 3.5, 3.7, 3.8],
      interestCoverage: [5.2, 4.8, 4.3, 4.1, 3.9],
    },
    cashflow: {
      labels: ['Operating', 'Investing', 'Financing', 'Free Cash Flow'],
      values: [2.18, -0.65, -0.92, 1.53],
    },
    customerBreakdown: {
      customers: [
        { name: 'Customer A', percentage: 22 },
        { name: 'Customer B', percentage: 14 },
        { name: 'Customer C', percentage: 11 },
        { name: 'Customer D', percentage: 8 },
        { name: 'Customer E', percentage: 6 },
        { name: 'Other (35)', percentage: 39 },
      ],
    },
    expenseComposition: {
      labels: ['COGS', 'Operating Expenses', 'D&A', 'Interest', 'Tax & Other'],
      values: [6.5, 2.8, 0.3, 0.26, 0.14],
    },
    arAging: {
      labels: ['Current', '1–30 Days', '31–60 Days', '61–90 Days', '90+ Days'],
      values: [1092000, 302400, 151200, 84000, 50400],
    },
    apAging: {
      labels: ['Current', '1–30 Days', '31–60 Days', '61–90 Days', '90+ Days'],
      values: [691200, 153600, 67200, 28800, 19200],
    },
    debtProfile: {
      instruments: [
        { name: 'Senior Term Loan A', principal: 4.6, rate: 6.5, maturity: '2027-06-15' },
        { name: 'Revolving Credit', principal: 1.7, rate: 5.75, maturity: '2026-12-01' },
        { name: 'Subordinated Note', principal: 2.1, rate: 9.0, maturity: '2029-03-01' },
      ],
    },
    forecastComparison: {
      labels: ['2021', '2022', '2023', '2024', 'LTM', '2025', '2026', '2027'],
      historicalRevenue: [8.2, 9.4, 10.1, 11.3, 12.0, null, null, null],
      projectedRevenue: [null, null, null, null, null, 13.4, 15.0, 16.8],
      historicalEbitda: [1.3, 1.6, 1.7, 2.0, 2.18],
      projectedEbitda: [2.5, 2.9, 3.4],
      dividerIndex: 5,
    },
    workingCapital: {
      labels: ['2021', '2022', '2023', '2024', 'LTM'],
      currentAssets: [3.2, 3.5, 3.8, 4.1, 4.3],
      currentLiabilities: [1.8, 2.0, 2.1, 2.3, 2.4],
      netWorkingCapital: [1.4, 1.5, 1.7, 1.8, 1.9],
    },
    investmentSummary: `<strong>${'The target'}</strong> is a mid-market business generating $12.0M in trailing revenue with $2.18M adjusted EBITDA (18.2% margin). The company has demonstrated consistent top-line growth of ~8% CAGR over the past three years with expanding gross margins, indicating pricing power and operational efficiency gains.<br><br>Key strengths include strong cash flow conversion (89% OCF/EBITDA), low capital intensity, and a defensible market position in its niche. However, meaningful customer concentration (top 3 = 47% of revenue) and above-target leverage (3.8x) present integration and financing risks that should be addressed through deal structuring.<br><br>At a preliminary valuation range of 5.0–6.5x EBITDA ($10.9M–$14.2M enterprise value), the deal is actionable contingent on satisfactory Quality of Earnings findings and negotiation of customer retention provisions.`,
    acquisitionAdvice: {
      attractiveness: 'Potentially attractive, but only with targeted diligence',
      confidence: 'high',
      summary: 'The opportunity is actionable for a search-fund buyer, but valuation and structure should remain conditional until the main risk items and document gaps are closed. Confidence is high.',
      keyRisks: [
        { severity: 'high', text: 'Top customer at 22% of revenue could drive a retention-based earnout or holdback.', dimension: 'Customer Concentration' },
        { severity: 'high', text: 'Debt / EBITDA of 3.8x may limit financing flexibility and increase refinance sensitivity.', dimension: 'Leverage' },
        { severity: 'medium', text: 'Owner compensation normalization still needs third-party validation.', dimension: 'Earnings Quality' },
      ],
      criticalBeforeLoi: [
        {
          priority: 1,
          category: 'financial verification',
          title: 'Close financial support gaps before tightening valuation',
          confidence: 'high',
          body: 'Do not treat the current EBITDA and leverage view as fully bankable until QoE support, debt detail, and customer-level revenue support are in hand. Keep any LOI range conditional rather than presenting a clean number.',
          support: {
            requests: ['Quality of Earnings Report not provided', 'Debt Schedule not provided', 'Customer Revenue Breakdown not provided'],
            valuationImpact: 'Use diligence conditions or a wider valuation range until the financial package is complete.',
          },
        },
        {
          priority: 2,
          category: 'customer diligence',
          title: 'Underwrite top-account durability',
          confidence: 'high',
          body: 'With the top customer at 22% of revenue, the buyer should request contract terms, account margin, and renewal history before moving to a cleaner LOI. A retention-based holdback is worth considering if relationships are seller-driven.',
          support: {
            managementQuestions: [
              'Which major customer relationships are personally owned by the seller?',
              'What renewals or repricing events are expected in the next 12 months?',
            ],
            structure: 'Potential earnout or holdback tied to customer retention.',
          },
        },
      ],
      importantDuringDiligence: [
        {
          priority: 3,
          category: 'working capital diligence',
          title: 'Set a normalized working-capital peg',
          confidence: 'high',
          body: 'Request trailing monthly AR/AP and working-capital detail to establish a defendable peg and identify any slow-pay or collection issues that should be handled through a true-up.',
          support: {
            requests: ['Trailing 12-month monthly working-capital rollforward', 'AR aging and bad-debt history', 'AP aging and key vendor terms'],
          },
        },
        {
          priority: 4,
          category: 'management reliance / owner dependence',
          title: 'Map owner dependence before confirmatory diligence ends',
          confidence: 'medium',
          body: 'The buyer should understand whether revenue, lender, or vendor relationships are concentrated in the owner. This is a common issue in search-fund transactions and may require a transition agreement or seller rollover.',
          support: {
            managementQuestions: [
              'Which decisions still require direct owner approval today?',
              'Who are the second-layer leaders critical to continuity after close?',
            ],
          },
        },
      ],
    },
    growthOpportunities: {
      summary: 'The target shows 5 data-backed value-creation levers worth underwriting. The cleanest path appears to be selective pricing inside major accounts, disciplined overhead cleanup, and capture of seller-specific normalization items rather than a heroic growth plan.',
      topOpportunities: [
        {
          category: 'Revenue Growth Opportunities',
          title: 'Targeted price-and-mix expansion within the largest accounts',
          whyItExists: 'Top customer concentration is high, but gross margin has expanded from 42.0% to 45.8% while revenue continued to grow. That combination suggests pricing power and room to sell higher-value work into the existing base.',
          supportingMetrics: ['Top 3 customers represent 47% of revenue', 'Gross margin improved to 45.8%', 'Historical revenue CAGR: 8.0%'],
          estimatedImpact: '$0.03M-$0.07M EBITDA',
          confidence: 'medium',
          executionWindow: 'quick win',
          underwritingAssumptions: [
            'Assumes 1% to 2% price-and-mix improvement inside the top 3 customer base.',
            'Assumes 75% to 80% flow-through at the current gross-margin profile.'
          ],
          evidenceDocuments: [
            { label: 'Income Statement / P&L', provided: true },
            { label: 'Revenue Breakdown / Customer Concentration', provided: true },
          ],
        },
        {
          category: 'Margin Expansion Opportunities',
          title: 'SG&A rationalization against the current revenue base',
          whyItExists: 'Operating expense is meaningful relative to scale, which suggests room to tighten overhead, remove owner-era spend, and institutionalize budgeting without relying on aggressive growth assumptions.',
          supportingMetrics: ['Operating expenses are 23.3% of revenue', 'EBITDA margin: 18.2%', 'Trailing revenue: $12.0M'],
          estimatedImpact: '$0.14M-$0.28M EBITDA',
          confidence: 'medium',
          executionWindow: 'quick win',
          underwritingAssumptions: [
            'Assumes 5% to 10% of operating expense can be removed without impairing growth.',
            'Assumes savings are overhead-related rather than customer-facing.'
          ],
          evidenceDocuments: [
            { label: 'Income Statement / P&L', provided: true },
            { label: 'Quality of Earnings Report', provided: false },
          ],
        },
        {
          category: 'Operational Improvements',
          title: 'Systematize procurement and gross-margin discipline',
          whyItExists: 'Gross margin has already expanded, which indicates there is a real operating playbook to formalize post-close around pricing discipline, purchasing, and account profitability tracking.',
          supportingMetrics: ['Gross margin expanded from 42.0% to 45.8%', 'Current COGS base: $6.5M'],
          estimatedImpact: '$0.06M-$0.12M EBITDA',
          confidence: 'medium',
          executionWindow: 'post-close initiative',
          underwritingAssumptions: [
            'Assumes another 50 to 100 bps of gross-margin improvement on the current revenue base.'
          ],
          evidenceDocuments: [
            { label: 'Income Statement / P&L', provided: true },
          ],
        },
        {
          category: 'Strategic / Acquisition Levers',
          title: 'Capture immediate EBITDA lift through owner-cost normalization',
          whyItExists: 'Owner compensation appears above market, which makes this one of the clearest sources of underwritable EBITDA improvement available to a buyer.',
          supportingMetrics: ['Owner compensation above market: ~$180K', 'Current EBITDA: $2.18M'],
          estimatedImpact: '$0.16M-$0.18M EBITDA',
          confidence: 'high',
          executionWindow: 'quick win',
          underwritingAssumptions: [
            'Assumes the identified owner compensation is genuinely non-recurring.',
            'Assumes no replacement hire is required beyond market-rate compensation already embedded in the adjustment.'
          ],
          evidenceDocuments: [
            { label: 'Quality of Earnings Report', provided: false },
            { label: 'Tax Return', provided: false },
          ],
        },
        {
          category: 'Operational Improvements',
          title: 'Tighten collections and order-to-cash execution',
          whyItExists: 'A meaningful portion of receivables sits past 30 days, which usually points to billing and collections leakage that can be tightened post-close.',
          supportingMetrics: ['17.0% of AR is older than 30 days', 'Total AR balance: $1.68M', 'Latest net working capital: $1.9M'],
          estimatedImpact: '$0.02M-$0.07M EBITDA',
          confidence: 'medium',
          executionWindow: 'quick win',
          underwritingAssumptions: [
            'Assumes better collections reduce bad debt, credits, and billing friction rather than creating a pure cash-only benefit.'
          ],
          evidenceDocuments: [
            { label: 'Accounts Receivable Aging', provided: true },
            { label: 'Balance Sheet', provided: true },
          ],
        },
      ],
      quickWins: [],
      postCloseInitiatives: [],
      categories: [],
    },
    nextSteps: [
      { title: 'Commission Quality of Earnings (QoE)', desc: 'Engage a third-party accounting firm to validate adjusted EBITDA, normalize owner comp, and stress-test working capital assumptions.' },
      { title: 'Customer Concentration Due Diligence', desc: 'Request detailed contract terms for top 5 customers. Assess renewal risk, switching costs, and historical churn. Consider holdback or earnout tied to key account retention.' },
      { title: 'Debt Capacity & Financing Structure', desc: 'Model acquisition financing at 3.0–3.5x senior leverage. Explore SBA 7(a) or mezzanine tranches to bridge gap. Sensitivity-test debt service coverage under downside scenarios.' },
      { title: 'Management Transition Assessment', desc: 'Evaluate owner dependency. If owner is critical to key relationships, negotiate a 12–24 month transition period with incentive alignment.' },
      { title: 'Preliminary LOI & Deal Structuring', desc: 'Draft LOI at 5.5x EBITDA with 15–20% seller note and earnout tied to revenue retention and EBITDA targets over 24 months post-close.' },
    ],
  };
}

// ===== DASHBOARD RENDERING =====
function showDashboard(pipelineData) {
  const data = pipelineData || getMockData();
  const overallScore = clampNumber(data.overallScore, 0, 100);

  // Update header
  $('#dash-company-name').textContent = state.companyName;
  $('#dash-industry').textContent = state.industry || 'Private Company';
  $('#dash-ebitda').textContent = state.ebitdaRange ? `EBITDA: ${state.ebitdaRange}` : '';
  if (!state.ebitdaRange) $('#dash-ebitda').style.display = 'none';
  const now = new Date();
  $('#dash-date').textContent = now.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });

  // Enable nav
  $('#nav-dashboard').disabled = false;

  // Overall score
  animateScore(overallScore);
  $('#score-verdict').textContent = data.verdict;
  $('#score-verdict').style.color = scoreColor(overallScore);
  $('#score-description').textContent = data.description;

  // Overall confidence badge (inject after description if present)
  const descEl = $('#score-description');
  const existingBadge = descEl.parentElement.querySelector('.overall-confidence');
  if (existingBadge) existingBadge.remove();
  if (data.overallConfidence) {
    const badge = document.createElement('span');
    const confidenceClass = safeClass(data.overallConfidence, SAFE_CONFIDENCE_CLASSES, 'low');
    badge.className = `overall-confidence ${confidenceClass}`;
    badge.textContent = `${data.overallConfidence} confidence`;
    descEl.parentElement.appendChild(badge);
  }

  // Strengths
  const strengthsList = $('#strengths-list');
  if (data.strengths && data.strengths.length > 0) {
    strengthsList.innerHTML = data.strengths.map(s => `
      <div class="strength-item">
        <span class="strength-dim">${escapeHtml(s.dimension)}</span>
        <span>${escapeHtml(s.text)}</span>
      </div>
    `).join('');
  } else {
    strengthsList.innerHTML = '<div class="empty-state">No standout strengths identified</div>';
  }

  // Risks
  const risksList = $('#risks-list');
  if (data.risks && data.risks.length > 0) {
    risksList.innerHTML = data.risks.slice(0, 6).map(r => `
      <div class="risk-item ${safeClass(r.severity, SAFE_STATUS_CLASSES, 'low')}">
        <span class="risk-dim">${escapeHtml(r.dimension)}</span>
        <span>${escapeHtml(r.text)}</span>
      </div>
    `).join('');
  } else {
    risksList.innerHTML = '<div class="empty-state">No significant risks identified</div>';
  }

  // Sub-scores (expandable cards with explanation, metrics, logic)
  const grid = $('#sub-scores-grid');
  grid.innerHTML = data.subScores.map(s => {
    const score = clampNumber(s.score, 0, 100);
    const metricsHtml = (s.metrics || []).map(m =>
      `<span class="sub-score-metric"><strong>${escapeHtml(m.name)}:</strong> ${escapeHtml(m.value)}</span>`
    ).join('');

    return `<div class="sub-score-card" data-key="${escapeAttr(s.key)}">
      <span class="sub-score-expand-icon">&#9662;</span>
      <div class="sub-score-label">${escapeHtml(s.label)}</div>
      <div class="sub-score-value" style="color:${scoreColor(score)}">${score}</div>
      <div class="sub-score-bar"><div class="sub-score-bar-fill" data-width="${score}" style="background:${scoreColor(score)}"></div></div>
      <div class="sub-score-note">${escapeHtml(s.note)}</div>
      ${s.confidence ? `<span class="sub-score-confidence ${safeClass(s.confidence, SAFE_CONFIDENCE_CLASSES, 'low')}">${escapeHtml(s.confidence)} confidence</span>` : ''}
      <div class="sub-score-detail">
        <div class="sub-score-explanation">${escapeHtml(s.explanation || '')}</div>
        ${metricsHtml ? `<div class="sub-score-metrics">${metricsHtml}</div>` : ''}
        ${s.logic ? `<div class="sub-score-logic">${escapeHtml(s.logic)}</div>` : ''}
      </div>
    </div>`;
  }).join('');

  // Expand/collapse on click
  grid.querySelectorAll('.sub-score-card').forEach(card => {
    card.addEventListener('click', () => card.classList.toggle('expanded'));
  });

  // Animate bars after paint
  requestAnimationFrame(() => {
    setTimeout(() => {
      grid.querySelectorAll('.sub-score-bar-fill').forEach(bar => {
        bar.style.width = bar.dataset.width + '%';
      });
    }, 100);
  });

  renderDataQuality(data.dataQuality);

  // Missing diligence items
  const missingEl = $('#missing-items');
  const missingTitle = $('#missing-title');
  if (data.missingItems && data.missingItems.length > 0) {
    missingTitle.style.display = '';
    missingEl.innerHTML = data.missingItems.map(m => `
      <div class="missing-item">
        <span class="missing-impact ${safeClass(m.impact, SAFE_STATUS_CLASSES, 'note')}">${escapeHtml(m.impact)}</span>
        <div>
          <div class="missing-text">${escapeHtml(m.text)}</div>
          ${m.affects ? `<div class="missing-affects">Affects: ${escapeHtml(m.affects)}</div>` : ''}
        </div>
      </div>
    `).join('');
  } else {
    missingTitle.style.display = 'none';
    missingEl.innerHTML = '';
  }

  // Risk flags
  $('#risk-flags').innerHTML = (data.riskFlags || []).map(r => `
    <div class="risk-flag ${safeClass(r.severity, SAFE_STATUS_CLASSES, 'low')}">
      <span class="risk-flag-severity">${escapeHtml(r.severity)}</span>
      <span>${escapeHtml(r.text)}</span>
    </div>
  `).join('');

  // Charts
  renderAnalyticsStudio(data);
  renderAllCharts(data);

  // Investment summary
  const investmentSummary = String(data.investmentSummary || '').replace('The target', state.companyName);
  $('#investment-summary').innerHTML = sanitizeRichText(investmentSummary);

  // Acquisition advice
  renderAcquisitionAdvice(data.acquisitionAdvice);

  // Growth opportunities
  renderGrowthOpportunities(data.growthOpportunities);

  // Next steps
  $('#next-steps').innerHTML = data.nextSteps.map((s, i) => `
    <div class="next-step">
      <div class="next-step-number">${i + 1}</div>
      <div>
        <div class="next-step-title">${escapeHtml(s.title)}</div>
        <div class="next-step-desc">${escapeHtml(s.desc)}</div>
      </div>
    </div>
  `).join('');

  showView('dashboard');
}

function renderAnalyticsStudio(data) {
  renderAnalyticsKpis(data);
  renderSignalMap(data);
  renderConcentrationMonitor(data);
  renderDebtLadder(data);
}

function renderDataQuality(dataQuality) {
  const summaryEl = $('#quality-summary');
  const docsEl = $('#quality-documents');
  const findingsEl = $('#quality-findings');
  const missingEl = $('#quality-missing');

  if (!summaryEl || !docsEl || !findingsEl || !missingEl) return;

  if (!dataQuality) {
    summaryEl.innerHTML = '<div class="empty-state">No ingestion quality metrics available</div>';
    docsEl.innerHTML = '';
    findingsEl.innerHTML = '';
    missingEl.innerHTML = '';
    renderProvenanceReviewPanel(null);
    renderAssumptionPanels(null);
    renderEvidencePanels(null);
    return;
  }

  const extraction = dataQuality.extractionConfidence || {};
  const adjustment = dataQuality.confidenceAdjustment || {};
  const reconciliation = dataQuality.reconciliation || {};
  summaryEl.innerHTML = `
    <div class="quality-metric">
      <div class="quality-metric-label">Extraction Confidence</div>
      <div class="quality-metric-value">${extraction.averagePct ?? 0}%</div>
      <div class="quality-metric-note">${extraction.documentCount || 0} normalized document${extraction.documentCount === 1 ? '' : 's'} in scoring</div>
    </div>
    <div class="quality-metric">
      <div class="quality-metric-label">Validation Status</div>
      <div class="quality-metric-value">${escapeHtml(formatValidationStatus(dataQuality.validationStatus))}</div>
      <div class="quality-metric-note">${escapeHtml(dataQuality.summary || 'No validation summary available.')}</div>
    </div>
    <div class="quality-metric">
      <div class="quality-metric-label">Confidence Adjustment</div>
      <div class="quality-metric-value">${escapeHtml(formatConfidenceAdjustment(adjustment.delta))}</div>
      <div class="quality-metric-note">${escapeHtml(formatAdjustmentNote(adjustment))}</div>
    </div>
    <div class="quality-metric">
      <div class="quality-metric-label">Consistency Score</div>
      <div class="quality-metric-value">${reconciliation.consistencyScore ?? 'N/A'}</div>
      <div class="quality-metric-note">${escapeHtml(reconciliation.summary || 'No cross-document reconciliation findings.')}</div>
    </div>
  `;

  docsEl.innerHTML = (dataQuality.documents || []).length > 0
    ? dataQuality.documents.map((doc) => `
      <button class="quality-document ${state.activeReviewDocType === doc.docType ? 'active' : ''}" type="button" data-review-doc="${escapeAttr(doc.docType)}">
        <div>
          <h4>${escapeHtml(doc.label)}</h4>
          <div class="quality-document-meta">${escapeHtml(doc.source)}${doc.warningCount ? ` • ${doc.warningCount} extraction warning${doc.warningCount === 1 ? '' : 's'}` : ''}</div>
          <div class="quality-document-trace">
            <span>${doc.interpretability?.mappedCount || 0} mapped</span>
            <span>${doc.interpretability?.derivedCount || 0} derived</span>
            <span>${doc.interpretability?.ambiguousCount || 0} ambiguous</span>
            <span>${doc.interpretability?.unmappedCount || 0} unmapped</span>
            <span>${doc.interpretability?.lowConfidenceCount || 0} low-confidence</span>
          </div>
          ${renderProvenancePreview(doc.provenancePreview)}
        </div>
        <div class="quality-doc-badges">
          <span class="quality-badge ${safeClass(doc.confidenceLabel, SAFE_CONFIDENCE_CLASSES, 'low')}">${doc.confidencePct}% confidence</span>
          <span class="quality-badge ${doc.source === 'modeled fallback' ? 'low' : 'validated'}">${escapeHtml(doc.source)}</span>
        </div>
      </button>
    `).join('')
    : '<div class="empty-state">No normalized documents were available for scoring</div>';

  docsEl.querySelectorAll('[data-review-doc]').forEach((button) => {
    button.addEventListener('click', () => {
      state.activeReviewDocType = button.dataset.reviewDoc;
      renderDataQuality(dataQuality);
    });
  });

  const findings = [
    ...(dataQuality.hardErrors || []).slice(0, 3),
    ...(dataQuality.validationWarnings || []).slice(0, 4),
    ...((dataQuality.reconciliation?.findings || []).filter((finding) => finding.severity === 'note').slice(0, 2)),
  ];

  findingsEl.innerHTML = `
    <div class="quality-section-label">Validation Findings</div>
    ${findings.length > 0
      ? findings.map((finding) => `
        <div class="quality-finding ${safeClass(finding.severity, SAFE_STATUS_CLASSES, 'warning')}">
          <span class="quality-finding-tag">${escapeHtml(finding.severity === 'hard_error' ? 'hard error' : finding.severity === 'note' ? 'note' : 'warning')}</span>
          <span>${escapeHtml(finding.message)}</span>
        </div>
      `).join('')
      : '<div class="empty-state">Validation did not surface material issues</div>'}
  `;

  const notes = (dataQuality.missingDataNotes || []).slice(0, 5);
  const ambiguity = (dataQuality.ambiguityHighlights || []).slice(0, 3);
  missingEl.innerHTML = `
    <div class="quality-section-label">Missing-Data Notes</div>
    ${notes.length > 0
      ? notes.map((note) => `
        <div class="quality-finding-note">
          <span class="quality-finding-tag">${escapeHtml(note.impact || 'note')}</span>
          <span>${escapeHtml(note.message)}</span>
        </div>
      `).join('')
      : '<div class="empty-state">Normalized uploads covered the core validation checks</div>'}
    <div class="quality-section-label">Ambiguity Highlights</div>
    ${ambiguity.length > 0
      ? ambiguity.map((item) => `
        <div class="quality-finding-note">
          <span class="quality-finding-tag">review</span>
          <span>${escapeHtml(formatAmbiguityHighlight(item))}</span>
        </div>
      `).join('')
      : '<div class="empty-state">No material row-mapping ambiguity surfaced</div>'}
  `;

  renderProvenanceReviewPanel(dataQuality);
  renderDataQualityDocSelection(dataQuality);
  renderAssumptionPanels(dataQuality);
  renderEvidencePanels(dataQuality);
}

function renderGrowthOpportunities(growth) {
  const summary = $('#growth-summary');
  const priorityList = $('#growth-priority-list');
  const categoryGrid = $('#growth-category-grid');
  if (!summary || !priorityList || !categoryGrid) return;

  if (!growth) {
    summary.innerHTML = '<div class="empty-state">No growth opportunities available</div>';
    priorityList.innerHTML = '';
    categoryGrid.innerHTML = '';
    return;
  }

  const categories = Array.isArray(growth.categories) && growth.categories.some((category) => category.items?.length)
    ? growth.categories
    : buildGrowthCategoriesFromTop(growth.topOpportunities || []);
  const quickWins = Array.isArray(growth.quickWins) && growth.quickWins.length
    ? growth.quickWins
    : (growth.topOpportunities || []).filter((item) => item.executionWindow === 'quick win');
  const postCloseInitiatives = Array.isArray(growth.postCloseInitiatives) && growth.postCloseInitiatives.length
    ? growth.postCloseInitiatives
    : (growth.topOpportunities || []).filter((item) => item.executionWindow === 'post-close initiative');

  summary.innerHTML = `
    <div class="growth-summary-inner">
      <div>
        <div class="growth-overline">Value-creation view</div>
        <h3>Buy-side growth underwriting</h3>
      </div>
      <p>${escapeHtml(growth.summary)}</p>
      <div class="growth-split-row">
        <div class="growth-split-card">
          <div class="growth-split-label">Quick wins</div>
          <div class="growth-split-value">${quickWins.length}</div>
        </div>
        <div class="growth-split-card">
          <div class="growth-split-label">Post-close initiatives</div>
          <div class="growth-split-value">${postCloseInitiatives.length}</div>
        </div>
      </div>
    </div>
  `;

  priorityList.innerHTML = (growth.topOpportunities || []).length > 0
    ? (growth.topOpportunities || []).map((item, index) => `
      <div class="growth-priority-card">
        <div class="growth-priority-top">
          <span class="growth-rank">P${index + 1}</span>
          <span class="growth-category-tag">${escapeHtml(item.category)}</span>
          <span class="advice-confidence ${safeClass(item.confidence, SAFE_CONFIDENCE_CLASSES, 'low')}">${escapeHtml(item.confidence)} confidence</span>
        </div>
        <h4>${escapeHtml(item.title)}</h4>
        <p>${escapeHtml(item.whyItExists)}</p>
        <div class="growth-impact-row">
          <span class="growth-window ${toGrowthWindowClass(item.executionWindow)}">${escapeHtml(item.executionWindow || 'timing not specified')}</span>
        </div>
        <div class="growth-impact-row">
          <span class="growth-impact-label">Estimated EBITDA impact</span>
          <strong>${escapeHtml(item.estimatedImpact)}</strong>
        </div>
        ${renderGrowthDetailBlocks(item)}
      </div>
    `).join('')
    : '<div class="empty-state">No data-backed growth opportunities identified</div>';

  categoryGrid.innerHTML = categories.map((category) => `
    <div class="growth-category-card">
      <div class="growth-category-title">${escapeHtml(category.name)}</div>
      ${category.items?.length
        ? category.items.map((item) => `
          <div class="growth-item">
            <div class="growth-item-top">
              <h4>${escapeHtml(item.title)}</h4>
              <span class="advice-confidence ${safeClass(item.confidence, SAFE_CONFIDENCE_CLASSES, 'low')}">${escapeHtml(item.confidence)} confidence</span>
            </div>
            <p class="growth-item-why">${escapeHtml(item.whyItExists)}</p>
            <div class="growth-metrics">
              ${(item.supportingMetrics || []).map((metric) => `<span>${escapeHtml(metric)}</span>`).join('')}
            </div>
            <div class="growth-item-bottom">
              <span class="growth-window ${toGrowthWindowClass(item.executionWindow)}">${escapeHtml(item.executionWindow || 'timing not specified')}</span>
            </div>
            <div class="growth-item-bottom">
              <span class="growth-impact-label">Estimated EBITDA impact</span>
              <strong>${escapeHtml(item.estimatedImpact)}</strong>
            </div>
            ${renderGrowthDetailBlocks(item)}
          </div>
        `).join('')
        : '<div class="empty-state">No credible opportunity identified from current data</div>'
      }
    </div>
  `).join('');
}

function formatValidationStatus(status) {
  const labels = {
    validated: 'Validated',
    partial: 'Partial',
    review: 'Review',
    'hard-error': 'Escalated',
  };
  return labels[status] || 'Unknown';
}

function formatConfidenceAdjustment(delta) {
  if (typeof delta !== 'number') return '0%';
  const pct = Math.round(delta * 100);
  return `${pct > 0 ? '+' : ''}${pct}%`;
}

function formatAdjustmentNote(adjustment = {}) {
  if (!adjustment || !Array.isArray(adjustment.reasons) || adjustment.reasons.length === 0) {
    return 'No confidence downgrade applied.';
  }
  const label = adjustment.magnitude && adjustment.magnitude !== 'none'
    ? `${adjustment.magnitude} downgrade`
    : 'confidence impact';
  return `${label} driven by validation and missing-data checks.`;
}

function renderProvenancePreview(preview) {
  if (!preview) return '';

  const mapped = (preview.mappedExamples || []).slice(0, 2)
    .map((entry) => `${entry.rowLabel} -> ${entry.fieldName}${entry.period ? ` (${entry.period})` : ''}`)
    .join(' • ');
  const derived = (preview.derivedFields || []).slice(0, 2)
    .map((entry) => `${entry.fieldName}${entry.period ? ` (${entry.period})` : ''}`)
    .join(' • ');
  const heuristic = (preview.lowConfidenceRows || []).slice(0, 2)
    .map((entry) => `${entry.rowLabel} -> ${entry.fieldName}`)
    .join(' • ');

  return `
    <div class="quality-document-preview">
      ${mapped ? `<div><strong>Trace:</strong> ${escapeHtml(mapped)}</div>` : ''}
      ${derived ? `<div><strong>Derived:</strong> ${escapeHtml(derived)}</div>` : ''}
      ${heuristic ? `<div><strong>Heuristic:</strong> ${escapeHtml(heuristic)}</div>` : ''}
    </div>
  `;
}

function formatAmbiguityHighlight(item) {
  const ambiguous = (item.ambiguousRows || []).map((row) => row.rowLabel).filter(Boolean);
  const unmapped = (item.unmappedRows || []).map((row) => row.rowLabel).filter(Boolean);
  const heuristic = (item.lowConfidenceRows || []).map((row) => row.rowLabel).filter(Boolean);

  if (ambiguous.length > 0 && unmapped.length > 0) {
    return `${item.label}: ambiguous rows ${ambiguous.join(', ')}; unmapped rows ${unmapped.join(', ')}.`;
  }
  if (ambiguous.length > 0) {
    return `${item.label}: ambiguous rows ${ambiguous.join(', ')} require review.`;
  }
  if (heuristic.length > 0 && unmapped.length > 0) {
    return `${item.label}: heuristic rows ${heuristic.join(', ')} were mapped weakly; unmapped rows ${unmapped.join(', ')} were excluded.`;
  }
  if (heuristic.length > 0) {
    return `${item.label}: heuristic rows ${heuristic.join(', ')} should be spot-checked.`;
  }
  return `${item.label}: unmapped rows ${unmapped.join(', ')} were excluded from normalization.`;
}

function renderProvenanceReviewPanel(dataQuality) {
  const tabsEl = $('#quality-review-tabs');
  const summaryEl = $('#quality-review-summary');
  const mappedEl = $('#quality-review-mapped');
  const derivedEl = $('#quality-review-derived');
  const issuesEl = $('#quality-review-issues');

  if (!tabsEl || !summaryEl || !mappedEl || !derivedEl || !issuesEl) return;

  const documents = dataQuality?.documents || [];
  if (documents.length === 0) {
    tabsEl.innerHTML = '';
    summaryEl.innerHTML = '<div class="empty-state">No normalized documents available for provenance review</div>';
    mappedEl.innerHTML = '';
    derivedEl.innerHTML = '';
    issuesEl.innerHTML = '';
    return;
  }

  const preferredDoc = documents.find((doc) => doc.interpretability?.needsReview)
    || documents[0];
  const activeDoc = documents.find((doc) => doc.docType === state.activeReviewDocType)
    || preferredDoc;

  state.activeReviewDocType = activeDoc.docType;

  tabsEl.innerHTML = documents.map((doc) => `
    <button
      class="quality-review-tab ${doc.docType === activeDoc.docType ? 'active' : ''}"
      type="button"
      data-review-tab="${escapeAttr(doc.docType)}">
      <span>${escapeHtml(doc.label)}</span>
      <span class="quality-review-tab-meta">${doc.confidencePct}%</span>
    </button>
  `).join('');

  tabsEl.querySelectorAll('[data-review-tab]').forEach((button) => {
    button.addEventListener('click', () => {
      state.activeReviewDocType = button.dataset.reviewTab;
      renderProvenanceReviewPanel(dataQuality);
      renderDataQualityDocSelection(dataQuality);
    });
  });

  const review = activeDoc.reviewPacket || {};
  const fieldOptions = getReviewOverrideFieldOptions(activeDoc.docType);
  summaryEl.innerHTML = `
    <div class="quality-review-stat">
      <div class="quality-review-stat-label">Reviewing</div>
      <div class="quality-review-stat-value">${escapeHtml(activeDoc.label)}</div>
      <div class="quality-review-stat-note">${escapeHtml(activeDoc.source)} • ${activeDoc.confidencePct}% confidence</div>
    </div>
    <div class="quality-review-stat">
      <div class="quality-review-stat-label">Missing Fields</div>
      <div class="quality-review-stat-value">${(review.missingFields || []).length}</div>
      <div class="quality-review-stat-note">${escapeHtml((review.missingFields || []).slice(0, 3).join(', ') || 'No material missing fields')}</div>
    </div>
    <div class="quality-review-stat">
      <div class="quality-review-stat-label">Review Queue</div>
      <div class="quality-review-stat-value">${(review.ambiguousRows || []).length + (review.unmappedRows || []).length + (review.lowConfidenceRows || []).length}</div>
      <div class="quality-review-stat-note">${activeDoc.interpretability?.needsReview ? 'Ambiguity, excluded rows, or heuristic mappings surfaced' : 'No row-level review needed'}</div>
    </div>
    <div class="quality-review-stat">
      <div class="quality-review-stat-label">Confidence Stack</div>
      <div class="quality-review-stat-value">${review.confidenceDecomposition?.totalPct ?? activeDoc.confidencePct}%</div>
      <div class="quality-review-stat-note">${(review.confidenceDecomposition?.factors || []).slice(0, 2).map((factor) => `${factor.label}: ${Math.round((factor.score || 0) * 100)}%`).join(' • ') || 'No decomposition available'}</div>
    </div>
  `;

  mappedEl.innerHTML = (review.mappedRows || []).length > 0
    ? (review.mappedRows || []).map((entry) => `
      <div class="quality-review-item">
        <div class="quality-review-item-top">
          <strong>${escapeHtml(humanizeFieldName(entry.fieldName))}</strong>
          <span>${escapeHtml(entry.period && entry.period !== '_single' ? entry.period : 'point-in-time')}</span>
        </div>
        <div class="quality-review-item-body">${escapeHtml(entry.rowLabel)}</div>
        <div class="quality-review-item-meta">${escapeHtml(`${entry.matchAlias ? `matched "${entry.matchAlias}"` : 'direct structural mapping'}${entry.matchType ? ` • ${entry.matchType}` : ''}${typeof entry.matchScore === 'number' ? ` • ${Math.round(entry.matchScore * 100)}% score` : ''}`)}</div>
      </div>
    `).join('')
    : '<div class="empty-state">No mapped field traces were retained for this document</div>';

  derivedEl.innerHTML = `
    ${(review.derivedFields || []).length > 0
      ? (review.derivedFields || []).map((entry) => `
        <div class="quality-review-item">
          <div class="quality-review-item-top">
            <strong>${escapeHtml(humanizeFieldName(entry.fieldName))}</strong>
            <span>${escapeHtml(entry.period && entry.period !== '_single' ? entry.period : 'derived')}</span>
          </div>
          <div class="quality-review-item-body">${escapeHtml(entry.note)}</div>
        </div>
      `).join('')
      : '<div class="empty-state">No derived assumptions were required</div>'}
    ${(review.warnings || []).length > 0
      ? `
        <div class="quality-review-subsection">
          <div class="quality-section-label">Extraction Warnings</div>
          ${(review.warnings || []).map((warning) => `
            <div class="quality-finding-note">
              <span class="quality-finding-tag">warning</span>
              <span>${escapeHtml(warning)}</span>
            </div>
          `).join('')}
        </div>
      `
      : ''}
    ${(review.recommendations || []).length > 0
      ? `
        <div class="quality-review-subsection">
          <div class="quality-section-label">Suggested actions</div>
          ${(review.recommendations || []).map((note) => `
            <div class="quality-finding-note">
              <span class="quality-finding-tag">next</span>
              <span>${escapeHtml(note)}</span>
            </div>
          `).join('')}
        </div>
      `
      : ''}
    ${(review.confidenceDecomposition?.factors || []).length > 0
      ? `
        <div class="quality-review-subsection">
          <div class="quality-section-label">Confidence breakdown</div>
          ${(review.confidenceDecomposition.factors || []).map((factor) => `
            <div class="quality-finding-note">
              <span class="quality-finding-tag">${Math.round((factor.score || 0) * 100)}%</span>
              <span>${escapeHtml(factor.label)}: ${escapeHtml(factor.note)}</span>
            </div>
          `).join('')}
          ${(review.confidenceDecomposition.penalties || []).map((penalty) => `
            <div class="quality-finding-note">
              <span class="quality-finding-tag">penalty</span>
              <span>${escapeHtml(penalty.note)}</span>
            </div>
          `).join('')}
        </div>
      `
      : ''}
  `;

  const issueBlocks = [];
  if ((review.ambiguousRows || []).length > 0) {
    issueBlocks.push((review.ambiguousRows || []).map((entry) => `
      <div class="quality-review-item review">
        <div class="quality-review-item-top">
          <strong>${escapeHtml(entry.rowLabel)}</strong>
          <span>ambiguous</span>
        </div>
        <div class="quality-review-item-body">${escapeHtml((entry.candidates || []).map((candidate) => humanizeFieldName(candidate.fieldName)).join(' or '))}</div>
        <div class="quality-review-item-meta">${escapeHtml(entry.suggestedAction || '')}</div>
        ${renderOverrideControls(activeDoc, entry, fieldOptions)}
      </div>
    `).join(''));
  }
  if ((review.lowConfidenceRows || []).length > 0) {
    issueBlocks.push((review.lowConfidenceRows || []).map((entry) => `
      <div class="quality-review-item review">
        <div class="quality-review-item-top">
          <strong>${escapeHtml(entry.rowLabel)}</strong>
          <span>heuristic</span>
        </div>
        <div class="quality-review-item-body">${escapeHtml(`Mapped to ${humanizeFieldName(entry.fieldName)} using a ${Math.round((entry.score || 0) * 100)}% ${entry.matchType || 'heuristic'} match against "${entry.alias || entry.fieldName}".`)}</div>
        <div class="quality-review-item-meta">${escapeHtml(entry.suggestedAction || '')}</div>
        ${renderOverrideControls(activeDoc, entry, fieldOptions)}
      </div>
    `).join(''));
  }
  if ((review.unmappedRows || []).length > 0) {
    issueBlocks.push((review.unmappedRows || []).map((entry) => `
      <div class="quality-review-item review">
        <div class="quality-review-item-top">
          <strong>${escapeHtml(entry.rowLabel)}</strong>
          <span>unmapped</span>
        </div>
        <div class="quality-review-item-body">Excluded from normalization pending clearer schema mapping.</div>
        <div class="quality-review-item-meta">${escapeHtml(entry.suggestedAction || '')}</div>
        ${renderOverrideControls(activeDoc, entry, fieldOptions)}
      </div>
    `).join(''));
  }
  if ((review.missingFields || []).length > 0) {
    issueBlocks.push(`
      <div class="quality-review-subsection">
        <div class="quality-section-label">Fields still missing</div>
        <div class="quality-review-chip-row">
          ${(review.missingFields || []).map((field) => `<span class="quality-review-chip">${escapeHtml(humanizeFieldName(field))}</span>`).join('')}
        </div>
      </div>
    `);
  }

  issuesEl.innerHTML = issueBlocks.length > 0
    ? issueBlocks.join('')
    : '<div class="empty-state">This document did not surface row-level review issues</div>';

  bindOverrideControls(activeDoc);
}

function renderDataQualityDocSelection(dataQuality) {
  const docsEl = $('#quality-documents');
  if (!docsEl) return;
  docsEl.querySelectorAll('[data-review-doc]').forEach((button) => {
    button.classList.toggle('active', button.dataset.reviewDoc === state.activeReviewDocType);
  });
}

function renderAssumptionPanels(dataQuality) {
  const ledgerEl = $('#quality-assumption-ledger');
  const planEl = $('#quality-confidence-plan');
  if (!ledgerEl || !planEl) return;

  if (!dataQuality) {
    ledgerEl.innerHTML = '';
    planEl.innerHTML = '';
    return;
  }

  const ledger = dataQuality.assumptionLedger || [];
  ledgerEl.innerHTML = ledger.length > 0
    ? ledger.map((entry) => `
      <div class="quality-review-item ${entry.severity === 'medium' || entry.severity === 'high' ? 'review' : ''}">
        <div class="quality-review-item-top">
          <strong>${escapeHtml(entry.title)}</strong>
          <span>${escapeHtml(entry.confidenceImpact || entry.severity || 'note')} impact</span>
        </div>
        <div class="quality-review-item-body">${escapeHtml(entry.detail)}</div>
        <div class="quality-review-item-meta">${escapeHtml(entry.category)}</div>
      </div>
    `).join('')
    : '<div class="empty-state">No material assumptions were logged from the current ingestion set</div>';

  const plan = dataQuality.confidenceRecommendations || [];
  planEl.innerHTML = plan.length > 0
    ? plan.map((entry) => `
      <div class="quality-review-item review">
        <div class="quality-review-item-top">
          <strong>${escapeHtml(entry.title)}</strong>
          <span>${escapeHtml(entry.priority)}</span>
        </div>
        <div class="quality-review-item-body">${escapeHtml(entry.action)}</div>
        <div class="quality-review-item-meta">${escapeHtml(`${entry.rationale} • expected lift: ${entry.expectedLift}`)}</div>
      </div>
    `).join('')
    : '<div class="empty-state">Confidence is already strong relative to the current document set</div>';
}

function renderEvidencePanels(dataQuality) {
  const resolutionEl = $('#quality-evidence-resolution');
  const temporalEl = $('#quality-temporal-alignment');
  const entityEl = $('#quality-entity-resolution');
  const workflowEl = $('#quality-ambiguity-workflows');
  const reviewerEl = $('#quality-reviewer-memory');
  if (!resolutionEl || !temporalEl || !entityEl || !workflowEl || !reviewerEl) return;

  if (!dataQuality) {
    resolutionEl.innerHTML = '';
    temporalEl.innerHTML = '';
    entityEl.innerHTML = '';
    workflowEl.innerHTML = '';
    reviewerEl.innerHTML = '';
    return;
  }

  const resolvedFields = dataQuality.evidenceResolution?.resolvedFields || [];
  resolutionEl.innerHTML = resolvedFields.length > 0
    ? resolvedFields.map((field) => `
      <div
        class="quality-review-item ${field.competingCandidates?.length ? 'review' : ''}"
        data-evidence-field="${escapeAttr(field.key || '')}"
        data-evidence-selected-doc-type="${escapeAttr(field.selected?.docType || '')}">
        <div class="quality-review-item-top">
          <strong>${escapeHtml(field.label)}</strong>
          <span>${field.confidencePct}% confidence</span>
        </div>
        <div class="quality-review-item-body">${escapeHtml(field.resolutionSummary)}</div>
        <div class="quality-review-item-meta">${escapeHtml(field.selected?.decomposition?.factors?.slice(0, 3).map((factor) => `${factor.label}: ${Math.round((factor.score || 0) * 100)}%`).join(' • ') || '')}</div>
        ${(field.competingCandidates || []).slice(0, 2).map((candidate) => `
          <div class="quality-finding-note">
            <span class="quality-finding-tag">alt</span>
            <span>${escapeHtml(`${candidate.docLabel}${candidate.periodKey ? ` (${candidate.periodKey})` : ''}: ${formatEvidenceValue(candidate.value, field.format)} at ${candidate.confidencePct}% confidence`)}</span>
            ${renderEvidenceCandidateActionControls(field, candidate)}
          </div>
        `).join('')}
        ${renderSourcePreferenceControls(field)}
      </div>
    `).join('')
    : '<div class="empty-state">No ranked evidence fields were available from the current upload set</div>';

  const timelines = dataQuality.temporalAlignment?.documents || [];
  const timelineConflicts = dataQuality.temporalAlignment?.conflicts || [];
  temporalEl.innerHTML = timelines.length > 0
    ? `
      ${timelines.map((timeline) => `
        <div class="quality-review-item" data-temporal-source="${escapeAttr(timeline.sourceRefKey || '')}">
          <div class="quality-review-item-top">
            <strong>${escapeHtml(timeline.label)}</strong>
            <span>${escapeHtml(timeline.alignmentLabel)}</span>
          </div>
          <div class="quality-review-item-body">${escapeHtml(timeline.summary)}</div>
          ${renderTimeBasisControls(timeline)}
        </div>
      `).join('')}
      ${timelineConflicts.slice(0, 4).map((conflict) => `
        <div class="quality-review-item review">
          <div class="quality-review-item-top">
            <strong>${escapeHtml(conflict.label)}</strong>
            <span>${escapeHtml(conflict.severity)}</span>
          </div>
          <div class="quality-review-item-body">${escapeHtml(conflict.summary)}</div>
          <div class="quality-review-item-meta">${escapeHtml(conflict.recommendedAction || '')}</div>
        </div>
      `).join('')}
    `
    : '<div class="empty-state">No temporal alignment model was generated from the current uploads</div>';

  const clusters = dataQuality.entityResolution?.clusters || [];
  entityEl.innerHTML = clusters.length > 0
    ? clusters.map((cluster) => `
      <div class="quality-review-item ${cluster.aliases?.length > 1 ? 'review' : ''}" data-entity-cluster="${escapeAttr(cluster.id || '')}">
        <div class="quality-review-item-top">
          <strong>${escapeHtml(cluster.canonicalName)}</strong>
          <span>${escapeHtml(cluster.kind.replace(/_/g, ' '))} • ${cluster.confidencePct}%</span>
        </div>
        <div class="quality-review-item-body">${escapeHtml(cluster.summary)}</div>
        <div class="quality-review-item-meta">${escapeHtml((cluster.aliases || []).join(', '))}</div>
        ${renderEntityResolutionControls(cluster)}
      </div>
    `).join('')
    : '<div class="empty-state">No cross-document entities were available to cluster</div>';

  const workflows = dataQuality.ambiguityWorkflows?.items || [];
  workflowEl.innerHTML = workflows.length > 0
    ? workflows.map((item) => `
      <div class="quality-review-item review">
        <div class="quality-review-item-top">
          <strong>${escapeHtml(item.title)}</strong>
          <span>${escapeHtml(item.priority)}</span>
        </div>
        <div class="quality-review-item-body">${escapeHtml(item.detail)}</div>
        <div class="quality-review-item-meta">${escapeHtml(item.recommendedAction)}</div>
      </div>
    `).join('')
    : '<div class="empty-state">No ambiguity-specific workflows are open for the current upload set</div>';

  const reviewerSignals = dataQuality.reviewerSignals || null;
  const reviewerDocTypes = reviewerSignals?.docTypes || [];
  const noisyLabels = reviewerSignals?.noisyLabels || [];
  const fieldSignals = reviewerSignals?.fields || [];
  const sourcePreferences = reviewerSignals?.sourcePreferences || [];
  const conceptSuppressions = reviewerSignals?.conceptSuppressions || [];
  const timeBasisOverrides = reviewerSignals?.timeBasisOverrides || [];
  const entityResolutions = reviewerSignals?.entityResolutions || [];
  const recentHistory = getEffectiveReviewMemory().recentHistory || [];
  reviewerEl.innerHTML = reviewerSignals && (
    (reviewerSignals.ruleCount || 0) > 0
    || (reviewerSignals.sourcePreferenceCount || 0) > 0
    || (reviewerSignals.conceptSuppressionCount || 0) > 0
    || (reviewerSignals.timeBasisOverrideCount || 0) > 0
    || (reviewerSignals.entityResolutionCount || 0) > 0
  )
    ? `
      <div class="quality-review-item">
        <div class="quality-review-item-top">
          <strong>Reviewer ranking memory</strong>
          <span>${(reviewerSignals.ruleCount || 0) + (reviewerSignals.explicitActionCount || 0)} saved decision${((reviewerSignals.ruleCount || 0) + (reviewerSignals.explicitActionCount || 0)) === 1 ? '' : 's'}</span>
        </div>
        <div class="quality-review-item-body">${escapeHtml(reviewerSignals.summary || '')}</div>
        <div class="quality-review-item-meta">${escapeHtml(reviewerDocTypes.slice(0, 2).map((entry) => `${humanizeFieldName(entry.docType)}: ${entry.trustPct}% trust`).join(' • ') || 'No doc-family adjustments')}</div>
      </div>
      ${reviewerDocTypes.slice(0, 4).map((entry) => `
        <div class="quality-review-item ${entry.ignoreCount > entry.mapCount ? 'review' : ''}">
          <div class="quality-review-item-top">
            <strong>${escapeHtml(humanizeFieldName(entry.docType))}</strong>
            <span>${entry.trustPct}% trust</span>
          </div>
          <div class="quality-review-item-body">${escapeHtml(entry.summary)}</div>
          <div class="quality-review-item-meta">${escapeHtml(`adjustment ${entry.trustAdjustment >= 0 ? '+' : ''}${Math.round(entry.trustAdjustment * 100)} pts • noise ratio ${Math.round((entry.noiseRatio || 0) * 100)}%`)}</div>
        </div>
      `).join('')}
      ${fieldSignals.slice(0, 3).map((entry) => `
        <div class="quality-review-item">
          <div class="quality-review-item-top">
            <strong>${escapeHtml(`${humanizeFieldName(entry.docType)} → ${humanizeFieldName(entry.fieldName)}`)}</strong>
            <span>+${Math.round((entry.confidenceBoost || 0) * 100)} pts</span>
          </div>
          <div class="quality-review-item-body">${escapeHtml(`Confirmed by ${entry.mapCount} reviewer mapping${entry.mapCount === 1 ? '' : 's'} from ${entry.sourceLabels.join(', ') || 'saved labels'}.`)}</div>
        </div>
      `).join('')}
      ${sourcePreferences.slice(0, 3).map((entry) => `
        <div
          class="quality-review-item"
          data-reviewer-source-preference="${escapeAttr(entry.conceptKey || '')}"
          data-preferred-doc-type="${escapeAttr(entry.preferredDocType || '')}">
          <div class="quality-review-item-top">
            <strong>${escapeHtml(humanizeFieldName(entry.conceptKey))}</strong>
            <span>${escapeHtml(humanizeFieldName(entry.preferredDocType))}</span>
          </div>
          <div class="quality-review-item-body">${escapeHtml(`Reviewer memory is explicitly preferring ${humanizeFieldName(entry.preferredDocType)} when resolving ${humanizeFieldName(entry.conceptKey)} conflicts.`)}</div>
        </div>
      `).join('')}
      ${conceptSuppressions.slice(0, 3).map((entry) => `
        <div class="quality-review-item review">
          <div class="quality-review-item-top">
            <strong>${escapeHtml(humanizeFieldName(entry.conceptKey))}</strong>
            <span>${escapeHtml(humanizeFieldName(entry.docType))}</span>
          </div>
          <div class="quality-review-item-body">${escapeHtml(`Reviewer memory is suppressing ${humanizeFieldName(entry.docType)} for ${humanizeFieldName(entry.conceptKey)}${entry.sourceRefLabel ? ` (${entry.sourceRefLabel})` : ''}.`)}</div>
        </div>
      `).join('')}
      ${timeBasisOverrides.slice(0, 3).map((entry) => `
        <div class="quality-review-item">
          <div class="quality-review-item-top">
            <strong>${escapeHtml(humanizeFieldName(entry.docType))}</strong>
            <span>${escapeHtml(entry.basis.replace(/_/g, ' '))}</span>
          </div>
          <div class="quality-review-item-body">${escapeHtml(`Reviewer memory is forcing ${entry.sourceRefLabel || entry.sourceRefKey} to be interpreted as ${entry.basis.replace(/_/g, ' ')} evidence.`)}</div>
        </div>
      `).join('')}
      ${entityResolutions.slice(0, 3).map((entry) => `
        <div class="quality-review-item">
          <div class="quality-review-item-top">
            <strong>${escapeHtml(entry.canonicalName)}</strong>
            <span>${escapeHtml(entry.kind.replace(/_/g, ' '))}</span>
          </div>
          <div class="quality-review-item-body">${escapeHtml(`Reviewer memory confirmed ${entry.aliases.join(', ')} as one entity cluster.`)}</div>
        </div>
      `).join('')}
      ${noisyLabels.slice(0, 3).map((entry) => `
        <div class="quality-review-item review">
          <div class="quality-review-item-top">
            <strong>${escapeHtml(entry.rowLabel)}</strong>
            <span>${entry.noisePct}% noise</span>
          </div>
          <div class="quality-review-item-body">${escapeHtml(`Reviewer history usually ignores this ${humanizeFieldName(entry.docType)} label, so it is de-prioritized during ranking unless explicitly remapped.`)}</div>
          <div class="quality-review-item-meta">${escapeHtml((entry.mappedFields || []).length > 0 ? `mapped exceptions: ${entry.mappedFields.map((field) => humanizeFieldName(field)).join(', ')}` : 'No confirmed mapped exceptions')}</div>
        </div>
      `).join('')}
      ${recentHistory.slice(0, 3).map((entry) => `
        <div class="quality-review-item">
          <div class="quality-review-item-top">
            <strong>Revision ${escapeHtml(entry.revision)}</strong>
            <span>${escapeHtml(entry.updatedBy || 'reviewer')}</span>
          </div>
          <div class="quality-review-item-body">${escapeHtml(entry.summary || '')}</div>
          <div class="quality-review-item-meta">${escapeHtml(entry.updatedAt || '')}</div>
        </div>
      `).join('')}
    `
    : '<div class="empty-state">No reviewer memory yet. Use the review panel to map or ignore ambiguous rows and the evidence ranker will start learning from those decisions.</div>';

  bindSourcePreferenceControls();
  bindEvidenceCandidateControls();
  bindTimeBasisControls();
  bindEntityResolutionControls();
}

function humanizeFieldName(value = '') {
  return String(value)
    .replace(/^__/, '')
    .replace(/([a-z])([A-Z])/g, '$1 $2')
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function formatEvidenceValue(value, format = 'currency') {
  const numeric = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(numeric)) return 'N/A';
  if (format === 'percentage') return `${Math.round(numeric * 10) / 10}%`;
  return `$${Math.round(numeric).toLocaleString()}`;
}

function applyReviewMemoryUpdate(updates = {}) {
  const reviewMemory = getEffectiveReviewMemory();
  const scope = buildReviewScope();
  state.reviewMemory = {
    ...reviewMemory,
    ...buildReviewMemoryBundle({
      reviewOverrides: updates.reviewOverrides ?? reviewMemory.reviewOverrides,
      sourcePreferences: updates.sourcePreferences ?? reviewMemory.sourcePreferences,
      conceptSuppressions: updates.conceptSuppressions ?? reviewMemory.conceptSuppressions,
      timeBasisOverrides: updates.timeBasisOverrides ?? reviewMemory.timeBasisOverrides,
      entityResolutions: updates.entityResolutions ?? reviewMemory.entityResolutions,
    }),
    companyName: scope.companyName,
    dealName: scope.dealName,
    reviewerId: scope.reviewerId,
    revision: reviewMemory.revision || 0,
    recentHistory: reviewMemory.recentHistory || [],
  };
  return state.reviewMemory;
}

function renderEvidenceCandidateActionControls(field, candidate) {
  const reviewMemory = getEffectiveReviewMemory();
  const suppression = getStoredConceptSuppression({
    conceptKey: field.key,
    docType: candidate.docType,
    sourceRefKey: candidate.sourceRefKey || '',
  }, reviewMemory.conceptSuppressions);

  return `
    <div class="quality-inline-actions">
      <button
        class="quality-override-btn subtle"
        type="button"
        data-evidence-trust="${escapeAttr(field.key)}"
        data-doc-type="${escapeAttr(candidate.docType || '')}">
        Trust source
      </button>
      <button
        class="quality-override-btn subtle"
        type="button"
        data-evidence-suppress="${escapeAttr(field.key)}"
        data-doc-type="${escapeAttr(candidate.docType || '')}"
        data-source-ref-key="${escapeAttr(candidate.sourceRefKey || '')}"
        data-source-ref-label="${escapeAttr(candidate.sourceRef || '')}">
        ${suppression ? 'Suppressed' : 'Suppress'}
      </button>
      ${suppression
        ? `
          <button
            class="quality-override-btn subtle"
            type="button"
            data-evidence-unsuppress="${escapeAttr(field.key)}"
            data-doc-type="${escapeAttr(candidate.docType || '')}"
            data-source-ref-key="${escapeAttr(candidate.sourceRefKey || '')}">
            Clear
          </button>
        `
        : ''}
    </div>
  `;
}

function renderTimeBasisControls(timeline) {
  if (!timeline?.docType || !timeline?.sourceRefKey) return '';
  const reviewMemory = getEffectiveReviewMemory();
  const existing = getStoredTimeBasisOverride({
    docType: timeline.docType,
    sourceRefKey: timeline.sourceRefKey,
    basis: timeline.basis,
  }, reviewMemory.timeBasisOverrides);
  const value = existing?.basis || timeline.basis || '';

  return `
    <div class="quality-override-controls">
      <select class="quality-override-select" data-time-basis-select="${escapeAttr(timeline.sourceRefKey)}">
        ${['historical', 'ltm', 'point_in_time', 'forecast'].map((basis) => `
          <option value="${escapeAttr(basis)}" ${value === basis ? 'selected' : ''}>${escapeHtml(humanizeFieldName(basis))}</option>
        `).join('')}
      </select>
      <button
        class="quality-override-btn"
        type="button"
        data-time-basis-save="${escapeAttr(timeline.sourceRefKey)}"
        data-doc-type="${escapeAttr(timeline.docType || '')}"
        data-source-ref-label="${escapeAttr(timeline.sourceRef || '')}">
        Save basis
      </button>
      ${existing
        ? `
          <button
            class="quality-override-btn subtle"
            type="button"
            data-time-basis-clear="${escapeAttr(timeline.sourceRefKey)}"
            data-doc-type="${escapeAttr(timeline.docType || '')}">
            Clear
          </button>
        `
        : ''}
    </div>
  `;
}

function renderEntityResolutionControls(cluster) {
  if (!cluster || !Array.isArray(cluster.aliases) || cluster.aliases.length < 2) return '';
  const reviewMemory = getEffectiveReviewMemory();
  const existing = getStoredEntityResolution({
    kind: cluster.kind,
    canonicalName: cluster.canonicalName,
    aliases: cluster.aliases,
  }, reviewMemory.entityResolutions);

  return `
    <div class="quality-override-controls">
      <select class="quality-override-select" data-entity-resolution-select="${escapeAttr(cluster.id || '')}">
        ${cluster.aliases.map((alias) => `
          <option value="${escapeAttr(alias)}" ${(existing?.canonicalName || cluster.canonicalName) === alias ? 'selected' : ''}>${escapeHtml(alias)}</option>
        `).join('')}
      </select>
      <button
        class="quality-override-btn"
        type="button"
        data-entity-resolution-save="${escapeAttr(cluster.id || '')}"
        data-entity-kind="${escapeAttr(cluster.kind || '')}"
        data-entity-aliases="${escapeAttr(JSON.stringify(cluster.aliases || []))}">
        Confirm aliases
      </button>
      ${existing
        ? `
          <button
            class="quality-override-btn subtle"
            type="button"
            data-entity-resolution-clear="${escapeAttr(cluster.id || '')}"
            data-entity-kind="${escapeAttr(cluster.kind || '')}"
            data-entity-aliases="${escapeAttr(JSON.stringify(cluster.aliases || []))}">
            Clear
          </button>
        `
        : ''}
    </div>
  `;
}

function renderOverrideControls(doc, entry, fieldOptions) {
  const rowKey = encodeURIComponent(entry.rowLabel);
  const reviewOverrides = getEffectiveReviewMemory().reviewOverrides;
  const existingMap = getStoredOverrideForRow({
    docType: doc.docType,
    sheetName: doc.sourceSheetName,
    rowLabel: entry.rowLabel,
    action: 'map',
  }, reviewOverrides);
  const existingIgnore = getStoredOverrideForRow({
    docType: doc.docType,
    sheetName: doc.sourceSheetName,
    rowLabel: entry.rowLabel,
    action: 'ignore',
  }, reviewOverrides);

  return `
    <div class="quality-override-controls">
      <select class="quality-override-select" data-override-select="${escapeAttr(rowKey)}">
        <option value="">Map to field…</option>
        ${fieldOptions.map((option) => `
          <option value="${escapeAttr(option.value)}" ${existingMap?.fieldName === option.value ? 'selected' : ''}>
            ${escapeHtml(option.label)}${option.required ? ' (core)' : ''}
          </option>
        `).join('')}
      </select>
      <button class="quality-override-btn" type="button" data-override-map="${escapeAttr(rowKey)}">Save mapping</button>
      <button class="quality-override-btn subtle" type="button" data-override-ignore="${escapeAttr(rowKey)}">${existingIgnore ? 'Ignored' : 'Ignore row'}</button>
      ${(existingMap || existingIgnore)
        ? `<button class="quality-override-btn subtle" type="button" data-override-clear="${escapeAttr(rowKey)}">Clear</button>`
        : ''}
    </div>
  `;
}

function renderSourcePreferenceControls(field) {
  if (!field || !Array.isArray(field.candidates) || field.candidates.length < 2) return '';

  const reviewMemory = getEffectiveReviewMemory();
  const existingPreference = getStoredSourcePreference({
    conceptKey: field.key,
    preferredDocType: (reviewMemory.sourcePreferences || []).find((entry) => entry.conceptKey === field.key)?.preferredDocType || '',
  }, reviewMemory.sourcePreferences);
  const candidates = dedupeSourcePreferenceCandidates(field.candidates);

  return `
    <div class="quality-override-controls">
      <select class="quality-override-select" data-source-preference-select="${escapeAttr(field.key)}">
        <option value="">Prefer source…</option>
        ${candidates.map((candidate) => `
          <option value="${escapeAttr(candidate.docType)}" ${existingPreference?.preferredDocType === candidate.docType ? 'selected' : ''}>
            ${escapeHtml(candidate.docLabel)}${candidate.periodKey ? ` (${candidate.periodKey})` : ''}
          </option>
        `).join('')}
      </select>
      <button class="quality-override-btn" type="button" data-source-preference-save="${escapeAttr(field.key)}">Save source</button>
      ${existingPreference
        ? `<button class="quality-override-btn subtle" type="button" data-source-preference-clear="${escapeAttr(field.key)}">Clear</button>`
        : ''}
    </div>
  `;
}

function dedupeSourcePreferenceCandidates(candidates = []) {
  const seen = new Set();
  return candidates.filter((candidate) => {
    if (!candidate?.docType || seen.has(candidate.docType)) return false;
    seen.add(candidate.docType);
    return true;
  });
}

function bindSourcePreferenceControls() {
  document.querySelectorAll('[data-source-preference-save]').forEach((button) => {
    button.addEventListener('click', async () => {
      const conceptKey = button.dataset.sourcePreferenceSave;
      const select = document.querySelector(`[data-source-preference-select="${cssEscape(conceptKey)}"]`);
      const preferredDocType = select?.value || '';
      if (!preferredDocType) return;

      const reviewMemory = getEffectiveReviewMemory();
      const nextSourcePreferences = saveSourcePreference({
        conceptKey,
        preferredDocType,
      }, {
        preferences: reviewMemory.sourcePreferences,
        persist: false,
      });
      state.reviewMemory = {
        ...reviewMemory,
        ...buildReviewMemoryBundle({
          reviewOverrides: reviewMemory.reviewOverrides,
          sourcePreferences: nextSourcePreferences,
          conceptSuppressions: reviewMemory.conceptSuppressions,
          timeBasisOverrides: reviewMemory.timeBasisOverrides,
          entityResolutions: reviewMemory.entityResolutions,
        }),
        companyName: state.companyName,
        dealName: buildReviewScope().dealName,
        reviewerId: buildReviewScope().reviewerId,
      };
      await persistReviewMemoryState();
      rerunAnalysisFromStoredIngestion();
    });
  });

  document.querySelectorAll('[data-source-preference-clear]').forEach((button) => {
    button.addEventListener('click', async () => {
      const conceptKey = button.dataset.sourcePreferenceClear;
      const reviewMemory = getEffectiveReviewMemory();
      const nextSourcePreferences = removeSourcePreference({
        conceptKey,
      }, {
        preferences: reviewMemory.sourcePreferences,
        persist: false,
      });
      state.reviewMemory = {
        ...reviewMemory,
        ...buildReviewMemoryBundle({
          reviewOverrides: reviewMemory.reviewOverrides,
          sourcePreferences: nextSourcePreferences,
          conceptSuppressions: reviewMemory.conceptSuppressions,
          timeBasisOverrides: reviewMemory.timeBasisOverrides,
          entityResolutions: reviewMemory.entityResolutions,
        }),
        companyName: state.companyName,
        dealName: buildReviewScope().dealName,
        reviewerId: buildReviewScope().reviewerId,
      };
      await persistReviewMemoryState();
      rerunAnalysisFromStoredIngestion();
    });
  });
}

function bindEvidenceCandidateControls() {
  document.querySelectorAll('[data-evidence-trust]').forEach((button) => {
    button.addEventListener('click', async () => {
      const conceptKey = button.dataset.evidenceTrust;
      const preferredDocType = button.dataset.docType || '';
      if (!conceptKey || !preferredDocType) return;

      const reviewMemory = getEffectiveReviewMemory();
      const nextSourcePreferences = saveSourcePreference({
        conceptKey,
        preferredDocType,
      }, {
        preferences: reviewMemory.sourcePreferences,
        persist: false,
      });
      applyReviewMemoryUpdate({ sourcePreferences: nextSourcePreferences });
      await persistReviewMemoryState();
      rerunAnalysisFromStoredIngestion();
    });
  });

  document.querySelectorAll('[data-evidence-suppress]').forEach((button) => {
    button.addEventListener('click', async () => {
      const conceptKey = button.dataset.evidenceSuppress;
      const docType = button.dataset.docType || '';
      if (!conceptKey || !docType) return;

      const reviewMemory = getEffectiveReviewMemory();
      const nextSuppressions = saveConceptSuppression({
        conceptKey,
        docType,
        sourceRefKey: button.dataset.sourceRefKey || '',
        sourceRefLabel: button.dataset.sourceRefLabel || '',
      }, {
        suppressions: reviewMemory.conceptSuppressions,
        persist: false,
      });
      applyReviewMemoryUpdate({ conceptSuppressions: nextSuppressions });
      await persistReviewMemoryState();
      rerunAnalysisFromStoredIngestion();
    });
  });

  document.querySelectorAll('[data-evidence-unsuppress]').forEach((button) => {
    button.addEventListener('click', async () => {
      const conceptKey = button.dataset.evidenceUnsuppress;
      const docType = button.dataset.docType || '';
      if (!conceptKey || !docType) return;

      const reviewMemory = getEffectiveReviewMemory();
      const nextSuppressions = removeConceptSuppression({
        conceptKey,
        docType,
        sourceRefKey: button.dataset.sourceRefKey || '',
      }, {
        suppressions: reviewMemory.conceptSuppressions,
        persist: false,
      });
      applyReviewMemoryUpdate({ conceptSuppressions: nextSuppressions });
      await persistReviewMemoryState();
      rerunAnalysisFromStoredIngestion();
    });
  });
}

function bindTimeBasisControls() {
  document.querySelectorAll('[data-time-basis-save]').forEach((button) => {
    button.addEventListener('click', async () => {
      const sourceRefKey = button.dataset.timeBasisSave;
      const docType = button.dataset.docType || '';
      const select = document.querySelector(`[data-time-basis-select="${cssEscape(sourceRefKey)}"]`);
      const basis = select?.value || '';
      if (!sourceRefKey || !docType || !basis) return;

      const reviewMemory = getEffectiveReviewMemory();
      const nextOverrides = saveTimeBasisOverride({
        docType,
        sourceRefKey,
        sourceRefLabel: button.dataset.sourceRefLabel || '',
        basis,
      }, {
        overrides: reviewMemory.timeBasisOverrides,
        persist: false,
      });
      applyReviewMemoryUpdate({ timeBasisOverrides: nextOverrides });
      await persistReviewMemoryState();
      rerunAnalysisFromStoredIngestion();
    });
  });

  document.querySelectorAll('[data-time-basis-clear]').forEach((button) => {
    button.addEventListener('click', async () => {
      const sourceRefKey = button.dataset.timeBasisClear;
      const docType = button.dataset.docType || '';
      if (!sourceRefKey || !docType) return;

      const reviewMemory = getEffectiveReviewMemory();
      const nextOverrides = removeTimeBasisOverride({
        docType,
        sourceRefKey,
        basis: 'historical',
      }, {
        overrides: reviewMemory.timeBasisOverrides,
        persist: false,
      });
      applyReviewMemoryUpdate({ timeBasisOverrides: nextOverrides });
      await persistReviewMemoryState();
      rerunAnalysisFromStoredIngestion();
    });
  });
}

function bindEntityResolutionControls() {
  document.querySelectorAll('[data-entity-resolution-save]').forEach((button) => {
    button.addEventListener('click', async () => {
      const clusterId = button.dataset.entityResolutionSave;
      const kind = button.dataset.entityKind || '';
      const aliases = safeParseJson(button.dataset.entityAliases, []);
      const select = document.querySelector(`[data-entity-resolution-select="${cssEscape(clusterId)}"]`);
      const canonicalName = select?.value || '';
      if (!clusterId || !kind || !canonicalName || aliases.length < 2) return;

      const reviewMemory = getEffectiveReviewMemory();
      const nextResolutions = saveEntityResolution({
        kind,
        canonicalName,
        aliases,
      }, {
        resolutions: reviewMemory.entityResolutions,
        persist: false,
      });
      applyReviewMemoryUpdate({ entityResolutions: nextResolutions });
      await persistReviewMemoryState();
      rerunAnalysisFromStoredIngestion();
    });
  });

  document.querySelectorAll('[data-entity-resolution-clear]').forEach((button) => {
    button.addEventListener('click', async () => {
      const kind = button.dataset.entityKind || '';
      const aliases = safeParseJson(button.dataset.entityAliases, []);
      if (!kind || aliases.length < 2) return;

      const reviewMemory = getEffectiveReviewMemory();
      const nextResolutions = removeEntityResolution({
        kind,
        aliases,
        canonicalName: aliases[0],
      }, {
        resolutions: reviewMemory.entityResolutions,
        persist: false,
      });
      applyReviewMemoryUpdate({ entityResolutions: nextResolutions });
      await persistReviewMemoryState();
      rerunAnalysisFromStoredIngestion();
    });
  });
}

function bindOverrideControls(activeDoc) {
  document.querySelectorAll('[data-override-map]').forEach((button) => {
    button.addEventListener('click', async () => {
      const rowLabel = decodeURIComponent(button.dataset.overrideMap);
      const rowKey = button.dataset.overrideMap;
      const select = document.querySelector(`[data-override-select="${cssEscape(rowKey)}"]`);
      const fieldName = select?.value || '';
      if (!fieldName) return;

      const reviewMemory = getEffectiveReviewMemory();
      const nextOverrides = saveReviewOverride({
        docType: activeDoc.docType,
        sheetName: activeDoc.sourceSheetName,
        rowLabel,
        action: 'map',
        fieldName,
      }, {
        rules: reviewMemory.reviewOverrides,
        persist: false,
      });
      state.reviewMemory = {
        ...reviewMemory,
        ...buildReviewMemoryBundle({
          reviewOverrides: nextOverrides,
          sourcePreferences: reviewMemory.sourcePreferences,
          conceptSuppressions: reviewMemory.conceptSuppressions,
          timeBasisOverrides: reviewMemory.timeBasisOverrides,
          entityResolutions: reviewMemory.entityResolutions,
        }),
        companyName: state.companyName,
        dealName: buildReviewScope().dealName,
        reviewerId: buildReviewScope().reviewerId,
      };
      await persistReviewMemoryState();
      rerunAnalysisFromStoredIngestion();
    });
  });

  document.querySelectorAll('[data-override-ignore]').forEach((button) => {
    button.addEventListener('click', async () => {
      const rowLabel = decodeURIComponent(button.dataset.overrideIgnore);
      const reviewMemory = getEffectiveReviewMemory();
      const nextOverrides = saveReviewOverride({
        docType: activeDoc.docType,
        sheetName: activeDoc.sourceSheetName,
        rowLabel,
        action: 'ignore',
      }, {
        rules: reviewMemory.reviewOverrides,
        persist: false,
      });
      state.reviewMemory = {
        ...reviewMemory,
        ...buildReviewMemoryBundle({
          reviewOverrides: nextOverrides,
          sourcePreferences: reviewMemory.sourcePreferences,
          conceptSuppressions: reviewMemory.conceptSuppressions,
          timeBasisOverrides: reviewMemory.timeBasisOverrides,
          entityResolutions: reviewMemory.entityResolutions,
        }),
        companyName: state.companyName,
        dealName: buildReviewScope().dealName,
        reviewerId: buildReviewScope().reviewerId,
      };
      await persistReviewMemoryState();
      rerunAnalysisFromStoredIngestion();
    });
  });

  document.querySelectorAll('[data-override-clear]').forEach((button) => {
    button.addEventListener('click', async () => {
      const rowLabel = decodeURIComponent(button.dataset.overrideClear);
      const reviewMemory = getEffectiveReviewMemory();
      const withoutMap = removeReviewOverride({
        docType: activeDoc.docType,
        sheetName: activeDoc.sourceSheetName,
        rowLabel,
        action: 'map',
      }, {
        rules: reviewMemory.reviewOverrides,
        persist: false,
      });
      const nextOverrides = removeReviewOverride({
        docType: activeDoc.docType,
        sheetName: activeDoc.sourceSheetName,
        rowLabel,
        action: 'ignore',
      }, {
        rules: withoutMap,
        persist: false,
      });
      state.reviewMemory = {
        ...reviewMemory,
        ...buildReviewMemoryBundle({
          reviewOverrides: nextOverrides,
          sourcePreferences: reviewMemory.sourcePreferences,
          conceptSuppressions: reviewMemory.conceptSuppressions,
          timeBasisOverrides: reviewMemory.timeBasisOverrides,
          entityResolutions: reviewMemory.entityResolutions,
        }),
        companyName: state.companyName,
        dealName: buildReviewScope().dealName,
        reviewerId: buildReviewScope().reviewerId,
      };
      await persistReviewMemoryState();
      rerunAnalysisFromStoredIngestion();
    });
  });
}

function rerunAnalysisFromStoredIngestion() {
  if (!state.lastIngestion) return;

  const companyContext = buildCompanyContext();
  const effectiveIngestion = applyReviewOverridesToIngestionResponse(state.lastIngestion, getEffectiveReviewMemory().reviewOverrides);
  const pipelineResult = runPipeline(buildPipelineFileDescriptors(effectiveIngestion), companyContext);

  state.lastDiagnostics = {
    ...(pipelineResult?.diagnostics || {}),
    backendIngestion: effectiveIngestion,
  };

  showDashboard(pipelineResult?.dashboardData);
}

function cssEscape(value) {
  if (typeof CSS !== 'undefined' && typeof CSS.escape === 'function') {
    return CSS.escape(value);
  }
  return String(value).replace(/"/g, '\\"');
}

function safeParseJson(rawValue, fallback) {
  if (!rawValue) return fallback;
  try {
    return JSON.parse(rawValue);
  } catch (_error) {
    return fallback;
  }
}

function renderGrowthDetailBlocks(item) {
  const bits = [];

  if (item.underwritingAssumptions?.length) {
    bits.push(`
      <div class="growth-detail-block">
        <div class="growth-detail-label">Underwriting assumptions</div>
        <div class="growth-detail-list">${item.underwritingAssumptions.map((entry) => `<span>${escapeHtml(entry)}</span>`).join('')}</div>
      </div>
    `);
  }

  if (item.evidenceDocuments?.length) {
    bits.push(`
      <div class="growth-detail-block">
        <div class="growth-detail-label">Evidence documents</div>
        <div class="growth-detail-list">
          ${item.evidenceDocuments.map((doc) => `<span class="${doc.provided ? 'provided' : 'missing'}">${escapeHtml(doc.label)}${doc.provided ? '' : ' (missing)'}</span>`).join('')}
        </div>
      </div>
    `);
  }

  return bits.join('');
}

function toGrowthWindowClass(windowLabel = '') {
  return windowLabel.includes('quick') ? 'quick' : 'post-close';
}

function buildGrowthCategoriesFromTop(items = []) {
  const names = [
    'Revenue Growth Opportunities',
    'Margin Expansion Opportunities',
    'Operational Improvements',
    'Strategic / Acquisition Levers',
  ];

  return names.map((name) => ({
    name,
    items: items.filter((item) => item.category === name),
  }));
}

function renderAcquisitionAdvice(advice) {
  const overview = $('#advice-overview');
  const critical = $('#advice-critical');
  const important = $('#advice-important');
  if (!overview || !critical || !important) return;

  if (!advice) {
    overview.innerHTML = '<div class="empty-state">No acquisition advice available</div>';
    critical.innerHTML = '';
    important.innerHTML = '';
    return;
  }

  overview.innerHTML = `
    <div class="advice-overview-card">
      <div class="advice-overview-head">
        <div>
          <div class="advice-overline">Search-fund take</div>
          <h3>${escapeHtml(advice.attractiveness)}</h3>
        </div>
        <span class="advice-confidence ${safeClass(advice.confidence, SAFE_CONFIDENCE_CLASSES, 'low')}">${escapeHtml(advice.confidence)} confidence</span>
      </div>
      <p class="advice-summary">${escapeHtml(advice.summary)}</p>
      <div class="advice-risk-strip">
        ${(advice.keyRisks || []).map((risk) => `
          <div class="advice-risk-pill ${safeClass(risk.severity, SAFE_STATUS_CLASSES, 'low')}">
            <strong>${escapeHtml(risk.dimension)}</strong>
            <span>${escapeHtml(risk.text)}</span>
          </div>
        `).join('')}
      </div>
    </div>
  `;

  critical.innerHTML = renderAdviceItems(advice.criticalBeforeLoi);
  important.innerHTML = renderAdviceItems(advice.importantDuringDiligence);
}

function renderAdviceItems(items = []) {
  if (items.length === 0) {
    return '<div class="empty-state">No additional items identified</div>';
  }

  return items.map((item) => `
    <div class="advice-item">
      <div class="advice-item-top">
        <span class="advice-priority">P${item.priority}</span>
        <span class="advice-category">${escapeHtml(item.category)}</span>
        <span class="advice-confidence ${safeClass(item.confidence, SAFE_CONFIDENCE_CLASSES, 'low')}">${escapeHtml(item.confidence)} confidence</span>
      </div>
      <h4>${escapeHtml(item.title)}</h4>
      <p>${escapeHtml(item.body)}</p>
      ${renderAdviceSupport(item.support)}
    </div>
  `).join('');
}

function renderAdviceSupport(support = {}) {
  const bits = [];

  if (support.requests?.length) {
    bits.push(`
      <div class="advice-support-block">
        <div class="advice-support-label">Request next</div>
        <div class="advice-support-list">${support.requests.map((entry) => `<span>${escapeHtml(entry)}</span>`).join('')}</div>
      </div>
    `);
  }

  if (support.managementQuestions?.length) {
    bits.push(`
      <div class="advice-support-block">
        <div class="advice-support-label">Management questions</div>
        <div class="advice-support-list">${support.managementQuestions.map((entry) => `<span>${escapeHtml(entry)}</span>`).join('')}</div>
      </div>
    `);
  }

  if (support.valuationImpact) {
    bits.push(`
      <div class="advice-support-block">
        <div class="advice-support-label">Valuation / structure</div>
        <div class="advice-support-note">${escapeHtml(support.valuationImpact)}</div>
      </div>
    `);
  }

  if (support.structure) {
    bits.push(`
      <div class="advice-support-block">
        <div class="advice-support-label">Deal structure angle</div>
        <div class="advice-support-note">${escapeHtml(support.structure)}</div>
      </div>
    `);
  }

  return bits.join('');
}

function renderAnalyticsKpis(data) {
  const kpiGrid = $('#analytics-kpi-grid');
  if (!kpiGrid) return;

  const latestRevenue = data.revenue?.revenue?.at(-1);
  const firstRevenue = data.revenue?.revenue?.[0];
  const periods = (data.revenue?.revenue?.length || 1) - 1;
  const revenueCagr = latestRevenue && firstRevenue && periods > 0
    ? ((Math.pow(latestRevenue / firstRevenue, 1 / periods) - 1) * 100)
    : null;

  const latestMargin = data.margins?.ebitda?.at(-1);
  const topCustomer = data.customerBreakdown?.customers?.[0]?.percentage;
  const leverage = data.leverage?.debtToEbitda?.at(-1);
  const concentrationDetail = data.customerBreakdown?.proxy ? 'Exposure proxy from aging detail' : 'Share of trailing revenue';

  const kpis = [
    {
      label: 'Revenue CAGR',
      value: revenueCagr != null ? `${revenueCagr.toFixed(1)}%` : 'N/A',
      tone: 'blue',
      detail: latestRevenue != null ? `${formatMillions(latestRevenue)} trailing revenue` : 'Insufficient data',
    },
    {
      label: 'EBITDA Margin',
      value: latestMargin != null ? `${latestMargin.toFixed(1)}%` : 'N/A',
      tone: 'green',
      detail: 'Profitability at latest period',
    },
    {
      label: 'Top Customer',
      value: topCustomer != null ? `${topCustomer}%` : 'N/A',
      tone: 'amber',
      detail: concentrationDetail,
    },
    {
      label: 'Debt / EBITDA',
      value: leverage != null ? `${leverage.toFixed(1)}x` : 'N/A',
      tone: 'red',
      detail: 'Latest leverage ratio',
    },
  ];

  kpiGrid.innerHTML = kpis.map((kpi) => `
    <div class="analytics-kpi-card ${kpi.tone}">
      <div class="analytics-kpi-label">${escapeHtml(kpi.label)}</div>
      <div class="analytics-kpi-value">${escapeHtml(kpi.value)}</div>
      <div class="analytics-kpi-detail">${escapeHtml(kpi.detail)}</div>
    </div>
  `).join('');
}

function renderSignalMap(data) {
  const signalMap = $('#signal-map');
  if (!signalMap) return;

  signalMap.innerHTML = (data.subScores || []).map((score) => {
    const scoreValue = clampNumber(score.score, 0, 100);
    return `
    <div class="signal-row">
      <div class="signal-row-main">
        <div class="signal-row-label">
          <strong>${escapeHtml(score.label)}</strong>
          <span>${escapeHtml(score.note)}</span>
        </div>
        <div class="signal-row-score" style="color:${scoreColor(scoreValue)}">${scoreValue}</div>
      </div>
      <div class="signal-track">
        <div class="signal-fill" style="width:${scoreValue}%; background:${scoreColor(scoreValue)}"></div>
      </div>
    </div>
  `;
  }).join('');
}

function renderConcentrationMonitor(data) {
  const monitor = $('#concentration-monitor');
  if (!monitor) return;

  const customers = data.customerBreakdown?.customers || [];
  if (customers.length === 0) {
    monitor.innerHTML = '<div class="empty-state">No customer concentration data available</div>';
    return;
  }

  const sourceLabel = data.customerBreakdown?.sourceLabel
    ? `<div class="concentration-source">${escapeHtml(data.customerBreakdown.sourceLabel)}</div>`
    : '';

  monitor.innerHTML = `${sourceLabel}${customers.map((customer, index) => {
    const percentage = clampNumber(customer.percentage, 0, 100);
    return `
    <div class="concentration-row">
      <div class="concentration-meta">
        <span class="concentration-name">${escapeHtml(customer.name)}</span>
        <span class="concentration-value">${percentage}%</span>
      </div>
      <div class="concentration-bar">
        <div class="concentration-fill concentration-fill-${Math.min(index + 1, 6)}" style="width:${percentage}%"></div>
      </div>
    </div>
  `;
  }).join('')}`;
}

function renderDebtLadder(data) {
  const ladder = $('#debt-ladder');
  if (!ladder) return;

  const instruments = data.debtProfile?.instruments || [];
  if (instruments.length === 0) {
    ladder.innerHTML = '<div class="empty-state">No debt schedule available</div>';
    return;
  }

  const totalPrincipal = instruments.reduce((sum, instrument) => sum + toFiniteNumber(instrument.principal, 0), 0);
  const maturities = instruments.map((instrument) => new Date(instrument.maturity).getTime()).filter(Boolean);
  const minMaturity = Math.min(...maturities);
  const maxMaturity = Math.max(...maturities);
  const maturitySpan = Math.max(maxMaturity - minMaturity, 1);

  ladder.innerHTML = instruments.map((instrument) => {
    const maturityTime = new Date(instrument.maturity).getTime();
    const hasValidMaturity = Boolean(instrument.maturity) && !Number.isNaN(maturityTime);
    const position = hasValidMaturity && Number.isFinite(minMaturity)
      ? clampNumber(((maturityTime - minMaturity) / maturitySpan) * 100, 0, 100)
      : 0;
    const rate = Number.isFinite(Number(instrument.rate)) ? Number(instrument.rate) : null;
    const principal = toFiniteNumber(instrument.principal, 0);
    const rateLabel = rate != null ? `${rate.toFixed(2)}% rate` : 'Rate N/A';
    const principalShare = totalPrincipal > 0 && principal > 0
      ? `${((principal / totalPrincipal) * 100).toFixed(0)}% of debt stack`
      : 'Share N/A';

    return `
      <div class="debt-item">
        <div class="debt-item-top">
          <div>
            <div class="debt-name">${escapeHtml(instrument.name)}</div>
            <div class="debt-meta">${escapeHtml(formatDate(instrument.maturity))} maturity</div>
          </div>
          <div class="debt-amount">${formatMillions(principal)}</div>
        </div>
        <div class="debt-timeline">
          <div class="debt-timeline-line"></div>
          <div class="debt-timeline-point" style="left:${position}%"></div>
        </div>
        <div class="debt-item-bottom">
          <span>${escapeHtml(rateLabel)}</span>
          <span>${escapeHtml(principalShare)}</span>
        </div>
      </div>
    `;
  }).join('');
}

function formatMillions(value) {
  if (typeof value !== 'number' || !Number.isFinite(value)) return 'N/A';
  return `$${value.toFixed(1)}M`;
}

function formatDate(value) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return 'Maturity N/A';
  return parsed.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
}

function scoreColor(score) {
  if (score >= 80) return 'var(--green)';
  if (score >= 65) return 'var(--accent)';
  if (score >= 50) return 'var(--yellow)';
  return 'var(--red)';
}

function animateScore(target) {
  const el = $('#overall-score');
  const ring = $('.score-ring-fill');
  const safeTarget = clampNumber(target, 0, 100);
  const circumference = 326.73;
  const offset = circumference - (safeTarget / 100) * circumference;

  ring.style.stroke = scoreColor(safeTarget);
  ring.style.strokeDashoffset = offset;

  let current = 0;
  const step = () => {
    current += 1;
    if (current > safeTarget) { el.textContent = safeTarget; return; }
    el.textContent = current;
    requestAnimationFrame(step);
  };
  requestAnimationFrame(step);
}

// ===== CHARTS =====
// Chart rendering is now handled by charts.js (renderAllCharts)

// ===== NEW ANALYSIS =====
$('#new-analysis-btn').addEventListener('click', () => {
  state.files = [];
  state.activeReviewDocType = null;
  state.lastIngestion = null;
  state.lastDiagnostics = null;
  renderFileList();
  updateAnalyzeBtn();
  // Reset processing steps
  $$('#processing-steps .step').forEach(s => { s.classList.remove('active', 'done'); });
  showView('upload');
});
