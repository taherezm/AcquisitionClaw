import { runPipeline, getDocTypeLabel } from './ingestion/pipeline.js';
import { classifyDocument } from './ingestion/classifier.js';
import { DOC_TYPES } from './ingestion/schemas.js';
import { renderAllCharts } from './charts.js';
import { ingestFiles, buildPipelineFileDescriptors } from './api.js';

// ===== STATE =====
const state = {
  files: [],
  companyName: '',
  industry: '',
  ebitdaRange: '',
  ingestionResult: null,
  isAnalyzing: false,
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
const industrySelect = $('#industry');
const ebitdaSelect = $('#ebitda-range');
const analyzeBtnDefaultMarkup = analyzeBtn.innerHTML;

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
  state.files.splice(index, 1);
  renderFileList();
  updateAnalyzeBtn();
}

function renderFileList() {
  fileList.innerHTML = state.files.map((f, i) => {
    const docType = detectDocType(f.name);
    return `<div class="file-item">
      <div class="file-info">
        <span class="file-type-badge">${getFileExt(f.name)}</span>
        <span class="file-name">${f.name}</span>
        ${docType ? `<span class="file-doc-type">&mdash; ${docType}</span>` : ''}
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
  state.industry = industrySelect.options[industrySelect.selectedIndex].text;
  state.ebitdaRange = ebitdaSelect.options[ebitdaSelect.selectedIndex].text;
  state.ebitdaRangeValue = ebitdaSelect.value; // raw value like "1m-3m"
  if (state.industry === 'Select industry...') state.industry = '';
  if (state.ebitdaRange === 'Select range...') state.ebitdaRange = '';

  showView('processing');
  $('#processing-company').textContent = state.companyName;

  const companyContext = {
    companyName: state.companyName,
    industry: state.industry,
    ebitdaRange: state.ebitdaRangeValue || '1m-3m',
    allowDemoFallback: false,
  };

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
        await delay(500);
      } else if (i === 1) {
        ingestResponse = await ingestFiles(state.files, companyContext);
        state.ingestionResult = ingestResponse;

        if (!ingestResponse.summary?.acceptedFiles) {
          throw new Error('No supported files were accepted. Upload CSV or XLSX files for v1. PDF parsing is not implemented yet.');
        }

        await delay(250);
      } else if (i === 2) {
        const fileDescriptors = buildPipelineFileDescriptors(ingestResponse);
        pipelineResult = runPipeline(fileDescriptors, companyContext);
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
      backendIngestion: ingestResponse,
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
  animateScore(data.overallScore);
  $('#score-verdict').textContent = data.verdict;
  $('#score-verdict').style.color = scoreColor(data.overallScore);
  $('#score-description').textContent = data.description;

  // Overall confidence badge (inject after description if present)
  const descEl = $('#score-description');
  const existingBadge = descEl.parentElement.querySelector('.overall-confidence');
  if (existingBadge) existingBadge.remove();
  if (data.overallConfidence) {
    const badge = document.createElement('span');
    badge.className = `overall-confidence ${data.overallConfidence}`;
    badge.textContent = `${data.overallConfidence} confidence`;
    descEl.parentElement.appendChild(badge);
  }

  // Strengths
  const strengthsList = $('#strengths-list');
  if (data.strengths && data.strengths.length > 0) {
    strengthsList.innerHTML = data.strengths.map(s => `
      <div class="strength-item">
        <span class="strength-dim">${s.dimension}</span>
        <span>${s.text}</span>
      </div>
    `).join('');
  } else {
    strengthsList.innerHTML = '<div class="empty-state">No standout strengths identified</div>';
  }

  // Risks
  const risksList = $('#risks-list');
  if (data.risks && data.risks.length > 0) {
    risksList.innerHTML = data.risks.slice(0, 6).map(r => `
      <div class="risk-item ${r.severity}">
        <span class="risk-dim">${r.dimension}</span>
        <span>${r.text}</span>
      </div>
    `).join('');
  } else {
    risksList.innerHTML = '<div class="empty-state">No significant risks identified</div>';
  }

  // Sub-scores (expandable cards with explanation, metrics, logic)
  const grid = $('#sub-scores-grid');
  grid.innerHTML = data.subScores.map(s => {
    const metricsHtml = (s.metrics || []).map(m =>
      `<span class="sub-score-metric"><strong>${m.name}:</strong> ${m.value}</span>`
    ).join('');

    return `<div class="sub-score-card" data-key="${s.key}">
      <span class="sub-score-expand-icon">&#9662;</span>
      <div class="sub-score-label">${s.label}</div>
      <div class="sub-score-value" style="color:${scoreColor(s.score)}">${s.score}</div>
      <div class="sub-score-bar"><div class="sub-score-bar-fill" data-width="${s.score}" style="background:${scoreColor(s.score)}"></div></div>
      <div class="sub-score-note">${s.note}</div>
      ${s.confidence ? `<span class="sub-score-confidence ${s.confidence}">${s.confidence} confidence</span>` : ''}
      <div class="sub-score-detail">
        <div class="sub-score-explanation">${s.explanation || ''}</div>
        ${metricsHtml ? `<div class="sub-score-metrics">${metricsHtml}</div>` : ''}
        ${s.logic ? `<div class="sub-score-logic">${s.logic}</div>` : ''}
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
        <span class="missing-impact ${m.impact}">${m.impact}</span>
        <div>
          <div class="missing-text">${m.text}</div>
          ${m.affects ? `<div class="missing-affects">Affects: ${m.affects}</div>` : ''}
        </div>
      </div>
    `).join('');
  } else {
    missingTitle.style.display = 'none';
    missingEl.innerHTML = '';
  }

  // Risk flags
  $('#risk-flags').innerHTML = (data.riskFlags || []).map(r => `
    <div class="risk-flag ${r.severity}">
      <span class="risk-flag-severity">${r.severity}</span>
      <span>${r.text}</span>
    </div>
  `).join('');

  // Charts
  renderAnalyticsStudio(data);
  renderAllCharts(data);

  // Investment summary
  data.investmentSummary = data.investmentSummary.replace('The target', state.companyName);
  $('#investment-summary').innerHTML = data.investmentSummary;

  // Acquisition advice
  renderAcquisitionAdvice(data.acquisitionAdvice);

  // Growth opportunities
  renderGrowthOpportunities(data.growthOpportunities);

  // Next steps
  $('#next-steps').innerHTML = data.nextSteps.map((s, i) => `
    <div class="next-step">
      <div class="next-step-number">${i + 1}</div>
      <div>
        <div class="next-step-title">${s.title}</div>
        <div class="next-step-desc">${s.desc}</div>
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
    return;
  }

  const extraction = dataQuality.extractionConfidence || {};
  const adjustment = dataQuality.confidenceAdjustment || {};
  summaryEl.innerHTML = `
    <div class="quality-metric">
      <div class="quality-metric-label">Extraction Confidence</div>
      <div class="quality-metric-value">${extraction.averagePct ?? 0}%</div>
      <div class="quality-metric-note">${extraction.documentCount || 0} normalized document${extraction.documentCount === 1 ? '' : 's'} in scoring</div>
    </div>
    <div class="quality-metric">
      <div class="quality-metric-label">Validation Status</div>
      <div class="quality-metric-value">${formatValidationStatus(dataQuality.validationStatus)}</div>
      <div class="quality-metric-note">${dataQuality.summary || 'No validation summary available.'}</div>
    </div>
    <div class="quality-metric">
      <div class="quality-metric-label">Confidence Adjustment</div>
      <div class="quality-metric-value">${formatConfidenceAdjustment(adjustment.delta)}</div>
      <div class="quality-metric-note">${formatAdjustmentNote(adjustment)}</div>
    </div>
  `;

  docsEl.innerHTML = (dataQuality.documents || []).length > 0
    ? dataQuality.documents.map((doc) => `
      <div class="quality-document">
        <div>
          <h4>${doc.label}</h4>
          <div class="quality-document-meta">${doc.source}${doc.warningCount ? ` • ${doc.warningCount} extraction warning${doc.warningCount === 1 ? '' : 's'}` : ''}</div>
        </div>
        <div class="quality-doc-badges">
          <span class="quality-badge ${doc.confidenceLabel}">${doc.confidencePct}% confidence</span>
          <span class="quality-badge ${doc.source === 'modeled fallback' ? 'low' : 'validated'}">${doc.source}</span>
        </div>
      </div>
    `).join('')
    : '<div class="empty-state">No normalized documents were available for scoring</div>';

  const findings = [
    ...(dataQuality.hardErrors || []).slice(0, 3),
    ...(dataQuality.validationWarnings || []).slice(0, 4),
  ];

  findingsEl.innerHTML = `
    <div class="quality-section-label">Validation Findings</div>
    ${findings.length > 0
      ? findings.map((finding) => `
        <div class="quality-finding ${finding.severity}">
          <span class="quality-finding-tag">${finding.severity === 'hard_error' ? 'hard error' : 'warning'}</span>
          <span>${finding.message}</span>
        </div>
      `).join('')
      : '<div class="empty-state">Validation did not surface material issues</div>'}
  `;

  const notes = (dataQuality.missingDataNotes || []).slice(0, 5);
  missingEl.innerHTML = `
    <div class="quality-section-label">Missing-Data Notes</div>
    ${notes.length > 0
      ? notes.map((note) => `
        <div class="quality-finding-note">
          <span class="quality-finding-tag">${note.impact || 'note'}</span>
          <span>${note.message}</span>
        </div>
      `).join('')
      : '<div class="empty-state">Normalized uploads covered the core validation checks</div>'}
  `;
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
      <p>${growth.summary}</p>
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
          <span class="growth-category-tag">${item.category}</span>
          <span class="advice-confidence ${item.confidence}">${item.confidence} confidence</span>
        </div>
        <h4>${item.title}</h4>
        <p>${item.whyItExists}</p>
        <div class="growth-impact-row">
          <span class="growth-window ${toGrowthWindowClass(item.executionWindow)}">${item.executionWindow || 'timing not specified'}</span>
        </div>
        <div class="growth-impact-row">
          <span class="growth-impact-label">Estimated EBITDA impact</span>
          <strong>${item.estimatedImpact}</strong>
        </div>
        ${renderGrowthDetailBlocks(item)}
      </div>
    `).join('')
    : '<div class="empty-state">No data-backed growth opportunities identified</div>';

  categoryGrid.innerHTML = categories.map((category) => `
    <div class="growth-category-card">
      <div class="growth-category-title">${category.name}</div>
      ${category.items?.length
        ? category.items.map((item) => `
          <div class="growth-item">
            <div class="growth-item-top">
              <h4>${item.title}</h4>
              <span class="advice-confidence ${item.confidence}">${item.confidence} confidence</span>
            </div>
            <p class="growth-item-why">${item.whyItExists}</p>
            <div class="growth-metrics">
              ${(item.supportingMetrics || []).map((metric) => `<span>${metric}</span>`).join('')}
            </div>
            <div class="growth-item-bottom">
              <span class="growth-window ${toGrowthWindowClass(item.executionWindow)}">${item.executionWindow || 'timing not specified'}</span>
            </div>
            <div class="growth-item-bottom">
              <span class="growth-impact-label">Estimated EBITDA impact</span>
              <strong>${item.estimatedImpact}</strong>
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

function renderGrowthDetailBlocks(item) {
  const bits = [];

  if (item.underwritingAssumptions?.length) {
    bits.push(`
      <div class="growth-detail-block">
        <div class="growth-detail-label">Underwriting assumptions</div>
        <div class="growth-detail-list">${item.underwritingAssumptions.map((entry) => `<span>${entry}</span>`).join('')}</div>
      </div>
    `);
  }

  if (item.evidenceDocuments?.length) {
    bits.push(`
      <div class="growth-detail-block">
        <div class="growth-detail-label">Evidence documents</div>
        <div class="growth-detail-list">
          ${item.evidenceDocuments.map((doc) => `<span class="${doc.provided ? 'provided' : 'missing'}">${doc.label}${doc.provided ? '' : ' (missing)'}</span>`).join('')}
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
          <h3>${advice.attractiveness}</h3>
        </div>
        <span class="advice-confidence ${advice.confidence}">${advice.confidence} confidence</span>
      </div>
      <p class="advice-summary">${advice.summary}</p>
      <div class="advice-risk-strip">
        ${(advice.keyRisks || []).map((risk) => `
          <div class="advice-risk-pill ${risk.severity}">
            <strong>${risk.dimension}</strong>
            <span>${risk.text}</span>
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
        <span class="advice-category">${item.category}</span>
        <span class="advice-confidence ${item.confidence}">${item.confidence} confidence</span>
      </div>
      <h4>${item.title}</h4>
      <p>${item.body}</p>
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
        <div class="advice-support-list">${support.requests.map((entry) => `<span>${entry}</span>`).join('')}</div>
      </div>
    `);
  }

  if (support.managementQuestions?.length) {
    bits.push(`
      <div class="advice-support-block">
        <div class="advice-support-label">Management questions</div>
        <div class="advice-support-list">${support.managementQuestions.map((entry) => `<span>${entry}</span>`).join('')}</div>
      </div>
    `);
  }

  if (support.valuationImpact) {
    bits.push(`
      <div class="advice-support-block">
        <div class="advice-support-label">Valuation / structure</div>
        <div class="advice-support-note">${support.valuationImpact}</div>
      </div>
    `);
  }

  if (support.structure) {
    bits.push(`
      <div class="advice-support-block">
        <div class="advice-support-label">Deal structure angle</div>
        <div class="advice-support-note">${support.structure}</div>
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
      detail: 'Share of trailing revenue',
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
      <div class="analytics-kpi-label">${kpi.label}</div>
      <div class="analytics-kpi-value">${kpi.value}</div>
      <div class="analytics-kpi-detail">${kpi.detail}</div>
    </div>
  `).join('');
}

function renderSignalMap(data) {
  const signalMap = $('#signal-map');
  if (!signalMap) return;

  signalMap.innerHTML = (data.subScores || []).map((score) => `
    <div class="signal-row">
      <div class="signal-row-main">
        <div class="signal-row-label">
          <strong>${score.label}</strong>
          <span>${score.note}</span>
        </div>
        <div class="signal-row-score" style="color:${scoreColor(score.score)}">${score.score}</div>
      </div>
      <div class="signal-track">
        <div class="signal-fill" style="width:${score.score}%; background:${scoreColor(score.score)}"></div>
      </div>
    </div>
  `).join('');
}

function renderConcentrationMonitor(data) {
  const monitor = $('#concentration-monitor');
  if (!monitor) return;

  const customers = data.customerBreakdown?.customers || [];
  if (customers.length === 0) {
    monitor.innerHTML = '<div class="empty-state">No customer concentration data available</div>';
    return;
  }

  monitor.innerHTML = customers.map((customer, index) => `
    <div class="concentration-row">
      <div class="concentration-meta">
        <span class="concentration-name">${customer.name}</span>
        <span class="concentration-value">${customer.percentage}%</span>
      </div>
      <div class="concentration-bar">
        <div class="concentration-fill concentration-fill-${Math.min(index + 1, 6)}" style="width:${customer.percentage}%"></div>
      </div>
    </div>
  `).join('');
}

function renderDebtLadder(data) {
  const ladder = $('#debt-ladder');
  if (!ladder) return;

  const instruments = data.debtProfile?.instruments || [];
  if (instruments.length === 0) {
    ladder.innerHTML = '<div class="empty-state">No debt schedule available</div>';
    return;
  }

  const totalPrincipal = instruments.reduce((sum, instrument) => sum + instrument.principal, 0);
  const maturities = instruments.map((instrument) => new Date(instrument.maturity).getTime()).filter(Boolean);
  const minMaturity = Math.min(...maturities);
  const maxMaturity = Math.max(...maturities);
  const maturitySpan = Math.max(maxMaturity - minMaturity, 1);

  ladder.innerHTML = instruments.map((instrument) => {
    const maturityTime = new Date(instrument.maturity).getTime();
    const position = ((maturityTime - minMaturity) / maturitySpan) * 100;

    return `
      <div class="debt-item">
        <div class="debt-item-top">
          <div>
            <div class="debt-name">${instrument.name}</div>
            <div class="debt-meta">${formatDate(instrument.maturity)} maturity</div>
          </div>
          <div class="debt-amount">${formatMillions(instrument.principal)}</div>
        </div>
        <div class="debt-timeline">
          <div class="debt-timeline-line"></div>
          <div class="debt-timeline-point" style="left:${position}%"></div>
        </div>
        <div class="debt-item-bottom">
          <span>${instrument.rate.toFixed(2)}% rate</span>
          <span>${((instrument.principal / totalPrincipal) * 100).toFixed(0)}% of debt stack</span>
        </div>
      </div>
    `;
  }).join('');
}

function formatMillions(value) {
  return `$${value.toFixed(1)}M`;
}

function formatDate(value) {
  return new Date(value).toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
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
  const circumference = 326.73;
  const offset = circumference - (target / 100) * circumference;

  ring.style.stroke = scoreColor(target);
  ring.style.strokeDashoffset = offset;

  let current = 0;
  const step = () => {
    current += 1;
    if (current > target) { el.textContent = target; return; }
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
  renderFileList();
  updateAnalyzeBtn();
  // Reset processing steps
  $$('#processing-steps .step').forEach(s => { s.classList.remove('active', 'done'); });
  showView('upload');
});
