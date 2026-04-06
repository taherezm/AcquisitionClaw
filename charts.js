// ============================================================
// charts.js — Modular chart rendering system
// ============================================================
//
// Each chart function:
//  - Receives a canvas element and its data slice
//  - Returns null if insufficient data (caller shows fallback)
//  - Uses shared defaults for consistent styling

// ===== PALETTE =====
const C = {
  blue:      '#2563eb',
  blueLight: 'rgba(37,99,235,.12)',
  blueFill:  'rgba(37,99,235,.7)',
  green:     '#16a34a',
  greenLight:'rgba(22,163,74,.12)',
  greenFill: 'rgba(22,163,74,.6)',
  red:       '#dc2626',
  redFill:   'rgba(220,38,38,.6)',
  gray:      '#9099ad',
  grayLight: 'rgba(144,153,173,.1)',
  orange:    '#ea580c',
  orangeFill:'rgba(234,88,12,.6)',
  yellow:    '#ca8a04',
  yellowFill:'rgba(202,138,4,.6)',
  purple:    '#7c3aed',
  purpleFill:'rgba(124,58,237,.6)',
  teal:      '#0d9488',
  tealFill:  'rgba(13,148,136,.6)',
  border:    '#eef0f3',
  piePalette: [
    'rgba(37,99,235,.75)',
    'rgba(22,163,74,.70)',
    'rgba(234,88,12,.70)',
    'rgba(124,58,237,.65)',
    'rgba(13,148,136,.65)',
    'rgba(202,138,4,.65)',
    'rgba(220,38,38,.60)',
    'rgba(144,153,173,.50)',
  ],
};

// ===== SHARED OPTIONS =====
function baseOpts(overrides = {}) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 800, easing: 'easeOutQuart' },
    plugins: {
      legend: { position: 'bottom', labels: { boxWidth: 10, padding: 14, font: { size: 11, family: 'Inter, system-ui, sans-serif' } } },
      tooltip: {
        backgroundColor: '#1a1d23', titleFont: { size: 12 }, bodyFont: { size: 11 },
        cornerRadius: 6, padding: 10, displayColors: true, boxPadding: 4,
      },
      ...overrides.plugins,
    },
    scales: {
      x: { grid: { display: false }, ticks: { font: { size: 11 } }, ...overrides.x },
      y: { grid: { color: C.border }, ticks: { font: { size: 11 } }, beginAtZero: true, ...overrides.y },
      ...(overrides.extraScales || {}),
    },
    ...overrides.root,
  };
}

function noScaleOpts(overrides = {}) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 800, easing: 'easeOutQuart' },
    plugins: {
      legend: { position: 'bottom', labels: { boxWidth: 10, padding: 14, font: { size: 11, family: 'Inter, system-ui, sans-serif' } } },
      tooltip: {
        backgroundColor: '#1a1d23', titleFont: { size: 12 }, bodyFont: { size: 11 },
        cornerRadius: 6, padding: 10, displayColors: true, boxPadding: 4,
      },
      ...overrides.plugins,
    },
    ...overrides.root,
  };
}

function hasData(arr) {
  return Array.isArray(arr) && arr.length > 0 && arr.some(v => v != null && v !== 0);
}

function getArray(value) {
  return Array.isArray(value) ? value : [];
}

// ===== CHART RENDERERS =====

/** 1. Revenue Trend — bar chart */
export function chartRevenue(canvas, data) {
  const labels = getArray(data.revenue?.labels);
  const revenue = getArray(data.revenue?.revenue);
  if (!hasData(revenue) || labels.length === 0) return null;
  return new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'Revenue ($M)',
        data: revenue,
        backgroundColor: C.blueFill,
        borderRadius: 4,
      }],
    },
    options: baseOpts({ y: { title: { display: true, text: '$ Millions', font: { size: 10 } } } }),
  });
}

/** 2. EBITDA Trend — bar chart */
export function chartEbitda(canvas, data) {
  const labels = getArray(data.revenue?.labels);
  const ebitda = getArray(data.revenue?.ebitda);
  if (!hasData(ebitda) || labels.length === 0) return null;
  return new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'EBITDA ($M)',
        data: ebitda,
        backgroundColor: C.greenFill,
        borderRadius: 4,
      }],
    },
    options: baseOpts({ y: { title: { display: true, text: '$ Millions', font: { size: 10 } } } }),
  });
}

/** 3. Margin Trends — line chart */
export function chartMargins(canvas, data) {
  const labels = getArray(data.margins?.labels);
  if ((!hasData(data.margins?.gross) && !hasData(data.margins?.ebitda)) || labels.length === 0) return null;
  const datasets = [];
  if (hasData(data.margins.gross)) {
    datasets.push({ label: 'Gross Margin %', data: data.margins.gross, borderColor: C.blue, backgroundColor: C.blueLight, fill: true, tension: .35, pointRadius: 3, borderWidth: 2 });
  }
  if (hasData(data.margins.ebitda)) {
    datasets.push({ label: 'EBITDA Margin %', data: data.margins.ebitda, borderColor: C.green, tension: .35, pointRadius: 3, borderWidth: 2 });
  }
  if (hasData(data.margins.net)) {
    datasets.push({ label: 'Net Margin %', data: data.margins.net, borderColor: C.gray, tension: .35, pointRadius: 3, borderWidth: 1.5, borderDash: [4, 3] });
  }
  return new Chart(canvas, {
    type: 'line',
    data: { labels, datasets },
    options: baseOpts({ y: { title: { display: true, text: '%', font: { size: 10 } } } }),
  });
}

/** 4. Customer Concentration — doughnut */
export function chartConcentration(canvas, data) {
  const cust = data.customerBreakdown;
  const customers = getArray(cust?.customers);
  if (customers.length === 0) return null;
  const shareLabel = cust.proxy ? 'of exposure' : 'of revenue';

  return new Chart(canvas, {
    type: 'doughnut',
    data: {
      labels: customers.map(c => c.name),
      datasets: [{
        data: customers.map(c => c.percentage),
        backgroundColor: C.piePalette.slice(0, customers.length),
        borderWidth: 2,
        borderColor: '#fff',
      }],
    },
    options: noScaleOpts({
      root: { cutout: '55%' },
      plugins: {
        tooltip: {
          callbacks: {
            label: (ctx) => ` ${ctx.label}: ${ctx.parsed}% ${shareLabel}`,
          },
        },
      },
    }),
  });
}

/** 5. Expense Composition — doughnut */
export function chartExpenses(canvas, data) {
  const exp = data.expenseComposition;
  const labels = getArray(exp?.labels);
  const values = getArray(exp?.values);
  if (labels.length === 0 || values.length === 0) return null;

  return new Chart(canvas, {
    type: 'doughnut',
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: C.piePalette.slice(0, labels.length),
        borderWidth: 2,
        borderColor: '#fff',
      }],
    },
    options: noScaleOpts({
      root: { cutout: '55%' },
      plugins: {
        tooltip: {
          callbacks: {
            label: (ctx) => ` ${ctx.label}: $${ctx.parsed.toFixed(1)}M`,
          },
        },
      },
    }),
  });
}

/** 6. AR Aging — horizontal bar */
export function chartARAging(canvas, data) {
  const ar = data.arAging;
  const labels = getArray(ar?.labels);
  const values = getArray(ar?.values);
  if (labels.length === 0 || values.length === 0) return null;

  return new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: '$ Amount',
        data: values,
        backgroundColor: values.map((_, i) => {
          const colors = [C.greenFill, C.blueFill, C.yellowFill, C.orangeFill, C.redFill];
          return colors[i] || C.grayLight;
        }),
        borderRadius: 4,
      }],
    },
    options: baseOpts({
      root: { indexAxis: 'y' },
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: (ctx) => ` $${ctx.parsed.x.toLocaleString()}` } },
      },
    }),
  });
}

/** 7. AP Aging — horizontal bar */
export function chartAPAging(canvas, data) {
  const ap = data.apAging;
  const labels = getArray(ap?.labels);
  const values = getArray(ap?.values);
  if (labels.length === 0 || values.length === 0) return null;

  return new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: '$ Amount',
        data: values,
        backgroundColor: values.map((_, i) => {
          const colors = [C.greenFill, C.blueFill, C.yellowFill, C.orangeFill, C.redFill];
          return colors[i] || C.grayLight;
        }),
        borderRadius: 4,
      }],
    },
    options: baseOpts({
      root: { indexAxis: 'y' },
      plugins: {
        legend: { display: false },
        tooltip: { callbacks: { label: (ctx) => ` $${ctx.parsed.x.toLocaleString()}` } },
      },
    }),
  });
}

/** 8. Debt Profile — horizontal stacked bar with rates */
export function chartDebt(canvas, data) {
  const debt = data.debtProfile;
  const instruments = getArray(debt?.instruments);
  if (instruments.length === 0) return null;

  return new Chart(canvas, {
    type: 'bar',
    data: {
      labels: instruments.map(d => d.name),
      datasets: [{
        label: 'Principal ($M)',
        data: instruments.map(d => d.principal),
        backgroundColor: instruments.map((_, i) => C.piePalette[i] || C.grayLight),
        borderRadius: 4,
      }],
    },
    options: baseOpts({
      root: { indexAxis: 'y' },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            afterLabel: (ctx) => {
              const inst = instruments[ctx.dataIndex] || {};
              return `Rate: ${inst.rate}% · Maturity: ${inst.maturity}`;
            },
            label: (ctx) => ` $${ctx.parsed.x.toFixed(1)}M`,
          },
        },
      },
    }),
  });
}

/** 9. Forecast vs Historical — combined bar + line */
export function chartForecast(canvas, data) {
  const fc = data.forecastComparison;
  const labels = getArray(fc?.labels);
  if (labels.length === 0) return null;

  const datasets = [];

  // Historical bars
  if (hasData(fc.historicalRevenue)) {
    datasets.push({
      label: 'Historical Revenue ($M)',
      data: fc.historicalRevenue,
      backgroundColor: C.blueFill,
      borderRadius: 4,
      order: 2,
    });
  }

  // Projected bars (different shade)
  if (hasData(fc.projectedRevenue)) {
    datasets.push({
      label: 'Projected Revenue ($M)',
      data: fc.projectedRevenue,
      backgroundColor: 'rgba(37,99,235,.3)',
      borderColor: C.blue,
      borderWidth: 1.5,
      borderDash: [4, 3],
      borderRadius: 4,
      order: 2,
    });
  }

  // EBITDA line spanning both
  const ebitdaLine = getArray(fc?.historicalEbitda).concat(getArray(fc?.projectedEbitda));
  if (hasData(ebitdaLine)) {
    datasets.push({
      label: 'EBITDA ($M)',
      data: ebitdaLine,
      type: 'line',
      borderColor: C.green,
      backgroundColor: C.greenLight,
      tension: .35,
      pointRadius: 3,
      borderWidth: 2,
      yAxisID: 'y',
      order: 1,
    });
  }

  return new Chart(canvas, {
    type: 'bar',
    data: { labels, datasets },
    options: baseOpts({
      y: { title: { display: true, text: '$ Millions', font: { size: 10 } } },
      plugins: {
        annotation: fc.dividerIndex != null ? {
          annotations: { divider: {
            type: 'line', xMin: fc.dividerIndex - 0.5, xMax: fc.dividerIndex - 0.5,
            borderColor: C.gray, borderWidth: 1, borderDash: [5, 3],
            label: { display: true, content: 'Projected →', position: 'start', font: { size: 9 }, backgroundColor: 'transparent', color: C.gray },
          } },
        } : {},
      },
    }),
  });
}

/** 10. Working Capital Trend — stacked area */
export function chartWorkingCapital(canvas, data) {
  const wc = data.workingCapital;
  const labels = getArray(wc?.labels);
  if (labels.length === 0) return null;

  const datasets = [];

  if (hasData(wc.currentAssets)) {
    datasets.push({
      label: 'Current Assets ($M)',
      data: wc.currentAssets,
      borderColor: C.blue,
      backgroundColor: C.blueLight,
      fill: true,
      tension: .35,
      pointRadius: 3,
      borderWidth: 2,
    });
  }
  if (hasData(wc.currentLiabilities)) {
    datasets.push({
      label: 'Current Liabilities ($M)',
      data: wc.currentLiabilities,
      borderColor: C.red,
      backgroundColor: 'rgba(220,38,38,.08)',
      fill: true,
      tension: .35,
      pointRadius: 3,
      borderWidth: 2,
    });
  }
  if (hasData(wc.netWorkingCapital)) {
    datasets.push({
      label: 'Net Working Capital ($M)',
      data: wc.netWorkingCapital,
      borderColor: C.green,
      tension: .35,
      pointRadius: 4,
      borderWidth: 2.5,
      borderDash: [0],
    });
  }

  return new Chart(canvas, {
    type: 'line',
    data: { labels, datasets },
    options: baseOpts({
      y: { title: { display: true, text: '$ Millions', font: { size: 10 } } },
    }),
  });
}

/** 11. Leverage (kept from original) — dual-axis line */
export function chartLeverage(canvas, data) {
  const labels = getArray(data.leverage?.labels);
  const debtToEbitda = getArray(data.leverage?.debtToEbitda);
  if (!hasData(debtToEbitda) || labels.length === 0) return null;

  const datasets = [
    { label: 'Debt/EBITDA', data: debtToEbitda, borderColor: C.red, tension: .3, pointRadius: 3, borderWidth: 2, yAxisID: 'y' },
  ];
  if (hasData(data.leverage.interestCoverage)) {
    datasets.push(
      { label: 'Interest Coverage', data: data.leverage.interestCoverage, borderColor: C.blue, tension: .3, pointRadius: 3, borderWidth: 2, yAxisID: 'y1' },
    );
  }

  return new Chart(canvas, {
    type: 'line',
    data: { labels, datasets },
    options: baseOpts({
      y: { position: 'left', title: { display: true, text: 'Debt/EBITDA', font: { size: 10 } } },
      extraScales: {
        y1: { position: 'right', grid: { drawOnChartArea: false }, title: { display: true, text: 'Coverage', font: { size: 10 } }, ticks: { font: { size: 11 } } },
      },
    }),
  });
}

/** 12. Cash Flow Composition (kept from original) — bar */
export function chartCashflow(canvas, data) {
  const values = getArray(data.cashflow?.values);
  const labels = getArray(data.cashflow?.labels);
  if (!hasData(values) || labels.length === 0) return null;

  return new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: '$ Millions',
        data: values,
        backgroundColor: values.map(v => v >= 0 ? C.blueFill : C.redFill),
        borderRadius: 4,
      }],
    },
    options: baseOpts({ plugins: { legend: { display: false } } }),
  });
}

// ===== ORCHESTRATOR =====

const CHART_REGISTRY = [
  { id: 'chart-revenue',       title: 'Revenue Trend',                renderer: chartRevenue },
  { id: 'chart-ebitda',        title: 'EBITDA Trend',                 renderer: chartEbitda },
  { id: 'chart-margins',       title: 'Margin Analysis',              renderer: chartMargins },
  { id: 'chart-concentration', title: 'Customer Concentration',       renderer: chartConcentration },
  { id: 'chart-expenses',      title: 'Expense Composition',          renderer: chartExpenses },
  { id: 'chart-ar-aging',      title: 'Accounts Receivable Aging',    renderer: chartARAging },
  { id: 'chart-ap-aging',      title: 'Accounts Payable Aging',       renderer: chartAPAging },
  { id: 'chart-debt',          title: 'Debt Profile',                 renderer: chartDebt },
  { id: 'chart-forecast',      title: 'Forecast vs. Historical',      renderer: chartForecast },
  { id: 'chart-working-cap',   title: 'Working Capital Trend',        renderer: chartWorkingCapital },
  { id: 'chart-leverage',      title: 'Leverage Metrics',             renderer: chartLeverage },
  { id: 'chart-cashflow',      title: 'Cash Flow Composition',        renderer: chartCashflow },
];

// Track active chart instances for cleanup
let activeCharts = [];

/**
 * Render all charts into their containers.
 * Shows fallback message when data is insufficient.
 */
export function renderAllCharts(data) {
  // Destroy previous instances
  for (const chart of activeCharts) {
    try { chart.destroy(); } catch (e) { /* already destroyed */ }
  }
  activeCharts = [];

  for (const entry of CHART_REGISTRY) {
    const container = document.getElementById(entry.id)?.closest('.chart-card');
    const canvas = document.getElementById(entry.id);
    if (!container || !canvas) continue;

    // Clear any existing fallback
    const existingFallback = container.querySelector('.chart-fallback');
    if (existingFallback) existingFallback.remove();
    canvas.style.display = '';

    // Attempt render
    let instance = null;
    try {
      instance = entry.renderer(canvas, data);
    } catch (e) {
      console.warn(`Chart "${entry.title}" failed:`, e);
    }

    if (instance) {
      activeCharts.push(instance);
      container.classList.remove('chart-no-data');
    } else {
      // Show fallback
      canvas.style.display = 'none';
      container.classList.add('chart-no-data');
      const fallback = document.createElement('div');
      fallback.className = 'chart-fallback';
      fallback.innerHTML = `<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M21 21H4.6c-.56 0-.84 0-1.054-.109a1 1 0 0 1-.437-.437C3 20.24 3 19.96 3 19.4V3"/><path d="M7 14l4-4 4 4 6-6"/></svg>
        <span>Insufficient data</span>`;
      container.appendChild(fallback);
    }
  }
}
