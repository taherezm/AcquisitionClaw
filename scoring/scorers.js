// ============================================================
// scorers.js — Individual scoring functions for each dimension
// ============================================================
//
// Every scorer returns a ScoreResult:
// {
//   key:          string,
//   label:        string,
//   score:        number (0–100),
//   confidence:   'high' | 'medium' | 'low' | 'none',
//   explanation:  string (plain-English),
//   metrics:      { name: value }[],   — inputs used
//   logic:        string,              — scoring rule description
//   flags:        string[],            — negative flags triggered
// }

import { INDUSTRY_BENCHMARKS } from '../ingestion/schemas.js';

// ---- Helpers ----

function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }
function r1(n) { return Math.round(n * 10) / 10; }
function last(arr) { if (!arr) return null; for (let i = arr.length - 1; i >= 0; i--) if (arr[i] != null) return arr[i]; return null; }
function prev(arr) { if (!arr || arr.length < 2) return null; for (let i = arr.length - 2; i >= 0; i--) if (arr[i] != null) return arr[i]; return null; }

function trend(arr) {
  const vals = (arr || []).filter(v => v != null);
  if (vals.length < 2) return null;
  // Simple linear direction: positive = improving
  let ups = 0;
  for (let i = 1; i < vals.length; i++) if (vals[i] > vals[i - 1]) ups++;
  return (ups / (vals.length - 1)) * 2 - 1; // -1 to +1
}

function cagr(arr) {
  const vals = (arr || []).filter(v => v != null && v > 0);
  if (vals.length < 2) return null;
  return r1((Math.pow(vals[vals.length - 1] / vals[0], 1 / (vals.length - 1)) - 1) * 100);
}

function conf(hasData, hasMultiPeriod) {
  if (!hasData) return 'none';
  if (hasMultiPeriod) return 'high';
  return 'medium';
}

// ============================================================
// 1. PROFITABILITY
// ============================================================
export function scoreProfitability(marginsData, bench) {
  const latestGross = last(marginsData.gross);
  const latestEbitda = last(marginsData.ebitda);
  const latestNet = last(marginsData.net);
  const prevEbitda = prev(marginsData.ebitda);
  const ebitdaTrend = trend(marginsData.ebitda);

  const hasData = latestEbitda != null;
  const hasMulti = marginsData.ebitda?.filter(v => v != null).length >= 3;

  const metrics = [];
  const flags = [];

  if (latestGross != null) metrics.push({ name: 'Gross Margin', value: `${r1(latestGross)}%` });
  if (latestEbitda != null) metrics.push({ name: 'EBITDA Margin', value: `${r1(latestEbitda)}%` });
  if (latestNet != null) metrics.push({ name: 'Net Margin', value: `${r1(latestNet)}%` });
  if (bench) metrics.push({ name: 'Industry Median EBITDA', value: `${bench.ebitdaMargin}%` });

  let score = 65;
  let explanation = '';

  if (latestEbitda != null) {
    const diff = latestEbitda - (bench?.ebitdaMargin || 14);
    // Base score from margin level relative to benchmark
    score = clamp(68 + diff * 2.2, 25, 95);

    // Trend adjustment
    if (ebitdaTrend != null && ebitdaTrend > 0.3) {
      score = Math.min(score + 5, 95);
    } else if (ebitdaTrend != null && ebitdaTrend < -0.3) {
      score = Math.max(score - 6, 25);
      flags.push('Declining EBITDA margins over the review period');
    }

    // Margin compression flag
    if (prevEbitda != null && latestEbitda < prevEbitda - 1.5) {
      flags.push(`EBITDA margin compressed from ${r1(prevEbitda)}% to ${r1(latestEbitda)}%`);
    }

    // Gross margin check
    if (latestGross != null && latestGross < 25) {
      flags.push(`Low gross margin (${r1(latestGross)}%) limits pricing power`);
      score = Math.max(score - 4, 25);
    }

    const benchStr = bench ? ` vs. ${bench.ebitdaMargin}% industry median` : '';
    const trendStr = ebitdaTrend != null
      ? (ebitdaTrend > 0.3 ? ', with an improving trend' : ebitdaTrend < -0.3 ? ', with a declining trend' : ', roughly stable')
      : '';
    explanation = `EBITDA margin of ${r1(latestEbitda)}%${benchStr}${trendStr}. ${score >= 75 ? 'Margins support healthy acquisition economics.' : 'Margin improvement opportunity should be modeled in the deal thesis.'}`;
  } else {
    explanation = 'Insufficient data to assess profitability. Income statement required.';
  }

  return {
    key: 'profitability', label: 'Profitability',
    score: Math.round(score),
    confidence: conf(hasData, hasMulti),
    explanation, metrics, flags,
    logic: 'Scores EBITDA margin level relative to industry benchmark, adjusted for trend direction and gross margin adequacy.',
  };
}

// ============================================================
// 2. REVENUE STABILITY
// ============================================================
export function scoreRevenueStability(revenueData) {
  const revs = (revenueData.revenue || []).filter(v => v != null && v > 0);
  const hasData = revs.length >= 2;
  const hasMulti = revs.length >= 4;

  const metrics = [];
  const flags = [];
  let score = 65;
  let explanation = '';

  if (hasData) {
    const growth = cagr(revs);
    metrics.push({ name: 'Revenue CAGR', value: growth != null ? `${growth}%` : 'N/A' });
    metrics.push({ name: 'Latest Revenue', value: `$${r1(revs[revs.length - 1])}M` });
    metrics.push({ name: 'Periods Observed', value: `${revs.length}` });

    // Compute YoY volatility
    const yoyRates = [];
    for (let i = 1; i < revs.length; i++) {
      yoyRates.push(((revs[i] - revs[i - 1]) / revs[i - 1]) * 100);
    }
    const avgGrowth = yoyRates.reduce((s, r) => s + r, 0) / yoyRates.length;
    const variance = yoyRates.reduce((s, r) => s + Math.pow(r - avgGrowth, 2), 0) / yoyRates.length;
    const volatility = Math.sqrt(variance);
    metrics.push({ name: 'Growth Volatility', value: `${r1(volatility)}pp` });

    // Any decline?
    const hasDecline = yoyRates.some(r => r < 0);
    const allPositive = yoyRates.every(r => r > 0);

    // Score components
    // Growth level
    if (growth >= 10) score = 82;
    else if (growth >= 5) score = 74;
    else if (growth >= 0) score = 62;
    else score = 40;

    // Consistency bonus/penalty
    if (allPositive && volatility < 5) score = Math.min(score + 8, 95);
    else if (hasDecline) {
      score = Math.max(score - 10, 25);
      flags.push('Revenue declined in at least one period');
    }
    if (volatility > 15) {
      score = Math.max(score - 5, 25);
      flags.push(`High revenue growth volatility (${r1(volatility)}pp)`);
    }

    explanation = `Revenue CAGR of ${growth}% over ${revs.length} periods. ${allPositive ? 'No revenue declines observed.' : 'Revenue declined in at least one period.'} Growth volatility is ${volatility < 5 ? 'low' : volatility < 10 ? 'moderate' : 'high'} at ${r1(volatility)}pp. ${score >= 75 ? 'Top-line trajectory supports acquisition valuation.' : 'Revenue trajectory warrants deeper commercial due diligence.'}`;
  } else {
    explanation = 'Insufficient revenue history to assess stability. Multiple periods of income statement data required.';
  }

  return {
    key: 'revenueStability', label: 'Revenue Stability',
    score: Math.round(score),
    confidence: conf(hasData, hasMulti),
    explanation, metrics, flags,
    logic: 'Evaluates revenue CAGR, consistency of growth across periods, and YoY volatility.',
  };
}

// ============================================================
// 3. LIQUIDITY
// ============================================================
export function scoreLiquidity(balanceSheetData) {
  const latest = balanceSheetData.latest || {};
  const previous = balanceSheetData.previous || {};

  const currentAssets = latest.totalCurrentAssets;
  const currentLiab = latest.totalCurrentLiabilities;
  const cash = latest.cash;
  const hasData = currentAssets != null && currentLiab != null && currentLiab > 0;
  const hasPrev = previous.totalCurrentAssets != null && previous.totalCurrentLiabilities != null;

  const metrics = [];
  const flags = [];
  let score = 65;
  let explanation = '';

  if (hasData) {
    const currentRatio = r1(currentAssets / currentLiab);
    metrics.push({ name: 'Current Ratio', value: `${currentRatio}x` });

    if (cash != null && currentLiab > 0) {
      const quickApprox = r1((cash + (latest.accountsReceivable || 0)) / currentLiab);
      metrics.push({ name: 'Quick Ratio (est.)', value: `${quickApprox}x` });
    }

    if (cash != null) metrics.push({ name: 'Cash', value: `$${r1(cash / 1000000)}M` });

    // Score
    if (currentRatio >= 2.0) score = 90;
    else if (currentRatio >= 1.5) score = 80;
    else if (currentRatio >= 1.2) score = 68;
    else if (currentRatio >= 1.0) score = 52;
    else score = 35;

    // Trend
    if (hasPrev) {
      const prevRatio = previous.totalCurrentAssets / previous.totalCurrentLiabilities;
      if (currentRatio < prevRatio - 0.2) {
        score = Math.max(score - 5, 25);
        flags.push(`Current ratio deteriorated from ${r1(prevRatio)}x to ${currentRatio}x`);
      }
    }

    // Low absolute cash
    if (cash != null && currentLiab > 0 && cash / currentLiab < 0.1) {
      flags.push('Cash position is thin relative to current obligations');
      score = Math.max(score - 4, 25);
    }

    explanation = `Current ratio of ${currentRatio}x ${currentRatio >= 1.5 ? 'indicates adequate working capital.' : currentRatio >= 1.0 ? 'is acceptable but leaves limited buffer.' : 'is below 1.0x — current liabilities exceed current assets.'} ${score >= 75 ? 'Liquidity supports normal operations and debt service.' : 'Working capital may constrain post-acquisition flexibility.'}`;
  } else {
    explanation = 'Balance sheet data required to assess liquidity. Current ratio cannot be computed.';
  }

  return {
    key: 'liquidity', label: 'Liquidity',
    score: Math.round(score),
    confidence: conf(hasData, hasPrev),
    explanation, metrics, flags,
    logic: 'Scores current ratio on a tiered scale (2.0x+ = strong, <1.0x = weak), adjusted for trend direction and cash adequacy.',
  };
}

// ============================================================
// 4. LEVERAGE
// ============================================================
export function scoreLeverage(leverageData) {
  const latestDTE = last(leverageData.debtToEbitda);
  const prevDTE = prev(leverageData.debtToEbitda);
  const latestIC = last(leverageData.interestCoverage);
  const dteTrend = trend(leverageData.debtToEbitda);

  const hasData = latestDTE != null;
  const hasMulti = (leverageData.debtToEbitda || []).filter(v => v != null).length >= 3;

  const metrics = [];
  const flags = [];
  let score = 65;
  let explanation = '';

  if (hasData) {
    metrics.push({ name: 'Debt / EBITDA', value: `${r1(latestDTE)}x` });
    if (latestIC != null) metrics.push({ name: 'Interest Coverage', value: `${r1(latestIC)}x` });

    // Score on Debt/EBITDA
    if (latestDTE < 1.5) score = 93;
    else if (latestDTE < 2.5) score = 82;
    else if (latestDTE < 3.5) score = 68;
    else if (latestDTE < 4.5) score = 50;
    else score = 32;

    // Interest coverage overlay
    if (latestIC != null) {
      if (latestIC < 2.0) {
        score = Math.max(score - 10, 20);
        flags.push(`Interest coverage of ${r1(latestIC)}x is dangerously low`);
      } else if (latestIC < 3.0) {
        score = Math.max(score - 4, 20);
        flags.push(`Interest coverage of ${r1(latestIC)}x is tight`);
      }
    }

    // Trend: rising leverage
    if (dteTrend != null && dteTrend > 0.3) {
      score = Math.max(score - 5, 20);
      flags.push('Leverage trending upward over the review period');
    }

    if (latestDTE > 3.5) {
      flags.push(`Debt/EBITDA of ${r1(latestDTE)}x exceeds typical acquisition financing threshold`);
    }

    const levelStr = latestDTE < 2.5 ? 'conservative' : latestDTE < 3.5 ? 'moderate' : latestDTE < 4.5 ? 'elevated' : 'high';
    explanation = `Leverage is ${levelStr} at ${r1(latestDTE)}x Debt/EBITDA. ${latestIC != null ? `Interest coverage of ${r1(latestIC)}x ${latestIC >= 3 ? 'provides adequate debt service capacity.' : 'may constrain debt service.'}` : ''} ${score >= 70 ? 'Capital structure is manageable for acquisition financing.' : 'Leverage presents refinancing and debt capacity risk.'}`;
  } else {
    explanation = 'Debt schedule and income statement data required to compute leverage ratios.';
  }

  return {
    key: 'leverage', label: 'Leverage',
    score: Math.round(score),
    confidence: conf(hasData, hasMulti),
    explanation, metrics, flags,
    logic: 'Tiered scoring on Debt/EBITDA (<1.5x = strong, >4.5x = weak), adjusted for interest coverage adequacy and leverage trend.',
  };
}

// ============================================================
// 5. CASH CONVERSION
// ============================================================
export function scoreCashConversion(cashflowData, revenueData) {
  const ocf = cashflowData.values?.[0];  // Operating
  const fcf = cashflowData.values?.[3];  // Free Cash Flow
  const latestEbitda = last(revenueData.ebitda);

  const hasData = ocf != null && latestEbitda != null && latestEbitda > 0;

  const metrics = [];
  const flags = [];
  let score = 65;
  let explanation = '';

  if (hasData) {
    const conversionPct = r1((ocf / latestEbitda) * 100);
    metrics.push({ name: 'OCF / EBITDA', value: `${conversionPct}%` });
    if (fcf != null) {
      const fcfConv = r1((fcf / latestEbitda) * 100);
      metrics.push({ name: 'FCF / EBITDA', value: `${fcfConv}%` });
    }
    metrics.push({ name: 'Operating Cash Flow', value: `$${r1(ocf)}M` });
    metrics.push({ name: 'EBITDA', value: `$${r1(latestEbitda)}M` });

    // Score
    if (conversionPct >= 95) score = 92;
    else if (conversionPct >= 85) score = 82;
    else if (conversionPct >= 70) score = 70;
    else if (conversionPct >= 50) score = 55;
    else score = 35;

    // FCF overlay: heavy capex penalty
    if (fcf != null && latestEbitda > 0) {
      const fcfConv = (fcf / latestEbitda) * 100;
      if (fcfConv < 40) {
        score = Math.max(score - 8, 20);
        flags.push('High capital intensity — FCF significantly below EBITDA');
      }
    }

    if (conversionPct < 70) {
      flags.push(`Cash conversion of ${conversionPct}% suggests working capital drag or non-cash earnings`);
    }

    explanation = `Operating cash flow converts at ${conversionPct}% of EBITDA. ${conversionPct >= 85 ? 'Strong conversion indicates high-quality, cash-generative earnings.' : conversionPct >= 70 ? 'Adequate conversion, though some working capital or timing effects may be present.' : 'Low conversion — investigate working capital dynamics and non-cash items.'}`;
  } else {
    explanation = 'Cash flow statement and EBITDA data required to assess cash conversion quality.';
  }

  return {
    key: 'cashConversion', label: 'Cash Conversion',
    score: Math.round(score),
    confidence: hasData ? 'high' : 'none',
    explanation, metrics, flags,
    logic: 'Scores OCF/EBITDA conversion ratio on a tiered scale (95%+ = excellent, <50% = weak), with penalty for high capital intensity.',
  };
}

// ============================================================
// 6. EARNINGS QUALITY
// ============================================================
export function scoreEarningsQuality(earningsQuality) {
  const { addBackPct, ownerCompAboveMarket } = earningsQuality;
  const hasData = addBackPct != null;

  const metrics = [];
  const flags = [];
  let score = 65;
  let explanation = '';

  if (hasData) {
    metrics.push({ name: 'Add-backs / Adj. EBITDA', value: `${r1(addBackPct)}%` });
    if (ownerCompAboveMarket != null) {
      metrics.push({ name: 'Owner Comp Above Market', value: `$${Math.round(ownerCompAboveMarket / 1000)}K` });
    }

    // Score
    if (addBackPct < 5) score = 92;
    else if (addBackPct < 10) score = 80;
    else if (addBackPct < 15) score = 68;
    else if (addBackPct < 25) score = 52;
    else score = 35;

    // Owner comp flag
    if (ownerCompAboveMarket != null && ownerCompAboveMarket > 100000) {
      flags.push(`Owner compensation exceeds market rate by ~$${Math.round(ownerCompAboveMarket / 1000)}K — requires normalization`);
    }

    if (addBackPct > 20) {
      flags.push(`Add-backs of ${r1(addBackPct)}% of EBITDA are aggressive — earnings reliability is uncertain`);
    }

    explanation = `EBITDA add-backs represent ${r1(addBackPct)}% of adjusted EBITDA. ${addBackPct < 10 ? 'Low adjustment levels suggest reliable, repeatable earnings.' : addBackPct < 20 ? 'Moderate add-backs — each line item should be validated in QoE.' : 'High add-back levels raise questions about the sustainability of reported earnings.'}`;
  } else {
    score = 60;
    explanation = 'No Quality of Earnings report available. Earnings quality cannot be directly assessed — recommend commissioning QoE analysis.';
    flags.push('No QoE data available to validate earnings adjustments');
  }

  return {
    key: 'earningsQuality', label: 'Earnings Quality',
    score: Math.round(score),
    confidence: hasData ? 'high' : 'low',
    explanation, metrics, flags,
    logic: 'Scores total add-backs as a percentage of adjusted EBITDA (<5% = excellent, >25% = poor). Flags aggressive owner compensation.',
  };
}

// ============================================================
// 7. CUSTOMER CONCENTRATION
// ============================================================
export function scoreConcentration(concentration) {
  const { topCustomerPct, top3Pct, top5Pct, customerCount } = concentration;
  const hasData = topCustomerPct != null;

  const metrics = [];
  const flags = [];
  let score = 65;
  let explanation = '';

  if (hasData) {
    metrics.push({ name: 'Top Customer', value: `${r1(topCustomerPct)}%` });
    if (top3Pct != null) metrics.push({ name: 'Top 3 Customers', value: `${r1(top3Pct)}%` });
    if (top5Pct != null) metrics.push({ name: 'Top 5 Customers', value: `${r1(top5Pct)}%` });
    if (customerCount != null) metrics.push({ name: 'Total Customers', value: `${customerCount}` });

    // Score on top customer
    if (topCustomerPct < 8) score = 94;
    else if (topCustomerPct < 12) score = 84;
    else if (topCustomerPct < 18) score = 72;
    else if (topCustomerPct < 25) score = 56;
    else if (topCustomerPct < 35) score = 42;
    else score = 28;

    // Top 3 overlay
    if (top3Pct != null && top3Pct > 50) {
      score = Math.max(score - 6, 20);
    }

    // Flags
    if (topCustomerPct > 20) {
      flags.push(`Top customer at ${r1(topCustomerPct)}% of revenue — loss would materially impair economics`);
    }
    if (top3Pct != null && top3Pct > 40) {
      flags.push(`Top 3 customers represent ${r1(top3Pct)}% — revenue base is concentrated`);
    }
    if (customerCount != null && customerCount < 15) {
      flags.push(`Only ${customerCount} total customers — limited diversification`);
    }

    const levelStr = topCustomerPct < 12 ? 'well-diversified' : topCustomerPct < 20 ? 'moderately concentrated' : 'highly concentrated';
    explanation = `Customer base is ${levelStr} with the top customer at ${r1(topCustomerPct)}% of revenue${top3Pct != null ? ` and top 3 at ${r1(top3Pct)}%` : ''}. ${score >= 70 ? 'Diversification provides resilience.' : 'Concentration risk should be addressed via earnout or holdback mechanisms in deal structure.'}`;
  } else {
    score = 55;
    explanation = 'No customer revenue breakdown available. Concentration risk is unknown — request customer-level revenue data.';
    flags.push('Customer concentration data unavailable');
  }

  return {
    key: 'concentration', label: 'Customer Concentration',
    score: Math.round(score),
    confidence: hasData ? 'high' : 'low',
    explanation, metrics, flags,
    logic: 'Tiered scoring on top customer revenue share (<8% = excellent, >35% = critical). Penalized further when top 3 exceed 50%.',
  };
}

// ============================================================
// 8. FORECAST CREDIBILITY
// ============================================================
export function scoreForecastCredibility(forecastData) {
  const { projectedGrowth, historicalCAGR, gap } = forecastData;
  const hasData = projectedGrowth != null;
  const hasHistorical = historicalCAGR != null;

  const metrics = [];
  const flags = [];
  let score = 65;
  let explanation = '';

  if (hasData) {
    metrics.push({ name: 'Projected Growth', value: `${r1(projectedGrowth)}%` });
    if (hasHistorical) {
      metrics.push({ name: 'Historical CAGR', value: `${r1(historicalCAGR)}%` });
      metrics.push({ name: 'Projection Gap', value: `${r1(gap)}pp` });
    }

    if (hasHistorical && gap != null) {
      const absGap = Math.abs(gap);
      if (absGap < 2) score = 90;
      else if (absGap < 4) score = 78;
      else if (absGap < 7) score = 62;
      else if (absGap < 12) score = 48;
      else score = 32;

      if (gap > 5) {
        flags.push(`Projections assume ${r1(projectedGrowth)}% growth vs. ${r1(historicalCAGR)}% historical — ${r1(gap)}pp gap`);
      }
      if (gap > 10) {
        flags.push('Forecast appears materially disconnected from historical performance');
      }

      explanation = `Management projects ${r1(projectedGrowth)}% growth vs. a trailing CAGR of ${r1(historicalCAGR)}% (${r1(gap)}pp gap). ${absGap < 4 ? 'Projections are well-anchored to historical performance.' : absGap < 7 ? 'Moderate projection gap — validate key growth assumptions.' : 'Material gap between forecast and history. Require bottom-up substantiation before underwriting projected growth.'}`;
    } else {
      score = 60;
      explanation = `Management projects ${r1(projectedGrowth)}% growth, but insufficient historical data to benchmark credibility.`;
    }
  } else {
    score = 58;
    explanation = 'No management projections provided. Forecast credibility cannot be assessed — deal valuation will rely entirely on historical performance.';
    flags.push('No forward projections available for evaluation');
  }

  return {
    key: 'forecastCredibility', label: 'Forecast Credibility',
    score: Math.round(score),
    confidence: hasData && hasHistorical ? 'high' : hasData ? 'medium' : 'low',
    explanation, metrics, flags,
    logic: 'Scores the gap between projected growth rate and historical CAGR (<2pp = excellent, >12pp = poor).',
  };
}
