// ============================================================
// engine.js — Scoring engine orchestrator
// ============================================================
//
// Consumes normalized financial data from the ingestion layer,
// runs all 8 dimension scorers, computes the overall score,
// and generates strengths / risks / missing diligence items.
//
// Returns a ScoringResult:
// {
//   overall:       { score, verdict, confidence, explanation },
//   dimensions:    ScoreResult[],        — from scorers.js
//   strengths:     { text, dimension }[],
//   risks:         { severity, text, dimension }[],
//   missingItems:  { text, impact }[],
//   flags:         { severity, text }[], — backward-compat risk flags
// }

import { DOC_TYPES, DOC_TYPE_LABELS, INDUSTRY_BENCHMARKS } from '../ingestion/schemas.js';
import {
  scoreProfitability,
  scoreRevenueStability,
  scoreLiquidity,
  scoreLeverage,
  scoreCashConversion,
  scoreEarningsQuality,
  scoreConcentration,
  scoreForecastCredibility,
} from './scorers.js';

// Weights for overall score — must sum to 1.0
const WEIGHTS = {
  profitability:      0.18,
  revenueStability:   0.12,
  liquidity:          0.08,
  leverage:           0.14,
  cashConversion:     0.13,
  earningsQuality:    0.13,
  concentration:      0.12,
  forecastCredibility: 0.10,
};

// Confidence multipliers — low-confidence scores are pulled toward neutral
const CONFIDENCE_DAMPING = {
  high:   1.0,
  medium: 0.85,
  low:    0.65,
  none:   0.40,
};

const NEUTRAL_SCORE = 60;

/**
 * Run the full scoring engine.
 *
 * @param {Object} financialModel - Assembled financial data from normalizer
 * @param {string} industry       - Industry name for benchmark lookup
 * @param {Object|null} validation - Financial validation result from validator.js
 * @returns {ScoringResult}
 */
export function runScoringEngine(financialModel, industry, validation = null) {
  const bench = INDUSTRY_BENCHMARKS[industry] || INDUSTRY_BENCHMARKS['Other'];
  const {
    revenueData, marginsData, leverageData, cashflowData,
    concentration, earningsQuality, forecastData, balanceSheetData,
  } = financialModel;

  // ---- Run all 8 scorers ----
  const dimensions = [
    scoreProfitability(marginsData, bench),
    scoreRevenueStability(revenueData),
    scoreLiquidity(balanceSheetData),
    scoreLeverage(leverageData),
    scoreCashConversion(cashflowData, revenueData),
    scoreEarningsQuality(earningsQuality),
    scoreConcentration(concentration),
    scoreForecastCredibility(forecastData),
  ];

  // ---- Compute overall score ----
  const overall = computeOverall(dimensions, validation);

  // ---- Extract strengths, risks, missing items ----
  const strengths = extractStrengths(dimensions);
  const risks = extractRisks(dimensions, validation);
  const missingItems = extractMissingItems(dimensions, financialModel.documentTypes, validation);

  // ---- Build backward-compatible risk flags ----
  const flags = buildRiskFlags(risks);

  return { overall, dimensions, strengths, risks, missingItems, flags };
}

// ---- Overall Score ----

function computeOverall(dimensions, validation) {
  let weightedSum = 0;
  let totalWeight = 0;
  let lowConfCount = 0;

  for (const dim of dimensions) {
    const w = WEIGHTS[dim.key] || 0;
    const damping = CONFIDENCE_DAMPING[dim.confidence] || 0.5;

    // Damp low-confidence scores toward neutral
    const dampedScore = NEUTRAL_SCORE + (dim.score - NEUTRAL_SCORE) * damping;
    weightedSum += dampedScore * w;
    totalWeight += w;

    if (dim.confidence === 'low' || dim.confidence === 'none') lowConfCount++;
  }

  const score = totalWeight > 0 ? Math.round(weightedSum / totalWeight) : NEUTRAL_SCORE;

  // Overall confidence
  let confidence;
  if (lowConfCount === 0) confidence = 'high';
  else if (lowConfCount <= 2) confidence = 'medium';
  else confidence = 'low';

  const baseConfidence = confidence;
  confidence = applyValidationConfidence(confidence, validation?.confidenceAdjustment);

  const verdict = generateVerdict(score);
  const explanation = generateOverallExplanation(score, dimensions, confidence, validation);

  return {
    score,
    verdict,
    confidence,
    baseConfidence,
    validationAdjustment: validation?.confidenceAdjustment || null,
    explanation,
  };
}

function generateVerdict(score) {
  if (score >= 85) return 'Strong — Highly Suitable for Acquisition';
  if (score >= 72) return 'Moderately Healthy — Suitable for Acquisition with Conditions';
  if (score >= 58) return 'Caution Advised — Significant Due Diligence Required';
  return 'Significant Concerns — High Risk Acquisition';
}

function generateOverallExplanation(score, dimensions, confidence, validation) {
  const strong = dimensions.filter(d => d.score >= 78).map(d => d.label);
  const weak = dimensions.filter(d => d.score < 55).map(d => d.label);

  let text = '';
  if (score >= 72) {
    text = 'The target demonstrates solid financial fundamentals suitable for acquisition.';
  } else if (score >= 58) {
    text = 'The target presents a mixed financial profile that warrants thorough due diligence.';
  } else {
    text = 'The target raises significant financial concerns that must be addressed before proceeding.';
  }

  if (strong.length > 0) text += ` Strengths: ${strong.join(', ')}.`;
  if (weak.length > 0) text += ` Concerns: ${weak.join(', ')}.`;
  if (confidence !== 'high') text += ` Note: overall confidence is ${confidence} due to incomplete document coverage.`;
  if (validation?.hardErrors?.length) {
    text += ` Validation flagged ${validation.hardErrors.length} hard issue(s) that should be resolved before relying on the score.`;
  } else if (validation?.warnings?.length) {
    text += ` Validation surfaced ${validation.warnings.length} warning(s) that warrant review.`;
  } else if (validation?.missingDataNotes?.length) {
    text += ' Validation coverage is partial because some supporting schedules were incomplete.';
  }

  return text;
}

// ---- Strengths ----

function extractStrengths(dimensions) {
  const strengths = [];

  for (const dim of dimensions) {
    if (dim.score >= 80 && dim.confidence !== 'none') {
      strengths.push({
        text: firstSentence(dim.explanation),
        dimension: dim.label,
        score: dim.score,
      });
    }
  }

  // Sort by score descending, cap at 5
  strengths.sort((a, b) => b.score - a.score);
  return strengths.slice(0, 5);
}

// ---- Risks ----

function extractRisks(dimensions, validation) {
  const risks = [];

  for (const dim of dimensions) {
    for (const flag of dim.flags) {
      const severity = dim.score < 45 ? 'high' : dim.score < 60 ? 'medium' : 'low';
      risks.push({ severity, text: flag, dimension: dim.label });
    }
  }

  for (const finding of validation?.hardErrors || []) {
    risks.push({
      severity: 'high',
      text: finding.message,
      dimension: 'Data Validation',
    });
  }

  for (const finding of validation?.warnings || []) {
    risks.push({
      severity: 'medium',
      text: finding.message,
      dimension: 'Data Validation',
    });
  }

  // Sort: high first, then medium, then low
  const order = { high: 0, medium: 1, low: 2 };
  risks.sort((a, b) => order[a.severity] - order[b.severity]);
  return risks;
}

// ---- Missing Diligence Items ----

function extractMissingItems(dimensions, documentTypes, validation) {
  const items = [];
  const foundTypes = new Set(documentTypes || []);

  const docRequirements = [
    { type: DOC_TYPES.INCOME_STATEMENT,    label: 'Income Statement / P&L',    impact: 'critical', affects: 'Profitability, revenue stability, leverage ratios' },
    { type: DOC_TYPES.BALANCE_SHEET,       label: 'Balance Sheet',             impact: 'critical', affects: 'Liquidity, leverage, working capital analysis' },
    { type: DOC_TYPES.CASH_FLOW_STATEMENT, label: 'Cash Flow Statement',       impact: 'high',     affects: 'Cash conversion quality, free cash flow analysis' },
    { type: DOC_TYPES.QOE_REPORT,          label: 'Quality of Earnings Report', impact: 'high',    affects: 'Earnings quality, add-back validation' },
    { type: DOC_TYPES.DEBT_SCHEDULE,       label: 'Debt Schedule',             impact: 'high',     affects: 'Leverage analysis, debt service capacity' },
    { type: DOC_TYPES.REVENUE_BREAKDOWN,   label: 'Customer Revenue Breakdown', impact: 'high',    affects: 'Customer concentration risk assessment' },
    { type: DOC_TYPES.TAX_RETURN,          label: 'Tax Returns',               impact: 'medium',   affects: 'Revenue validation, entity structure' },
    { type: DOC_TYPES.PROJECTIONS,         label: 'Management Projections',    impact: 'medium',   affects: 'Forecast credibility evaluation' },
    { type: DOC_TYPES.AR_AGING,            label: 'AR Aging Schedule',         impact: 'medium',   affects: 'Working capital quality, collection risk' },
    { type: DOC_TYPES.AP_AGING,            label: 'AP Aging Schedule',         impact: 'low',      affects: 'Vendor dependency, payment discipline' },
  ];

  for (const req of docRequirements) {
    if (!foundTypes.has(req.type)) {
      items.push({
        text: `${req.label} not provided`,
        impact: req.impact,
        affects: req.affects,
      });
    }
  }

  // Add scorer-driven items (from low-confidence dimensions)
  for (const dim of dimensions) {
    if (dim.confidence === 'none') {
      items.push({
        text: `Data unavailable for ${dim.label} assessment`,
        impact: 'high',
        affects: dim.label,
      });
    }
  }

  for (const note of validation?.missingDataNotes || []) {
    items.push({
      text: note.message,
      impact: note.impact || 'medium',
      affects: note.docType ? (DOC_TYPE_LABELS[note.docType] || note.docType) : 'Validation coverage',
    });
  }

  // Deduplicate by text
  const seen = new Set();
  return items.filter(i => {
    if (seen.has(i.text)) return false;
    seen.add(i.text);
    return true;
  });
}

// ---- Backward-compat risk flags for existing dashboard ----

function buildRiskFlags(risks) {
  return risks.map(r => ({
    severity: r.severity,
    text: r.text,
  }));
}

// ---- Format dimensions for dashboard sub-score cards ----

export function formatDimensionsForDashboard(dimensions) {
  return dimensions.map(d => ({
    key: d.key,
    label: d.label,
    score: d.score,
    note: firstSentence(d.explanation),
    confidence: d.confidence,
    explanation: d.explanation,
    metrics: d.metrics,
    logic: d.logic,
  }));
}

// ---- Helpers ----

/** Extract the first real sentence, avoiding splits on decimal numbers. */
function firstSentence(text) {
  if (!text) return '';
  // Match a period followed by a space and uppercase letter
  const match = text.match(/^(.+?\.)\s+(?=[A-Z])/);
  const result = match ? match[1] : text.split('. ')[0];
  return result.endsWith('.') ? result : result + '.';
}

function applyValidationConfidence(baseConfidence, adjustment) {
  const levels = ['none', 'low', 'medium', 'high'];
  const currentIndex = levels.indexOf(baseConfidence);
  if (currentIndex === -1 || !adjustment) return baseConfidence;

  const penalty = Math.abs(adjustment.delta || 0);
  const downgradeSteps = penalty >= 0.28 ? 2 : penalty >= 0.08 ? 1 : 0;
  return levels[Math.max(0, currentIndex - downgradeSteps)];
}
