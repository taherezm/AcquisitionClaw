import { DOC_TYPES, DOC_TYPE_LABELS } from './schemas.js';

const MAX_CONFIDENCE_PENALTY = 0.45;

export function validateFinancialData({ byType, financialModel }) {
  const warnings = [];
  const hardErrors = [];
  const missingDataNotes = [];
  const adjustmentReasons = [];
  let confidencePenalty = 0;

  const context = {
    byType,
    financialModel,
    warnings,
    hardErrors,
    missingDataNotes,
    penalize(amount, reason) {
      confidencePenalty = round(Math.min(MAX_CONFIDENCE_PENALTY, confidencePenalty + amount));
      adjustmentReasons.push(reason);
    },
    addWarning(code, message, meta = {}, penalty = 0.06) {
      warnings.push({ code, severity: 'warning', message, ...meta });
      this.penalize(penalty, message);
    },
    addHardError(code, message, meta = {}, penalty = 0.12) {
      hardErrors.push({ code, severity: 'hard_error', message, ...meta });
      this.penalize(penalty, message);
    },
    addMissingNote(message, impact = 'medium', meta = {}, penalty = 0.03) {
      missingDataNotes.push({ message, impact, ...meta });
      this.penalize(penalty, message);
    },
  };

  validateBalanceSheetEquation(context);
  validateSubtotalReasonableness(context);
  validateAgingVsBalance(context, DOC_TYPES.AR_AGING, 'totalAR', 'accountsReceivable', 'ar_aging_vs_balance');
  validateAgingVsBalance(context, DOC_TYPES.AP_AGING, 'totalAP', 'accountsPayable', 'ap_aging_vs_balance');
  validateDebtConsistency(context);
  validateEbitdaReasonableness(context);

  return {
    status: deriveValidationStatus({ warnings, hardErrors, missingDataNotes }),
    warnings,
    hardErrors,
    missingDataNotes,
    confidenceAdjustment: {
      delta: round(-confidencePenalty),
      magnitude: getAdjustmentMagnitude(confidencePenalty),
      reasons: dedupeStrings(adjustmentReasons),
    },
    summary: summarizeValidation({ warnings, hardErrors, missingDataNotes, confidencePenalty }),
  };
}

function validateBalanceSheetEquation(context) {
  const balanceSheet = context.byType[DOC_TYPES.BALANCE_SHEET];
  if (!balanceSheet) {
    context.addMissingNote('Balance sheet not available; unable to validate the accounting equation.', 'high', {
      docType: DOC_TYPES.BALANCE_SHEET,
      label: DOC_TYPE_LABELS[DOC_TYPES.BALANCE_SHEET],
    });
    return;
  }

  let checkedPeriods = 0;

  for (const period of balanceSheet.periods || []) {
    const periodData = balanceSheet.data?.[period];
    if (!periodData) continue;

    const assets = toNumber(periodData.totalAssets);
    const liabilities = toNumber(periodData.totalLiabilities);
    const equity = toNumber(periodData.equity);

    if (![assets, liabilities, equity].every(isFiniteNumber)) {
      context.addMissingNote(`Balance sheet equation could not be tested for ${period}; total assets, liabilities, or equity is missing.`, 'medium', {
        docType: DOC_TYPES.BALANCE_SHEET,
        period,
      }, 0.02);
      continue;
    }

    checkedPeriods += 1;
    const delta = assets - (liabilities + equity);
    const deltaPct = relativeDifference(assets, liabilities + equity);

    if (deltaPct > 0.08) {
      context.addHardError(
        'balance_sheet_equation',
        `${period} balance sheet does not foot: assets differ from liabilities plus equity by ${formatCurrency(delta)} (${formatPercent(deltaPct)}).`,
        { docType: DOC_TYPES.BALANCE_SHEET, period }
      );
    } else if (deltaPct > 0.03) {
      context.addWarning(
        'balance_sheet_equation',
        `${period} balance sheet is slightly out of balance by ${formatCurrency(delta)} (${formatPercent(deltaPct)}).`,
        { docType: DOC_TYPES.BALANCE_SHEET, period },
        0.05
      );
    }
  }

  if (checkedPeriods === 0) {
    context.addMissingNote('Balance sheet was provided but lacked enough usable totals to test the accounting equation.', 'medium', {
      docType: DOC_TYPES.BALANCE_SHEET,
    }, 0.04);
  }
}

function validateSubtotalReasonableness(context) {
  const incomeStatement = context.byType[DOC_TYPES.INCOME_STATEMENT];
  if (incomeStatement) {
    let anyIncomeCheck = false;

    for (const period of incomeStatement.periods || []) {
      const periodData = incomeStatement.data?.[period];
      if (!periodData) continue;

      const revenue = toNumber(periodData.revenue);
      const cogs = toNumber(periodData.cogs);
      const grossProfit = toNumber(periodData.grossProfit);

      if ([revenue, cogs, grossProfit].every(isFiniteNumber)) {
        anyIncomeCheck = true;
        const expectedGrossProfit = revenue - cogs;
        const deltaPct = relativeDifference(expectedGrossProfit, grossProfit);

        if (deltaPct > 0.1) {
          context.addHardError(
            'gross_profit_subtotal',
            `${period} gross profit does not reconcile to revenue less COGS within a reasonable tolerance.`,
            { docType: DOC_TYPES.INCOME_STATEMENT, period }
          );
        } else if (deltaPct > 0.04) {
          context.addWarning(
            'gross_profit_subtotal',
            `${period} gross profit is directionally inconsistent with revenue less COGS.`,
            { docType: DOC_TYPES.INCOME_STATEMENT, period },
            0.05
          );
        }

        if (grossProfit > revenue * 1.02) {
          context.addHardError(
            'gross_profit_exceeds_revenue',
            `${period} gross profit exceeds revenue, which indicates a likely mapping or subtotal issue.`,
            { docType: DOC_TYPES.INCOME_STATEMENT, period }
          );
        }
      }
    }

    if (!anyIncomeCheck) {
      context.addMissingNote('Income statement subtotals could not be fully reconciled because revenue, COGS, or gross profit was missing.', 'medium', {
        docType: DOC_TYPES.INCOME_STATEMENT,
      }, 0.03);
    }
  }

  const balanceSheet = context.byType[DOC_TYPES.BALANCE_SHEET];
  if (balanceSheet) {
    let anyBalanceCheck = false;

    for (const period of balanceSheet.periods || []) {
      const periodData = balanceSheet.data?.[period];
      if (!periodData) continue;

      const currentAssets = toNumber(periodData.totalCurrentAssets);
      const totalAssets = toNumber(periodData.totalAssets);
      const currentLiabilities = toNumber(periodData.totalCurrentLiabilities);
      const totalLiabilities = toNumber(periodData.totalLiabilities);

      if (isFiniteNumber(currentAssets) && isFiniteNumber(totalAssets)) {
        anyBalanceCheck = true;
        if (currentAssets > totalAssets * 1.02) {
          context.addHardError(
            'current_assets_exceed_total_assets',
            `${period} current assets exceed total assets, which is not internally consistent.`,
            { docType: DOC_TYPES.BALANCE_SHEET, period }
          );
        }
      }

      if (isFiniteNumber(currentLiabilities) && isFiniteNumber(totalLiabilities)) {
        anyBalanceCheck = true;
        if (currentLiabilities > totalLiabilities * 1.02) {
          context.addHardError(
            'current_liabilities_exceed_total_liabilities',
            `${period} current liabilities exceed total liabilities, which indicates a subtotal issue.`,
            { docType: DOC_TYPES.BALANCE_SHEET, period }
          );
        }
      }
    }

    if (!anyBalanceCheck) {
      context.addMissingNote('Balance sheet subtotal checks were skipped because current and total balances were incomplete.', 'low', {
        docType: DOC_TYPES.BALANCE_SHEET,
      }, 0.02);
    }
  }

  const cashFlow = context.byType[DOC_TYPES.CASH_FLOW_STATEMENT];
  if (cashFlow) {
    let anyCashFlowCheck = false;

    for (const period of cashFlow.periods || []) {
      const periodData = cashFlow.data?.[period];
      if (!periodData) continue;

      const operatingCashFlow = toNumber(periodData.operatingCashFlow);
      const capex = toNumber(periodData.capex);
      const freeCashFlow = toNumber(periodData.freeCashFlow);

      if ([operatingCashFlow, capex, freeCashFlow].every(isFiniteNumber)) {
        anyCashFlowCheck = true;
        const expectedFreeCashFlow = operatingCashFlow + (capex < 0 ? capex : -capex);
        const deltaPct = relativeDifference(expectedFreeCashFlow, freeCashFlow);

        if (deltaPct > 0.1) {
          context.addWarning(
            'free_cash_flow_subtotal',
            `${period} free cash flow does not closely reconcile to operating cash flow less capex.`,
            { docType: DOC_TYPES.CASH_FLOW_STATEMENT, period },
            0.05
          );
        }
      }
    }

    if (!anyCashFlowCheck) {
      context.addMissingNote('Cash flow subtotal checks were limited because operating cash flow, capex, or free cash flow was missing.', 'low', {
        docType: DOC_TYPES.CASH_FLOW_STATEMENT,
      }, 0.02);
    }
  }
}

function validateAgingVsBalance(context, agingDocType, agingField, balanceField, codePrefix) {
  const aging = context.byType[agingDocType];
  if (!aging) return;

  const balanceSheet = context.byType[DOC_TYPES.BALANCE_SHEET];
  if (!balanceSheet) {
    context.addMissingNote(`${DOC_TYPE_LABELS[agingDocType]} was provided, but no balance sheet was available for reconciliation.`, 'medium', {
      docType: agingDocType,
      relatedDocType: DOC_TYPES.BALANCE_SHEET,
    }, 0.03);
    return;
  }

  const agingTotal = toNumber(aging.data?._single?.[agingField]);
  const latestPeriod = getLatestPeriod(balanceSheet);
  const balanceTotal = toNumber(latestPeriod ? balanceSheet.data?.[latestPeriod]?.[balanceField] : null);

  if (!isFiniteNumber(agingTotal) || !isFiniteNumber(balanceTotal)) {
    context.addMissingNote(`${DOC_TYPE_LABELS[agingDocType]} could not be reconciled to the balance sheet because one side of the comparison was missing.`, 'medium', {
      docType: agingDocType,
      relatedDocType: DOC_TYPES.BALANCE_SHEET,
      period: latestPeriod || null,
    }, 0.03);
    return;
  }

  const deltaPct = relativeDifference(agingTotal, balanceTotal);
  const label = agingDocType === DOC_TYPES.AR_AGING ? 'AR aging total' : 'AP aging total';

  if (deltaPct > 0.5) {
    context.addHardError(
      codePrefix,
      `${label} differs materially from the balance sheet balance (${formatPercent(deltaPct)} variance).`,
      { docType: agingDocType, relatedDocType: DOC_TYPES.BALANCE_SHEET, period: latestPeriod }
    );
  } else if (deltaPct > 0.15) {
    context.addWarning(
      codePrefix,
      `${label} does not closely match the balance sheet balance (${formatPercent(deltaPct)} variance).`,
      { docType: agingDocType, relatedDocType: DOC_TYPES.BALANCE_SHEET, period: latestPeriod },
      0.05
    );
  }
}

function validateDebtConsistency(context) {
  const debtSchedule = context.byType[DOC_TYPES.DEBT_SCHEDULE];
  if (!debtSchedule) return;

  const balanceSheet = context.byType[DOC_TYPES.BALANCE_SHEET];
  if (!balanceSheet) {
    context.addMissingNote('Debt schedule was provided, but no balance sheet was available to reconcile reported debt.', 'medium', {
      docType: DOC_TYPES.DEBT_SCHEDULE,
      relatedDocType: DOC_TYPES.BALANCE_SHEET,
    }, 0.03);
    return;
  }

  const scheduleDebt = toNumber(debtSchedule.data?._single?.totalDebt);
  const latestPeriod = getLatestPeriod(balanceSheet);
  const balanceData = latestPeriod ? balanceSheet.data?.[latestPeriod] : null;
  const balanceDebt = isFiniteNumber(toNumber(balanceData?.currentPortionLTD)) || isFiniteNumber(toNumber(balanceData?.longTermDebt))
    ? round((toNumber(balanceData?.currentPortionLTD) || 0) + (toNumber(balanceData?.longTermDebt) || 0))
    : null;

  if (!isFiniteNumber(scheduleDebt) || !isFiniteNumber(balanceDebt)) {
    context.addMissingNote('Debt schedule could not be reconciled to balance sheet debt because one side of the comparison was incomplete.', 'medium', {
      docType: DOC_TYPES.DEBT_SCHEDULE,
      relatedDocType: DOC_TYPES.BALANCE_SHEET,
      period: latestPeriod || null,
    }, 0.03);
    return;
  }

  const deltaPct = relativeDifference(scheduleDebt, balanceDebt);
  if (deltaPct > 0.4) {
    context.addHardError(
      'debt_schedule_vs_balance_sheet',
      `Debt schedule total differs materially from balance sheet debt (${formatPercent(deltaPct)} variance).`,
      { docType: DOC_TYPES.DEBT_SCHEDULE, relatedDocType: DOC_TYPES.BALANCE_SHEET, period: latestPeriod }
    );
  } else if (deltaPct > 0.12) {
    context.addWarning(
      'debt_schedule_vs_balance_sheet',
      `Debt schedule total does not closely match balance sheet debt (${formatPercent(deltaPct)} variance).`,
      { docType: DOC_TYPES.DEBT_SCHEDULE, relatedDocType: DOC_TYPES.BALANCE_SHEET, period: latestPeriod },
      0.05
    );
  }
}

function validateEbitdaReasonableness(context) {
  const incomeStatement = context.byType[DOC_TYPES.INCOME_STATEMENT];
  if (!incomeStatement) {
    context.addMissingNote('Income statement not available; EBITDA reasonableness checks were skipped.', 'high', {
      docType: DOC_TYPES.INCOME_STATEMENT,
    });
    return;
  }

  let checkedPeriods = 0;

  for (const period of incomeStatement.periods || []) {
    const periodData = incomeStatement.data?.[period];
    if (!periodData) continue;

    const revenue = toNumber(periodData.revenue);
    const grossProfit = toNumber(periodData.grossProfit);
    const ebitda = toNumber(periodData.ebitda);

    if (!isFiniteNumber(revenue) || !isFiniteNumber(ebitda)) {
      context.addMissingNote(`EBITDA reasonableness for ${period} could not be tested because revenue or EBITDA was missing.`, 'low', {
        docType: DOC_TYPES.INCOME_STATEMENT,
        period,
      }, 0.02);
      continue;
    }

    checkedPeriods += 1;
    const ebitdaMargin = revenue !== 0 ? ebitda / revenue : null;

    if (isFiniteNumber(ebitdaMargin) && ebitdaMargin > 0.6) {
      context.addHardError(
        'ebitda_vs_revenue',
        `${period} EBITDA margin of ${formatPercent(ebitdaMargin)} is unusually high and likely requires mapping review.`,
        { docType: DOC_TYPES.INCOME_STATEMENT, period }
      );
    } else if (isFiniteNumber(ebitdaMargin) && ebitdaMargin < -0.35) {
      context.addWarning(
        'ebitda_vs_revenue',
        `${period} EBITDA margin of ${formatPercent(ebitdaMargin)} is deeply negative and should be reviewed for normalization issues.`,
        { docType: DOC_TYPES.INCOME_STATEMENT, period },
        0.05
      );
    }

    if (isFiniteNumber(grossProfit) && ebitda > grossProfit * 1.35) {
      context.addHardError(
        'ebitda_vs_gross_profit',
        `${period} EBITDA exceeds gross profit by a wide margin, which is unlikely without mapping error or non-operating offsets.`,
        { docType: DOC_TYPES.INCOME_STATEMENT, period }
      );
    } else if (isFiniteNumber(grossProfit) && ebitda > grossProfit * 1.1) {
      context.addWarning(
        'ebitda_vs_gross_profit',
        `${period} EBITDA is above gross profit; review operating expense mapping and sign conventions.`,
        { docType: DOC_TYPES.INCOME_STATEMENT, period },
        0.05
      );
    }
  }

  if (checkedPeriods === 0) {
    context.addMissingNote('EBITDA reasonableness checks were skipped because no usable income statement periods were available.', 'medium', {
      docType: DOC_TYPES.INCOME_STATEMENT,
    }, 0.04);
  }
}

function deriveValidationStatus({ warnings, hardErrors, missingDataNotes }) {
  if (hardErrors.length > 0) return 'hard-error';
  if (warnings.length > 0) return 'review';
  if (missingDataNotes.length > 0) return 'partial';
  return 'validated';
}

function summarizeValidation({ warnings, hardErrors, missingDataNotes, confidencePenalty }) {
  if (hardErrors.length > 0) {
    return `Validation found ${hardErrors.length} hard error(s) and ${warnings.length} warning(s). Confidence adjusted by ${formatSignedPercent(-confidencePenalty)}.`;
  }
  if (warnings.length > 0) {
    return `Validation found ${warnings.length} warning(s). Confidence adjusted by ${formatSignedPercent(-confidencePenalty)}.`;
  }
  if (missingDataNotes.length > 0) {
    return `Validation completed with limited data. Confidence adjusted by ${formatSignedPercent(-confidencePenalty)}.`;
  }
  return 'Validation checks passed without material issues.';
}

function getAdjustmentMagnitude(confidencePenalty) {
  if (confidencePenalty >= 0.28) return 'heavy';
  if (confidencePenalty >= 0.12) return 'moderate';
  if (confidencePenalty > 0) return 'light';
  return 'none';
}

function getLatestPeriod(balanceSheet) {
  const periods = (balanceSheet?.periods || []).filter((period) => period !== '_single' && balanceSheet.data?.[period]);
  return periods.length > 0 ? periods[periods.length - 1] : null;
}

function relativeDifference(left, right) {
  const a = toNumber(left);
  const b = toNumber(right);
  if (!isFiniteNumber(a) || !isFiniteNumber(b)) return 0;
  return Math.abs(a - b) / Math.max(Math.abs(a), Math.abs(b), 1);
}

function toNumber(value) {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

function dedupeStrings(values) {
  return [...new Set(values.filter(Boolean))];
}

function formatCurrency(value) {
  const amount = toNumber(value);
  if (!isFiniteNumber(amount)) return '$0';
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    maximumFractionDigits: 0,
  }).format(amount);
}

function formatPercent(value) {
  const amount = toNumber(value);
  if (!isFiniteNumber(amount)) return '0%';
  return `${round(amount * 100)}%`;
}

function formatSignedPercent(value) {
  const amount = toNumber(value);
  if (!isFiniteNumber(amount)) return '0%';
  const pct = round(amount * 100);
  return `${pct > 0 ? '+' : ''}${pct}%`;
}

function round(value) {
  return Math.round(value * 100) / 100;
}
