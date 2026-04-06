import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { expect, test } from '@playwright/test';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '..', '..');

const demoPackFiles = [
  'income_statement.csv',
  'balance_sheet.csv',
  'cash_flow_statement.csv',
  'tax_return.csv',
  'qoe_report.csv',
  'projections.csv',
  'ar_aging.csv',
  'ap_aging.csv',
  'debt_schedule.csv',
  'revenue_breakdown.csv',
].map((filename) => path.join(projectRoot, 'mock-data', 'client-demo-pack', filename));

test.describe('reviewer memory and evidence ranking', () => {
  test('analyzes the demo pack end to end without crashing', async ({ page }) => {
    const scope = buildScope('E2E Smoke Co', 'Smoke Deal');
    await cleanupPersistedReviewMemory(scope);
    await page.goto('/');
    await clearBrowserStorage(page);

    await runAnalysis(page, scope);

    await expect(page.locator('#dash-company-name')).toHaveText(scope.companyName);
    await expect(page.locator('[data-evidence-field="revenue"]')).toHaveAttribute(
      'data-evidence-selected-doc-type',
      'income_statement',
    );
    await expect(page.locator('[data-evidence-field="ebitda"]')).toHaveAttribute(
      'data-evidence-selected-doc-type',
      'qoe_report',
    );
    await expect.poll(async () => page.locator('#quality-ambiguity-workflows .quality-review-item').count()).toBeGreaterThan(0);

    await cleanupPersistedReviewMemory(scope);
  });

  test('persists a preferred source across reload and reapplies it on rerun', async ({ page }) => {
    const scope = buildScope('E2E Reviewer Memory Co', 'Reviewer Memory Deal');
    await cleanupPersistedReviewMemory(scope);
    await page.goto('/');
    await clearBrowserStorage(page);

    await runAnalysis(page, scope);

    const revenueField = page.locator('[data-evidence-field="revenue"]');
    await expect(revenueField).toHaveAttribute('data-evidence-selected-doc-type', 'income_statement');

    await page.locator('[data-source-preference-select="revenue"]').selectOption('tax_return');
    await page.locator('[data-source-preference-save="revenue"]').click();

    await expect(revenueField).toHaveAttribute('data-evidence-selected-doc-type', 'tax_return');
    await expect(page.locator('[data-reviewer-source-preference="revenue"]')).toHaveAttribute(
      'data-preferred-doc-type',
      'tax_return',
    );

    await clearBrowserStorage(page);
    await page.reload();
    await runAnalysis(page, scope, { navigate: false });

    await expect(page.locator('[data-evidence-field="revenue"]')).toHaveAttribute(
      'data-evidence-selected-doc-type',
      'tax_return',
    );
    await expect(page.locator('[data-reviewer-source-preference="revenue"]')).toHaveAttribute(
      'data-preferred-doc-type',
      'tax_return',
    );

    const persistedMemoryPath = path.join(getReviewMemoryDir(scope), 'state.json');
    const persistedMemory = JSON.parse(await fs.readFile(persistedMemoryPath, 'utf8'));
    expect(persistedMemory.sourcePreferences).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          conceptKey: 'revenue',
          preferredDocType: 'tax_return',
        }),
      ]),
    );

    await cleanupPersistedReviewMemory(scope);
  });
});

async function runAnalysis(page, scope, options = {}) {
  if (options.navigate !== false) {
    await page.goto('/');
  }

  await page.locator('#company-name').fill(scope.companyName);
  await page.locator('#deal-name').fill(scope.dealName);
  await page.locator('#reviewer-id').fill(scope.reviewerId);
  await page.locator('#file-input').setInputFiles(demoPackFiles);
  await expect(page.locator('#analyze-btn')).toBeEnabled();
  await page.locator('#analyze-btn').click();

  await expect(page.locator('#view-dashboard.active')).toBeVisible();
  await expect(page.locator('[data-evidence-field="revenue"]')).toBeVisible();
}

async function clearBrowserStorage(page) {
  await page.evaluate(() => {
    window.localStorage.clear();
    window.sessionStorage.clear();
  });
}

async function cleanupPersistedReviewMemory(scope) {
  await fs.rm(getReviewMemoryDir(scope), { recursive: true, force: true });
}

function getReviewMemoryDir(scope) {
  return path.join(
    projectRoot,
    '.data',
    'review-memory',
    slugify(scope.companyName),
    slugify(scope.dealName),
  );
}

function buildScope(companyName, dealName, reviewerId = 'e2e-reviewer') {
  return {
    companyName,
    dealName,
    reviewerId,
  };
}

function slugify(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'default';
}
