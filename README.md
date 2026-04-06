# AcquisitionClaw

AcquisitionClaw is a buy-side acquisition analysis app for search-fund and small-cap deal workflows. It ingests messy seller files, normalizes them into a structured financial model, ranks conflicting evidence across documents, and turns that into diligence-oriented scoring, charts, recommendations, and review workflows.

## Current Product Shape

This is no longer a static demo.

The current build includes:

- browser uploads wired to a real backend API
- deterministic ingestion for `.csv`, `.xlsx`, and text-readable `.pdf`
- OCR fallback for image-based PDF pages
- mixed-sheet and mixed-section splitting for ugly workbook exports
- document classification for common search-fund file types
- schema mapping into normalized financial objects
- cross-document evidence ranking and conflict resolution
- field-level confidence decomposition
- temporal alignment detection for FY, LTM, point-in-time, and similar date-basis conflicts
- entity resolution for repeated lenders, customers, and other renamed concepts across files
- reviewer-memory actions for source preferences, suppressions, time-basis overrides, and entity confirmation
- assumption ledger output and ambiguity-specific review workflows
- benchmark fixtures for clean packs, ugly packs, and preferred-source overrides
- browser regression coverage and CI

## Deployment

The deployed app is designed to run on Netlify:

- the frontend is published as static assets from `.netlify/public`
- the backend Express app runs behind Netlify Functions
- `/api/*` routes are rewritten to the Netlify function entrypoint

Relevant deployment files:

- [netlify.toml](/Users/tatoenahaisi/Downloads/AcquisitionClaw/netlify.toml)
- [netlify/functions/api.js](/Users/tatoenahaisi/Downloads/AcquisitionClaw/netlify/functions/api.js)
- [scripts/buildNetlifySite.js](/Users/tatoenahaisi/Downloads/AcquisitionClaw/scripts/buildNetlifySite.js)

## What The App Does

Uploaded files move through this pipeline:

1. file validation
2. parsing
3. document classification
4. schema mapping and extraction
5. normalization into the internal financial model
6. validation and reconciliation
7. evidence ranking and ambiguity handling
8. scoring, charting, diligence notes, and recommendation output

The app prefers real uploaded data. It no longer silently fills gaps with demo values during the normal upload flow.

## Supported Inputs

### File formats

- `.csv`
- `.xlsx`
- `.pdf`

### Common document types

- income statement / P&L
- balance sheet
- cash flow statement
- tax return summary
- QoE summary
- projections / forecast
- accounts receivable aging
- accounts payable aging
- debt schedule
- revenue breakdown / customer concentration
- unknown / mixed exports

## Interpretability Features

The current build is centered on ambiguity handling rather than just parsing.

### Evidence ranking

When multiple documents disagree, the app can rank candidates using:

- document family priors
- source quality and match confidence
- OCR vs native extraction quality
- temporal alignment
- reconciliation support
- reviewer memory

### Confidence decomposition

Confidence is not just one score. The app decomposes it across factors such as:

- label match strength
- source quality
- period alignment
- unit certainty
- reconciliation support
- reviewer support
- derived vs direct values

### Reviewer actions

The dashboard supports explicit ambiguity resolution:

- prefer one document family for a field
- suppress a noisy source for a concept
- override time basis
- confirm entity aliases

Reviewer memory is scoped by company, deal, and reviewer.

### Provenance and ambiguity surfaces

The dashboard exposes:

- evidence cards per key metric
- ambiguity workflows
- temporal conflicts
- entity clusters
- reviewer-memory history
- assumption ledger entries

## API

### `POST /api/ingest`

Accepts `multipart/form-data` with one or more `files` fields.

Optional metadata fields:

- `companyName`
- `dealName`
- `reviewerId`
- `industry`
- `ebitdaRange`

Optional reviewer-memory fields:

- `reviewOverrideRules`
- `reviewSourcePreferences`
- `reviewConceptSuppressions`
- `reviewTimeBasisOverrides`
- `reviewEntityResolutions`

### `GET /api/review-memory`

Query params:

- `companyName`
- `dealName`
- `reviewerId`

Loads persisted reviewer memory for the current scope.

### `PUT /api/review-memory`

Persists reviewer memory for the current scope with optimistic revision checks.

Request fields:

- `companyName`
- `dealName`
- `reviewerId`
- `expectedRevision`
- `reviewOverrides`
- `sourcePreferences`
- `conceptSuppressions`
- `timeBasisOverrides`
- `entityResolutions`

## Validation And Reconciliation

Validation runs before scoring and lowers confidence when data is incomplete or inconsistent.

Current checks include:

- balance-sheet consistency
- subtotal reasonableness
- AR aging vs balance-sheet AR
- AP aging vs balance-sheet AP
- debt-schedule totals vs stated debt
- revenue vs tax-return gross receipts
- EBITDA vs QoE adjusted EBITDA
- unit and scale mismatches
- period and granularity conflicts

## Testing And Verification

Useful project commands:

```bash
npm run build:netlify
npm run benchmark
npm run test:e2e
```

The benchmark runner covers:

- clean demo packs
- ugly search-fund style packs
- reviewer-memory source-preference fixtures

## Project Structure

```text
backend/
  app.js
  server.js
  controllers/
  middleware/
  routes/
  services/
    classification/
    extraction/
    mapping/
    normalization/
    parsing/
    review/
    validation/
    workbook/

ingestion/
  classifier.js
  evidenceResolver.js
  extractor.js
  normalizer.js
  pipeline.js
  reconciliation.js
  reviewOverrides.js
  schemas.js
  validator.js

netlify/
  functions/

scripts/
tests/
mock-data/
```

## Key Files

- [app.js](/Users/tatoenahaisi/Downloads/AcquisitionClaw/app.js): upload flow, dashboard rendering, reviewer actions
- [api.js](/Users/tatoenahaisi/Downloads/AcquisitionClaw/api.js): browser API client and runtime API-origin handling
- [backend/app.js](/Users/tatoenahaisi/Downloads/AcquisitionClaw/backend/app.js): Express app and API mounting
- [backend/controllers/ingestController.js](/Users/tatoenahaisi/Downloads/AcquisitionClaw/backend/controllers/ingestController.js): ingestion orchestration
- [backend/services/parsing/pdfParser.js](/Users/tatoenahaisi/Downloads/AcquisitionClaw/backend/services/parsing/pdfParser.js): PDF parsing, layout recovery, OCR fallback
- [backend/services/review/reviewMemoryService.js](/Users/tatoenahaisi/Downloads/AcquisitionClaw/backend/services/review/reviewMemoryService.js): scoped reviewer-memory persistence
- [ingestion/evidenceResolver.js](/Users/tatoenahaisi/Downloads/AcquisitionClaw/ingestion/evidenceResolver.js): conflict resolution, temporal reasoning, entity resolution, confidence breakdown
- [ingestion/reviewOverrides.js](/Users/tatoenahaisi/Downloads/AcquisitionClaw/ingestion/reviewOverrides.js): reviewer-memory normalization and ranking signals
- [scripts/runBenchmarks.js](/Users/tatoenahaisi/Downloads/AcquisitionClaw/scripts/runBenchmarks.js): regression and calibration runner

## Current Limitations

- OCR on poor scans is still weaker than native digital PDFs
- reviewer-memory persistence is durable in local Node mode, but Netlify preview storage is still serverless-temp and should be moved to a real shared store for production
- there are no authenticated multi-user accounts yet
- the app does not yet produce a formal IC memo or lender export package
- confidence is substantially better than earlier builds, but still heuristic rather than statistically calibrated against a large gold dataset
