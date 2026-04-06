# AcquisitionClaw

AcquisitionClaw is a buy-side financial analysis app for search fund and small-cap acquisition workflows. It combines a browser-based dashboard with a local Node/Express ingestion service that accepts uploaded financial files, parses them deterministically, maps them into a normalized model, validates the resulting financials, and drives scoring, charts, growth opportunities, and acquisition recommendations.

## Current Status

This repo is no longer a static demo only.

The current build supports:

- browser uploads wired to a real backend at `/api/ingest`
- multipart upload handling with in-memory file processing
- deterministic parsing for `.csv`, `.xlsx`, and text-readable `.pdf`
- page-level PDF provenance with OCR fallback for image-only pages
- intra-sheet section splitting for mixed workbooks / combined exports
- sheet-aware document classification
- schema mapping into normalized financial objects
- learned alias reuse from manual review mappings
- reviewer-memory ranking signals from persisted map / ignore decisions
- local filesystem persistence for company/deal-scoped reviewer memory with revisions and audit history
- explicit reviewer actions for preferred sources, source suppressions, time-basis overrides, and entity confirmation
- cross-document reconciliation and period / unit conflict detection
- assumption ledger and confidence-lift recommendations in the dashboard
- benchmark fixtures and a local regression runner for clean, ugly, and preferred-source search-fund packs
- calibration metrics for labeled evidence selections, false-high-confidence cases, and reviewer lift
- browser regression coverage for upload, evidence ranking, and persisted reviewer-memory flows
- GitHub Actions CI for syntax, benchmarks, and browser regression
- pre-scoring validation and confidence adjustment
- dashboard outputs driven by real normalized data when available
- demo/synthetic fallback preserved only when explicitly enabled in code

The current v1 intentionally does not support:

- production-grade OCR accuracy on very poor scans or handwritten/image-heavy PDFs
- production-grade shared database storage or authenticated user accounts
- user accounts or multi-user workflows
- deal history, versioning, or audit storage

## What The App Does Today

Uploaded files move through this pipeline:

1. upload to `/api/ingest`
2. file-type validation
3. deterministic parsing
4. heuristic document classification
5. schema mapping / extraction
6. normalization into the app’s internal financial model
7. validation of mapped financials
8. scoring, charting, growth opportunity generation, and acquisition recommendation output

If uploaded data maps cleanly, the dashboard uses the uploaded values directly.

If uploaded data is incomplete or weakly mapped, the app now lowers confidence and surfaces missing-data and validation notes instead of silently inventing production values.

## Supported Inputs

### File formats

- `.csv`
- `.xlsx`
- `.pdf`

### Supported document types

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
- unknown

## Local Run

From the project root:

```bash
npm install
npm start
```

Open:

```text
http://localhost:8080
```

For development with file watching:

```bash
npm run dev
```

To run the interpretability benchmark fixtures:

```bash
npm run benchmark
```

To run the browser regression suite:

```bash
npm run test:e2e
```

## Frontend Behavior

The browser flow is:

- upload files in the UI
- scope reviewer memory by company, deal, and reviewer id
- send them with `FormData` to `/api/ingest`
- receive structured JSON per file
- store the ingestion result client-side
- pass normalized backend-driven data into the scoring pipeline
- render score, charts, underwriting notes, growth opportunities, and acquisition advice

The dashboard now also shows:

- extraction confidence
- validation status and warnings
- hard validation errors when present
- missing-data notes
- ranked evidence conflicts with reviewer actions
- temporal override controls and entity-confirmation controls
- reviewer-memory history and revision state

## Backend API

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

### `GET /api/review-memory?companyName=...&dealName=...&reviewerId=...`

Loads persisted reviewer memory for a company/deal scope. The backend stores this locally under `.data/review-memory/<company>/<deal>/`.

### `PUT /api/review-memory`

Persists reviewer memory for a company/deal scope using optimistic revision checks.

Request body:

- `companyName`
- `dealName`
- `reviewerId`
- `expectedRevision`
- `reviewOverrides`
- `sourcePreferences`
- `conceptSuppressions`
- `timeBasisOverrides`
- `entityResolutions`

### Example

```bash
curl -X POST \
  -F "files=@/path/to/income_statement.csv" \
  -F "files=@/path/to/debt_schedule.xlsx" \
  -F "companyName=Acme Manufacturing" \
  -F "industry=Manufacturing" \
  -F "ebitdaRange=1m-3m" \
  http://localhost:8080/api/ingest
```

### Response shape

The response includes:

- request metadata
- upload summary
- one structured result per file

Each file result currently includes:

- `file`
- `validation`
- `parsing`
- `classification`
- `extraction`
- `normalization`

## Parsing, Classification, Mapping, Validation

### Parsing

The backend parser is deterministic and modular.

- CSV files are converted into structured rows.
- Excel files are read workbook-by-workbook and sheet-by-sheet.
- PDF files are read page-by-page into structured pseudo-sheets with page provenance.
- PDF text extraction now attempts multi-column reading order reconstruction, footnote capture, and table-block recovery before falling back to OCR.
- Excel and PDF outputs can be split into section-level candidate documents when one source contains multiple statements.
- Excel outputs include sheet names, rows, columns, and structured records per sheet.

### Classification

Classification is deterministic for v1 and uses heuristics from:

- filename
- sheet name
- header keywords

Each sheet can map to a different document type, with:

- document type
- confidence score
- manual-review warnings for weak classifications

### Schema mapping

Mapped outputs normalize common financial label variations, including:

- `Revenue / Net Sales / Total Sales`
- `COGS / Cost of Revenue`
- `EBITDA / Operating Profit + D&A`
- `A/R / Accounts Receivable`
- `A/P / Accounts Payable`
- `SG&A / Selling, General & Administrative`

Interpretability now also includes:

- ambiguous row detection
- low-confidence heuristic match surfacing
- learned alias reuse from manual review
- explicit preferred-source and suppression controls in the evidence resolver
- reviewer time-basis overrides for ambiguous FY/LTM/snapshot interpretation
- reviewer-confirmed entity alias clustering
- page / sheet / section source metadata
- cross-document reconciliation findings

Supported normalized outputs include:

- income statement
- balance sheet
- cash flow
- AR aging
- AP aging
- debt schedule
- revenue concentration
- projections
- tax return
- QoE summary

### Validation

Validation runs before scoring and is designed to degrade confidence rather than fail unnecessarily when data is incomplete.

Current checks include:

- balance sheet equation consistency
- subtotal reasonableness
- AR aging total vs AR balance comparison
- AP aging total vs AP balance comparison
- debt schedule total vs balance sheet debt comparison
- EBITDA reasonableness relative to revenue and gross profit
- period granularity conflicts inside a document
- cross-document scale mismatch detection
- revenue vs tax-return gross receipts reconciliation
- income statement EBITDA vs QoE adjusted EBITDA reconciliation

Validation returns:

- `status`
- `warnings`
- `hardErrors`
- `missingDataNotes`
- `confidenceAdjustment`

## Scoring And Dashboard Outputs

The scoring engine uses normalized financial data to generate:

- overall health score
- dimension-level sub-scores
- strengths
- risks
- missing diligence items
- chart data
- investment summary
- acquisition advice
- growth opportunities
- next steps

Real uploaded data is now the primary source for these outputs. Synthetic fallback is no longer used in the normal upload flow.

## Demo / Fallback Mode

Synthetic fallback still exists for demo/testing, but it is opt-in.

The upload flow in `app.js` currently sets:

```js
allowDemoFallback: false
```

That means:

- production-style browser use does not silently backfill missing documents with synthetic numbers
- weak or incomplete uploads are surfaced as lower-confidence results
- demo fallback remains available to developers if they explicitly enable it in code

## Project Structure

```text
backend/
  app.js
  server.js
  routes/
    ingestRoutes.js
  middleware/
    uploadMiddleware.js
  controllers/
    ingestController.js
  services/
    parsing/
    classification/
    mapping/
    extraction/
    normalization/
    validation/
  utils/

ingestion/
  classifier.js
  extractor.js
  normalizer.js
  pipeline.js
  schemas.js
  validator.js

api.js
app.js
charts.js
index.html
styles.css
```

## Key Files

- `backend/app.js`: Express app and static file serving
- `backend/routes/ingestRoutes.js`: `/api/ingest` route
- `backend/controllers/ingestController.js`: backend ingestion orchestration
- `backend/services/parsing/*`: deterministic CSV/XLSX parsing
- `backend/services/classification/*`: heuristic document classification
- `backend/services/mapping/*`: schema mapping into normalized financial data
- `backend/services/extraction/extractionService.js`: backend extraction output
- `backend/services/normalization/normalizationService.js`: pipeline-ready normalized output
- `ingestion/pipeline.js`: frontend orchestration into dashboard-ready outputs
- `ingestion/normalizer.js`: financial model assembly and validation/scoring handoff
- `ingestion/validator.js`: pre-scoring financial validation
- `scoring/engine.js`: scoring and confidence handling
- `app.js`: UI flow and dashboard rendering

## Limitations In The Current Build

- no PDF parsing yet
- no OCR
- no persistence of uploads or analysis sessions
- no user-editable mapping overrides in the UI
- no explicit source-to-metric audit trail in the dashboard yet
- no scenario modeling or sensitivity analysis yet
- no lender-style covenant or debt-capacity package yet
- current scoring confidence is still heuristic and can be expanded

## v1.5 (Currently Building): Interpretability For Search Fund Acquisitions

The next version should focus less on adding raw features and more on making the outputs legible, defensible, and investment-committee ready.

### 1. Metric provenance and traceability

Add a source trail for every scored metric:

- metric -> normalized field -> document -> sheet -> row/column reference
- clickable “why this number” panels in the dashboard
- highlight whether a metric was direct, derived, or partially inferred

Why this matters:

- search fund buyers need to trust the number before trusting the recommendation
- it reduces black-box behavior during seller-file review and lender discussions

### 2. Score explainability by dimension

Expose exactly how each dimension score was computed:

- raw inputs used
- benchmark comparisons
- penalties applied
- confidence dampening applied
- validation findings affecting the dimension

Why this matters:

- buyers can distinguish a true business issue from a document-quality issue
- it makes the score defendable in IC memos and diligence meetings

### 3. Confidence decomposition

Break confidence into separate components:

- document coverage confidence
- extraction/mapping confidence
- validation confidence
- scoring confidence

Why this matters:

- “low confidence” is too blunt
- the buyer needs to know whether the problem is missing files, weak mapping, or economically inconsistent data

### 4. Buy-side deal thesis panels

Add structured narrative modules for:

- reasons to believe
- reasons to worry
- issues requiring confirmatory diligence
- likely LOI structure implications
- key seller follow-up questions

Why this matters:

- search-fund acquisitions are decision workflows, not only analytics workflows
- the app should help convert numbers into an actionable deal thesis

### 5. Working capital and QoE interpretability

Expand support for:

- normalized working capital bridge
- add-back classification by quality level
- seller-adjusted EBITDA vs buyer-adjusted EBITDA
- recurring vs non-recurring normalization labeling

Why this matters:

- this is where many small-company deals break
- buyers need to see what is operationally real versus negotiated adjustment logic

### 6. Debt and downside interpretability

Add lender-style and downside views:

- debt service sensitivity
- fixed charge / interest coverage stress
- downside EBITDA case
- covenant-style headroom indicators
- “what breaks first” summary

Why this matters:

- search-fund deals are financing constrained
- the recommendation should show fragility, not just base-case quality

### 7. Manual override and review workflow

Add a review layer so a buyer can:

- reclassify a sheet
- remap a label
- approve or reject derived metrics
- lock a preferred number for scoring

Why this matters:

- middle-market seller files are messy
- interpretability improves when the user can correct mappings without editing code

### 8. Board/IC-ready export layer

Generate an exportable package with:

- investment memo summary
- diligence flags
- score rationale
- confidence explanation
- source-document appendix

Why this matters:

- the end product for a search fund is usually a memo, lender pack, or diligence summary
- the app should close that last mile

## Recommended v1.5 Build Order

1. metric provenance and score explainability
2. confidence decomposition
3. manual override workflow
4. working capital / QoE interpretability
5. downside and debt-capacity views
6. IC memo export

## Suggested Near-Term Engineering Additions

- a source-reference object on every normalized field
- a score explanation payload per dimension
- a UI drawer for “why this score” and “where this number came from”
- override storage for document type and field mapping corrections
- a reusable diligence note model for carry-through from ingestion to recommendation output

## Summary

AcquisitionClaw is currently a local, backend-enabled v1 for deterministic ingestion and buy-side financial analysis. It already produces real dashboard outputs from uploaded `.csv` and `.xlsx` files when they map cleanly. The most important next step is not broader file support alone; it is interpretability, provenance, and decision support so a search-fund buyer can understand, defend, and act on the outputs.
