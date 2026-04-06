# Client Demo Pack

This folder is designed for AcquisitionClaw demo calls.

## How to use it

1. Start the app backend:

```bash
npm start
```

2. Open:

```text
http://localhost:8080
```

3. In the upload screen:

- enter a company name
- pick `Manufacturing`
- optionally pick EBITDA range `3m-5m`
- drag all of the files from this folder into the drop zone

## Why this pack is split into separate files

The current v1 pipeline classifies multiple sheets inside a workbook, but it only promotes one normalized document per uploaded file into the scoring pipeline.

That means a single workbook with tabs for P&L, balance sheet, AR aging, debt schedule, and projections will not fully populate the dashboard yet.

For the current demo build, the best experience is:

- one file per document type
- clean filenames that match the classifier
- consistent row labels that match the mapper

## Files included

- `income_statement.csv`
- `balance_sheet.csv`
- `cash_flow_statement.csv`
- `tax_return.csv`
- `qoe_report.csv`
- `projections.csv`
- `ar_aging.csv`
- `ap_aging.csv`
- `debt_schedule.csv`
- `revenue_breakdown.csv`

## Expected demo outcome

Uploading all files should populate:

- strengths
- risks
- financial dimensions
- data quality & validation
- validation findings
- missing diligence items
- risk flags
- analytics studio
- charts
- investment summary
- acquisition advice
- growth opportunities
- next steps

## Current limitation to know on live calls

If you upload a single multi-sheet workbook like `HVAC_data.xlsx`, much of the dashboard can stay sparse because the current backend only normalizes one primary document per uploaded file.

That is a product limitation in the current v1 architecture, not a problem with your workbook itself.
