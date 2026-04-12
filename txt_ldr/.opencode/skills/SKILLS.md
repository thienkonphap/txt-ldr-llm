---
name: sqlldr-gen
description: >
  Generates Oracle SQL*Loader control files (.ctl) and CREATE TABLE DDL scripts (.sql)
  from a CSV file using a hybrid pipeline: rule-based schema detection followed by
  AI semantic enrichment. Use this skill whenever the user wants to load a CSV into
  Oracle, needs a control file for SQLLDR, wants to auto-generate a CREATE TABLE from
  a flat file, or mentions ETL, data loading, Oracle ingestion, or schema inference
  from CSV. Trigger even if the user just says "generate the ctl file" or "create the
  table script from this csv" — don't wait for them to say "skill".
---

# SQLLDR Hybrid Pipeline Skill

## What this skill does

Given a CSV file, this skill runs a two-stage pipeline:

1. **Rule-based detection** — `scripts/csv_schema_detector.py` samples the file and infers Oracle types (VARCHAR2, NUMBER, DATE, TIMESTAMP) with confidence scores.
2. **AI enrichment** — `scripts/ai_enrich.py` sends the draft schema + sample data to Claude to correct semantic mistakes, refine types, and set date masks.
3. **Output generation** — produces a `.ctl` SQL*Loader control file and a `.sql` CREATE TABLE DDL, both written to disk plus printed as a summary.

## When to use this skill

Trigger on any of:
- "generate a control file for sqlldr"
- "create the oracle table from this csv"
- "load this csv into oracle"
- "infer the schema from this file"
- "build the ctl and ddl for this dataset"
- User uploads or references a `.csv` / `.txt` flat file and mentions Oracle, database, or loading

## How to run the pipeline

### Standard invocation

```bash
python scripts/generate.py \
  --csv path/to/file.csv \
  --table TARGET_TABLE_NAME \
  --out-dir path/to/output/
```

### Full options

```bash
python scripts/generate.py \
  --csv       path/to/file.csv        # required
  --table     MY_TABLE                # default: CSV filename uppercased
  --schema    DWH                     # Oracle owner/schema prefix (optional)
  --delimiter ","                     # default: comma
  --enclosure '"'                     # default: double-quote (pass '' to disable)
  --sample    100                     # rows to sample for type inference (default: 100)
  --encoding  utf-8                   # file encoding (default: utf-8)
  --out-dir   ./output/               # where to write .ctl and .sql (default: CSV dir)
  --skip-ai                           # skip AI enrichment, rule-based only
```

### Skip AI enrichment (faster, no API call)

```bash
python scripts/generate.py --csv file.csv --table MY_TABLE --skip-ai
```

## Output files

| File                  | Content                                      |
|-----------------------|----------------------------------------------|
| `{table}.ctl`         | SQL*Loader control file                      |
| `{table}.sql`         | CREATE TABLE DDL                             |

Both files are also printed to stdout as part of the summary.

## CTL format reference

```
OPTIONS (SKIP=1, ERRORS=100, DIRECT=FALSE)
LOAD DATA
INFILE 'file.csv'
APPEND
INTO TABLE MY_TABLE
FIELDS TERMINATED BY ','
         OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
  ID                   "TO_NUMBER(:ID)",
  NAME                 CHAR(255),
  AMOUNT               "TO_NUMBER(:AMOUNT, '9999999999999.99')",
  LOAD_DATE            "TO_DATE(:LOAD_DATE, 'YYYY-MM-DD')",
  CREATED_AT           "TO_TIMESTAMP(:CREATED_AT, 'YYYY-MM-DD HH24:MI:SS')",
  NOTES                CHAR(4000)
)
```

**Column declaration rules:**
- `VARCHAR2(n)` / `CHAR(n)` → `CHAR(n)` — buffer sized from DDL type
- `NUMBER(p)` integer → `"TO_NUMBER(:COL)"`
- `NUMBER(p,s)` decimal → `"TO_NUMBER(:COL, 'fmt')"` — mask auto-built from precision/scale
- `DATE` → `"TO_DATE(:COL, 'mask')"`
- `TIMESTAMP` → `"TO_TIMESTAMP(:COL, 'mask')"` — SQLLDR cannot cast natively

## AI enrichment details

The AI enrichment step calls Claude with:
- The full rule-based schema (column name, detected type, confidence, sample values)
- A semantic review prompt asking it to apply domain knowledge

Claude corrects things the rule engine cannot know from data alone:
- `zip_code` detected as `NUMBER` → corrected to `VARCHAR2(10)`
- `email` detected as `VARCHAR2(50)` → corrected to `VARCHAR2(255)`
- `amount` with all-integer samples → corrected to `NUMBER(15,2)`

If AI enrichment fails (network error, API error), the pipeline automatically falls back
to the rule-based schema and logs a warning.

## Type mapping reference

For detailed Oracle type mapping rules used during AI enrichment, see:
`references/oracle_types.md`

## Asking the user for missing info

Before running, make sure you have:
1. **CSV file path** — required
2. **Table name** — if not provided, derive from the CSV filename (uppercase, underscores)
3. **Delimiter** — if not standard comma, ask. Check file extension or first line for hints.
4. **Schema/owner** — only ask if the user mentions a specific Oracle schema

Do NOT ask for all options upfront. Run with sensible defaults and let the user correct
in the next iteration if needed.

## Workflow for Claude

1. Confirm the CSV path and table name with the user (derive if obvious)
2. Run `scripts/generate.py` with the appropriate arguments
3. Show the printed summary from stdout
4. Present the two output files using `present_files`
5. Ask the user to review the generated types — especially any medium/low confidence columns
6. If the user wants corrections, they can either:
   - Re-run with `--skip-ai` to iterate faster
   - Edit the output files manually
   - Ask Claude to adjust specific columns and regenerate

## Error handling

| Error                          | Action                                              |
|-------------------------------|-----------------------------------------------------|
| File not found                | Tell user, ask for correct path                     |
| Encoding error                | Retry with `--encoding latin-1` or `--encoding cp1252` |
| AI enrichment timeout/failure | Pipeline auto-falls back to rule-based, log warning |
| All columns low confidence    | Warn user, suggest increasing `--sample` rows       |
| Delimiter not detected        | Ask user to specify `--delimiter` explicitly        |