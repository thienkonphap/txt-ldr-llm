"""
AI Enrichment for CSV Schema
-----------------------------
Takes the rule-based schema (JSON) + sample rows (JSON),
calls Claude to review/correct types, nullability, masks, and suggest indexes.
Outputs enriched schema as JSON to stdout.

Usage:
    python ai_enrich.py --schema schema.json --samples samples.json --table MY_TABLE
"""

import argparse
import json
import sys
import urllib.request
import urllib.error


SYSTEM_PROMPT = """You are an expert Oracle DBA and ETL engineer specializing in SQL*Loader.
You receive a schema detected by a rule-based engine and your job is to:
1. Correct Oracle types based on column name semantics AND sample data
2. Set nullable=false for obvious mandatory fields (IDs, codes, keys, dates that are always present)
3. Provide the correct SQL*Loader date mask for DATE and TIMESTAMP columns
4. Flag columns where the name implies a domain constraint (email, phone, zip, status codes)
5. Suggest useful indexes (primary key candidates, foreign keys, frequent filter columns)

Rules:
- zip_code, postal_code → always VARCHAR2, never NUMBER
- email, mail → VARCHAR2(255)
- phone, tel, fax → VARCHAR2(20)
- id, code, key columns → NOT NULL
- amount, price, qty, quantity → NUMBER with appropriate scale
- flag, active, enabled, status with 0/1 values → CHAR(1)
- description, comment, note, text with long values → CLOB if >500 chars typical

Respond ONLY in valid JSON, no markdown fences, no preamble, no explanation outside the JSON.
"""

USER_TEMPLATE = """Table: {table}

Rule-based schema:
{schema}

Sample data (up to 5 rows per column):
{samples}

Respond in this exact JSON structure:
{{
  "columns": [
    {{
      "name": "COLUMN_NAME",
      "oracle_type": "VARCHAR2(100)",
      "nullable": true,
      "sqlldr_mask": null,
      "ai_changed": false,
      "change_reason": "brief reason or null"
    }}
  ],
  "index_suggestions": [
    "CREATE INDEX IDX_{table}_COL ON {table}(COL);"
  ],
  "general_notes": [
    "any important loading or design observations"
  ]
}}
"""


def call_claude(schema: list, samples: dict, table: str) -> dict:
    schema_text = json.dumps(schema, indent=2)
    samples_text = json.dumps(samples, indent=2)

    payload = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 2000,
        "system": SYSTEM_PROMPT,
        "messages": [
            {
                "role": "user",
                "content": USER_TEMPLATE.format(
                    table=table,
                    schema=schema_text,
                    samples=samples_text,
                )
            }
        ]
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=data,
        headers={
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        print(f"[ai_enrich] API error {e.code}: {err}", file=sys.stderr)
        sys.exit(1)

    raw = "".join(
        block.get("text", "")
        for block in body.get("content", [])
        if block.get("type") == "text"
    )

    # strip accidental markdown fences
    clean = raw.strip()
    if clean.startswith("```"):
        clean = clean.split("\n", 1)[1]
        clean = clean.rsplit("```", 1)[0]

    try:
        return json.loads(clean)
    except json.JSONDecodeError as e:
        print(f"[ai_enrich] Failed to parse Claude response as JSON: {e}", file=sys.stderr)
        print(f"[ai_enrich] Raw response:\n{raw[:500]}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema",  required=True, help="Path to rule-based schema JSON")
    parser.add_argument("--samples", required=True, help="Path to sample data JSON")
    parser.add_argument("--table",   required=True, help="Oracle table name")
    args = parser.parse_args()

    with open(args.schema)  as f: schema  = json.load(f)
    with open(args.samples) as f: samples = json.load(f)

    result = call_claude(schema, samples, args.table.upper())
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()