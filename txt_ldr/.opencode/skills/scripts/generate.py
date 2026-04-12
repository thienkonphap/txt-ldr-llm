"""
SQLLDR Hybrid Pipeline — Main Orchestrator
-------------------------------------------
Runs the full pipeline:
  1. Rule-based schema detection  (csv_schema_detector.py)
  2. AI enrichment                (ai_enrich.py)
  3. CTL + DDL generation

Usage:
    python generate.py --csv path/to/file.csv --table MY_TABLE [options]

Options:
    --table       Target Oracle table name (default: CSV filename uppercased)
    --schema      Oracle schema/owner prefix  e.g. DWH
    --delimiter   Field delimiter (default: ,)
    --enclosure   Quote character  (default: ")
    --sample      Number of rows to sample for inference (default: 100)
    --encoding    File encoding (default: utf-8)
    --out-dir     Output directory for .ctl and .sql files (default: same as CSV)
    --skip-ai     Skip AI enrichment and use rule-based schema only
"""

import argparse
import json
import os
import sys
import subprocess
import tempfile
from pathlib import Path

# ── locate sibling scripts ──────────────────────────────────────────────────
SCRIPTS_DIR = Path(__file__).parent
DETECTOR    = SCRIPTS_DIR / "csv_schema_detector.py"
AI_ENRICH   = SCRIPTS_DIR / "ai_enrich.py"

sys.path.insert(0, str(SCRIPTS_DIR))
from csv_schema_detector import (
    detect_schema, generate_create_table, generate_ctl,
    print_schema_report, ColumnMeta
)


# ── helpers ──────────────────────────────────────────────────────────────────

def schema_to_json(schema: list[ColumnMeta]) -> list[dict]:
    return [
        {
            "name":          col.name,
            "detected_type": col.detected_type,
            "oracle_type":   col.oracle_type,
            "nullable":      col.nullable,
            "confidence":    col.confidence,
            "note":          col.note,
            "sqlldr_mask":   col.sqlldr_mask,
        }
        for col in schema
    ]


def samples_to_json(schema: list[ColumnMeta]) -> dict:
    return {col.name: col.sample_values for col in schema}


def apply_ai_result(schema: list[ColumnMeta], ai_result: dict) -> tuple[list[ColumnMeta], list[str]]:
    """Merge AI enrichment back into the ColumnMeta list. Returns (updated_schema, change_log)."""
    ai_by_name = {c["name"].upper(): c for c in ai_result.get("columns", [])}
    change_log = []

    for col in schema:
        ai = ai_by_name.get(col.name.upper())
        if not ai:
            continue
        if ai.get("ai_changed"):
            old_type     = col.oracle_type
            old_nullable = col.nullable
            col.oracle_type  = ai["oracle_type"]
            col.nullable     = ai["nullable"]
            col.sqlldr_mask  = ai.get("sqlldr_mask") or col.sqlldr_mask
            reason = ai.get("change_reason") or "AI recommendation"
            parts = []
            if old_type != col.oracle_type:
                parts.append(f"type {old_type} → {col.oracle_type}")
            if old_nullable != col.nullable:
                parts.append(f"nullable {'YES' if old_nullable else 'NO'} → {'YES' if col.nullable else 'NO'}")
            if parts:
                change_log.append(f"  {col.name}: {', '.join(parts)} ({reason})")
        else:
            # even if not changed, absorb mask and nullable if AI provided them
            if ai.get("sqlldr_mask"):
                col.sqlldr_mask = ai["sqlldr_mask"]
            col.nullable = ai.get("nullable", col.nullable)

    return schema, change_log


def print_section(title: str):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Hybrid CSV → Oracle SQLLDR pipeline (rule-based + AI enrichment)"
    )
    parser.add_argument("--csv",        required=True,         help="Path to the CSV file")
    parser.add_argument("--table",      default="",            help="Oracle table name")
    parser.add_argument("--schema",     default="",            help="Oracle schema/owner prefix")
    parser.add_argument("--delimiter",  default=",",           help="Field delimiter (default: ,)")
    parser.add_argument("--enclosure",  default='"',           help="Quote character (default: \")")
    parser.add_argument("--sample",     default=100, type=int, help="Rows to sample (default: 100)")
    parser.add_argument("--encoding",   default="utf-8",       help="File encoding (default: utf-8)")
    parser.add_argument("--out-dir",    default="",            help="Output directory")
    parser.add_argument("--skip-ai",    action="store_true",   help="Skip AI enrichment")
    args = parser.parse_args()

    csv_path   = Path(args.csv)
    table_name = (args.table or csv_path.stem).upper().replace("-", "_").replace(" ", "_")
    out_dir    = Path(args.out_dir) if args.out_dir else csv_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    ctl_path = out_dir / f"{table_name.lower()}.ctl"
    ddl_path = out_dir / f"{table_name.lower()}.sql"

    # ── step 1: rule-based detection ────────────────────────────────────────
    print_section("STEP 1 — Rule-based schema detection")
    schema = detect_schema(
        str(csv_path),
        delimiter=args.delimiter,
        enclosure=args.enclosure,
        sample_rows=args.sample,
        encoding=args.encoding,
    )
    print_schema_report(schema)

    low_conf = [c for c in schema if c.confidence in ("medium", "low")]
    if low_conf:
        print(f"\n  ⚠  {len(low_conf)} column(s) with medium/low confidence: "
              f"{', '.join(c.name for c in low_conf)}")

    # ── step 2: AI enrichment ────────────────────────────────────────────────
    if args.skip_ai:
        print("\n  [AI enrichment skipped — using rule-based schema]")
        index_suggestions = []
        general_notes     = []
    else:
        print_section("STEP 2 — AI enrichment")
        print("  Sending schema to Claude for semantic review...")

        schema_json  = schema_to_json(schema)
        samples_json = samples_to_json(schema)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as sf:
            json.dump(schema_json, sf)
            schema_tmp = sf.name
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as sf:
            json.dump(samples_json, sf)
            samples_tmp = sf.name

        try:
            result = subprocess.run(
                [sys.executable, str(AI_ENRICH),
                 "--schema",  schema_tmp,
                 "--samples", samples_tmp,
                 "--table",   table_name],
                capture_output=True, text=True, timeout=90
            )
        finally:
            os.unlink(schema_tmp)
            os.unlink(samples_tmp)

        if result.returncode != 0:
            print(f"\n  ⚠  AI enrichment failed — falling back to rule-based schema.")
            print(f"     Reason: {result.stderr.strip()[:200]}")
            index_suggestions = []
            general_notes     = ["AI enrichment failed — output based on rule-based detection only."]
        else:
            ai_result         = json.loads(result.stdout)
            schema, changelog = apply_ai_result(schema, ai_result)
            index_suggestions = []
            general_notes     = ai_result.get("general_notes", [])

            if changelog:
                print(f"\n  Changes made by AI ({len(changelog)}):")
                for line in changelog:
                    print(line)
            else:
                print("\n  No type changes — rule-based schema confirmed by AI.")

            if general_notes:
                print("\n  AI notes:")
                for note in general_notes:
                    print(f"    • {note}")

    # ── step 3: generate output ──────────────────────────────────────────────
    print_section("STEP 3 — Generating output files")

    ddl = generate_create_table(schema, table_name, args.schema)
    ctl = generate_ctl(schema, table_name, csv_path.name, args.delimiter, args.enclosure)

    ddl_path.write_text(ddl)
    ctl_path.write_text(ctl)

    print(f"\n  ✔  DDL written : {ddl_path}")
    print(f"  ✔  CTL written : {ctl_path}")

    # ── summary ──────────────────────────────────────────────────────────────
    print_section("SUMMARY")
    print(f"\n  Table        : {table_name}")
    print(f"  Columns      : {len(schema)}")
    print(f"  AI enrichment: {'skipped' if args.skip_ai else 'applied'}")
    print(f"  CTL file     : {ctl_path}")
    print(f"  DDL file     : {ddl_path}")

    print("\n--- CREATE TABLE ---")
    print(ddl)
    print("\n--- SQLLDR CONTROL FILE ---")
    print(ctl)


if __name__ == "__main__":
    main()