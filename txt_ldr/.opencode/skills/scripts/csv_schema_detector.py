"""
CSV Schema Detector for Oracle / SQL*Loader
--------------------------------------------
Reads a CSV file, samples rows, and infers Oracle column types.
Outputs a schema dict that can be used to generate .ctl and CREATE TABLE scripts.
"""

import csv
import re
import math
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ColumnMeta:
    name: str
    detected_type: str          # internal label: integer, decimal, date, etc.
    oracle_type: str            # final Oracle DDL type
    nullable: bool
    confidence: str             # high / medium / low
    note: str
    sqlldr_mask: Optional[str] = None
    sample_values: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Type inference helpers
# ---------------------------------------------------------------------------

DATE_PATTERNS = [
    (re.compile(r"^\d{4}[-/]\d{2}[-/]\d{2}$"),              "YYYY-MM-DD"),
    (re.compile(r"^\d{2}[-/]\d{2}[-/]\d{4}$"),              "DD/MM/YYYY"),
    (re.compile(r"^\d{2}-[A-Z]{3}-\d{4}$", re.I),           "DD-MON-YYYY"),
]

DATETIME_PATTERNS = [
    (re.compile(r"^\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}:\d{2}$"),  "YYYY-MM-DD HH24:MI:SS"),
    (re.compile(r"^\d{4}[-/]\d{2}[-/]\d{2}[T ]\d{2}:\d{2}$"),         "YYYY-MM-DD HH24:MI"),
    (re.compile(r"^\d{2}[-/]\d{2}[-/]\d{4} \d{2}:\d{2}:\d{2}$"),      "DD/MM/YYYY HH24:MI:SS"),
]

INT_RE    = re.compile(r"^-?\d+$")
FLOAT_RE  = re.compile(r"^-?\d+[.,]\d+$")
BOOL_VALS = {"true", "false", "yes", "no", "1", "0", "y", "n", "oui", "non"}


def _match_date(value: str):
    for pattern, mask in DATETIME_PATTERNS:
        if pattern.match(value):
            return "timestamp", mask
    for pattern, mask in DATE_PATTERNS:
        if pattern.match(value):
            return "date", mask
    return None, None


def _decimal_places(value: str) -> int:
    parts = re.split(r"[.,]", value)
    return len(parts[1]) if len(parts) > 1 else 0


def infer_column(name: str, values: list, sample_size: int = 100) -> ColumnMeta:
    """
    Infer Oracle type from a list of raw string values.
    """
    sample = values[:sample_size]
    non_null = [v for v in sample if v.strip() != ""]
    nullable = len(non_null) < len(sample)

    # --- empty column ---
    if not non_null:
        return ColumnMeta(
            name=name, detected_type="empty", oracle_type="VARCHAR2(255)",
            nullable=True, confidence="low",
            note="No data to infer type from",
            sample_values=sample[:5],
        )

    # --- boolean ---
    if all(v.strip().lower() in BOOL_VALS for v in non_null):
        return ColumnMeta(
            name=name, detected_type="boolean", oracle_type="CHAR(1)",
            nullable=nullable, confidence="medium",
            note="Boolean-like values — consider CHAR(1) Y/N or NUMBER(1)",
            sample_values=non_null[:5],
        )

    # --- date / timestamp ---
    date_types, masks = zip(*[_match_date(v.strip()) for v in non_null]) if non_null else ([], [])
    unique_dtypes = set(date_types) - {None}
    if unique_dtypes and all(t in ("date", "timestamp") for t in date_types):
        dominant = "timestamp" if "timestamp" in unique_dtypes else "date"
        mask = next((m for m in masks if m), None)
        oracle = "TIMESTAMP" if dominant == "timestamp" else "DATE"
        return ColumnMeta(
            name=name, detected_type=dominant, oracle_type=oracle,
            nullable=nullable, confidence="high",
            note=f"Date mask: {mask}",
            sqlldr_mask=mask,
            sample_values=non_null[:5],
        )

    # --- integer ---
    if all(INT_RE.match(v.strip()) for v in non_null):
        max_val = max(abs(int(v.strip())) for v in non_null)
        if max_val > 9_999_999_999_999_999:
            oracle_type = "NUMBER(19)"
        elif max_val > 999_999_999:
            oracle_type = "NUMBER(15)"
        else:
            oracle_type = "NUMBER(10)"
        return ColumnMeta(
            name=name, detected_type="integer", oracle_type=oracle_type,
            nullable=nullable, confidence="high",
            note=f"Max observed value: {max_val}",
            sample_values=non_null[:5],
        )

    # --- decimal ---
    if all(FLOAT_RE.match(v.strip()) or INT_RE.match(v.strip()) for v in non_null):
        max_dec = max(_decimal_places(v.strip()) for v in non_null)
        precision = 18 if max_dec > 4 else 15
        oracle_type = f"NUMBER({precision},{max_dec})"
        return ColumnMeta(
            name=name, detected_type="decimal", oracle_type=oracle_type,
            nullable=nullable, confidence="high",
            note=f"Max decimal places: {max_dec}",
            sample_values=non_null[:5],
        )

    # --- varchar / clob ---
    max_len = max(len(v) for v in non_null)
    if max_len > 2000:
        return ColumnMeta(
            name=name, detected_type="varchar", oracle_type="VARCHAR2(4000)",
            nullable=nullable, confidence="medium",
            note=f"Max length {max_len} — capped at VARCHAR2(4000)",
            sample_values=[v[:80] + "..." for v in non_null[:3]],
        )

    # round up to next clean bucket
    buckets = [10, 20, 50, 100, 200, 255, 500, 1000, 2000, 4000]
    padded = next((b for b in buckets if b >= max_len), 4000)
    confidence = "high" if max_len < 200 else "medium"
    return ColumnMeta(
        name=name, detected_type="varchar", oracle_type=f"VARCHAR2({padded})",
        nullable=nullable, confidence=confidence,
        note=f"Max observed length: {max_len}",
        sample_values=non_null[:5],
    )


# ---------------------------------------------------------------------------
# CSV reader
# ---------------------------------------------------------------------------

def detect_schema(
    filepath: str,
    delimiter: str = ",",
    enclosure: str = '"',
    sample_rows: int = 100,
    encoding: str = "utf-8",
) -> list[ColumnMeta]:
    """
    Read a CSV file and return a list of ColumnMeta objects.

    Parameters
    ----------
    filepath    : path to the CSV file (first row = header)
    delimiter   : field separator character
    enclosure   : quote character (pass '' to disable)
    sample_rows : how many data rows to sample for inference
    encoding    : file encoding

    Returns
    -------
    List of ColumnMeta, one per column.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    quotechar = enclosure if enclosure else None

    with open(path, encoding=encoding, newline="") as fh:
        reader = csv.reader(fh, delimiter=delimiter, quotechar=quotechar or '"',
                            quoting=csv.QUOTE_NONE if not enclosure else csv.QUOTE_MINIMAL)
        headers = next(reader)
        columns = {h: [] for h in headers}
        for i, row in enumerate(reader):
            if i >= sample_rows:
                break
            for j, header in enumerate(headers):
                columns[header].append(row[j] if j < len(row) else "")

    return [infer_column(h, columns[h], sample_rows) for h in headers]


# ---------------------------------------------------------------------------
# Output generators
# ---------------------------------------------------------------------------

def generate_create_table(
    schema: list[ColumnMeta],
    table_name: str,
    schema_name: str = "",
) -> str:
    owner = f"{schema_name.upper()}." if schema_name else ""
    lines = [f"CREATE TABLE {owner}{table_name.upper()}"]
    lines.append("(")
    col_lines = []
    for col in schema:
        nullable_clause = "" if col.nullable else " NOT NULL"
        col_lines.append(f"  {col.name.upper():<30}{col.oracle_type}{nullable_clause}")
    lines.append(",\n".join(col_lines))
    lines.append(");")
    return "\n".join(lines)


def generate_ctl(
    schema: list[ColumnMeta],
    table_name: str,
    csv_filename: str = None,
    delimiter: str = ",",
    enclosure: str = '"',
) -> str:
    csv_file = csv_filename or f"{table_name.lower()}.csv"
    delim_str = "TAB" if delimiter == "\t" else f"\'{delimiter}\'"

    header = [
        f"-- SQL*Loader Control File -- {table_name.upper()}",
        "-- Generated by csv_schema_detector.py",
        "",
        "OPTIONS (SKIP=1, ERRORS=100, DIRECT=FALSE)",
        "LOAD DATA",
        f"INFILE \'{csv_file}\'",
        f"APPEND",
        f"INTO TABLE {table_name.upper()}",
        f"FIELDS TERMINATED BY {delim_str}",
    ]
    if enclosure:
        header.append(f"         OPTIONALLY ENCLOSED BY \'{enclosure}\'")
    header.append("TRAILING NULLCOLS")
    header.append("(")

    col_lines = []
    for col in schema:
        n = col.name.upper()
        t = col.oracle_type

        if t == "CLOB":
            col_lines.append(f"  {n} CHAR(65535)")

        elif t in ("DATE", "TIMESTAMP"):
            mask = col.sqlldr_mask or ("YYYY-MM-DD" if t == "DATE" else "YYYY-MM-DD HH24:MI:SS")
            fn   = "TO_DATE" if t == "DATE" else "TO_TIMESTAMP"
            col_lines.append(f"  {n} \"{fn}(:{n}, \'{mask}\')\"")

        elif t.startswith("NUMBER"):
            m = re.match(r"NUMBER\((\d+)(?:,(\d+))?\)", t)
            precision = int(m.group(1)) if m else 18
            scale     = int(m.group(2)) if (m and m.group(2)) else 0
            if scale > 0:
                fmt = "9" * (precision - scale) + "." + "9" * scale
                col_lines.append(f"  {n} \"TO_NUMBER(:{n}, \'{fmt}\')\"")
            else:
                col_lines.append(f"  {n} \"TO_NUMBER(:{n})\"")

        else:
            buf = _varchar_len(t)
            col_lines.append(f"  {n} CHAR({buf})")

    header.append(",\n".join(col_lines))
    header.append(")")
    return "\n".join(header)


def _varchar_len(oracle_type: str) -> int:
    """Extract the length/size from an Oracle type string, with sensible defaults."""
    m = re.search(r"\((\d+)", oracle_type)
    if m:
        return int(m.group(1))
    # fallback defaults per type family
    if oracle_type == "DATE":
        return 20
    if oracle_type == "TIMESTAMP":
        return 26
    if oracle_type in ("CHAR", "VARCHAR2"):
        return 255
    return 50


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

def print_schema_report(schema: list[ColumnMeta]):
    print(f"\n{'COLUMN':<30} {'DETECTED':<12} {'ORACLE TYPE':<20} {'NULL':<6} {'CONF':<8} NOTE")
    print("-" * 100)
    for col in schema:
        null_flag = "YES" if col.nullable else "NO"
        print(f"{col.name:<30} {col.detected_type:<12} {col.oracle_type:<20} {null_flag:<6} {col.confidence:<8} {col.note}")


if __name__ == "__main__":
    import argparse, sys

    parser = argparse.ArgumentParser(description="Detect CSV schema and generate Oracle DDL + SQLLDR control file")
    parser.add_argument("csv_file",                  help="Path to the CSV file")
    parser.add_argument("--table",    default="",    help="Target Oracle table name (default: filename)")
    parser.add_argument("--schema",   default="",    help="Oracle schema/owner prefix")
    parser.add_argument("--delimiter",default=",",   help="Field delimiter (default: comma)")
    parser.add_argument("--enclosure",default='"',   help="Quote character (default: double-quote)")
    parser.add_argument("--sample",   default=100, type=int, help="Rows to sample (default: 100)")
    parser.add_argument("--encoding", default="utf-8", help="File encoding (default: utf-8)")
    parser.add_argument("--out-ctl",  default="",    help="Write .ctl file to this path")
    parser.add_argument("--out-ddl",  default="",    help="Write .sql DDL file to this path")
    args = parser.parse_args()

    table_name = args.table or Path(args.csv_file).stem.upper().replace("-", "_")

    try:
        schema = detect_schema(
            args.csv_file,
            delimiter=args.delimiter,
            enclosure=args.enclosure,
            sample_rows=args.sample,
            encoding=args.encoding,
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print_schema_report(schema)

    ddl = generate_create_table(schema, table_name, args.schema)
    ctl = generate_ctl(schema, table_name, Path(args.csv_file).name, args.delimiter, args.enclosure)

    print("\n--- CREATE TABLE ---")
    print(ddl)
    print("\n--- SQLLDR CONTROL FILE ---")
    print(ctl)

    if args.out_ddl:
        Path(args.out_ddl).write_text(ddl)
        print(f"\nDDL written to {args.out_ddl}")
    if args.out_ctl:
        Path(args.out_ctl).write_text(ctl)
        print(f"CTL written to {args.out_ctl}")