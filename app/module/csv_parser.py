"""CSV parsing and validation for bulk hospital uploads.

Collects every problem instead of failing fast so the /validate endpoint
(and a rejected upload) can report all errors in one pass. File-level
errors use row 0; data rows are numbered from 1, matching the row numbers
in processing results.
"""
from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from ..core import config


@dataclass
class CsvRow:
    row: int
    name: str
    address: str
    phone: Optional[str]


@dataclass
class CsvError:
    row: int
    error: str


@dataclass
class ParsedCsv:
    rows: List[CsvRow] = field(default_factory=list)
    errors: List[CsvError] = field(default_factory=list)

    @property
    def valid(self) -> bool:
        return not self.errors


def parse_csv_bytes(data: bytes) -> ParsedCsv:
    parsed = ParsedCsv()
    if not data.strip():
        parsed.errors.append(CsvError(0, "file is empty"))
        return parsed
    try:
        text = data.decode("utf-8-sig")  # tolerate a UTF-8 BOM from Excel
    except UnicodeDecodeError:
        parsed.errors.append(CsvError(0, "file is not valid UTF-8"))
        return parsed

    try:
        lines = [r for r in csv.reader(io.StringIO(text)) if any(c.strip() for c in r)]
    except csv.Error as exc:
        parsed.errors.append(CsvError(0, "malformed CSV: {}".format(exc)))
        return parsed
    if not lines:
        parsed.errors.append(CsvError(0, "file is empty"))
        return parsed

    header = [cell.strip().lower() for cell in lines[0]]
    header_error = _check_header(header)
    if header_error:
        parsed.errors.append(CsvError(0, header_error))
        return parsed

    data_rows = lines[1:]
    if not data_rows:
        parsed.errors.append(CsvError(0, "CSV contains a header but no data rows"))
        return parsed
    if len(data_rows) > config.MAX_CSV_ROWS:
        parsed.errors.append(
            CsvError(
                0,
                "CSV has {} hospital rows; maximum is {}".format(
                    len(data_rows), config.MAX_CSV_ROWS
                ),
            )
        )
        return parsed

    for number, cells in enumerate(data_rows, start=1):
        row, errors = _parse_row(number, header, cells)
        if errors:
            parsed.errors.extend(errors)
        else:
            parsed.rows.append(row)
    return parsed


def _check_header(header: List[str]) -> Optional[str]:
    allowed = set(config.CSV_REQUIRED_COLUMNS) | set(config.CSV_OPTIONAL_COLUMNS)
    missing = [c for c in config.CSV_REQUIRED_COLUMNS if c not in header]
    if missing:
        return "missing required column(s): {}".format(", ".join(missing))
    unknown = [c for c in header if c not in allowed]
    if unknown:
        return "unknown column(s): {} (expected name,address,phone)".format(
            ", ".join(unknown)
        )
    duplicates = sorted({c for c in header if header.count(c) > 1})
    if duplicates:
        return "duplicate column(s): {}".format(", ".join(duplicates))
    return None


def _parse_row(
    number: int, header: List[str], cells: List[str]
) -> Tuple[Optional[CsvRow], List[CsvError]]:
    if len(cells) != len(header):
        return None, [
            CsvError(number, "expected {} fields, got {}".format(len(header), len(cells)))
        ]
    values = {column: cells[i].strip() for i, column in enumerate(header)}
    errors = []
    for column in config.CSV_REQUIRED_COLUMNS:
        if not values.get(column):
            errors.append(CsvError(number, "{} is required".format(column)))
    for column, value in values.items():
        limit = config.MAX_FIELD_LENGTHS.get(column)
        if limit and len(value) > limit:
            errors.append(
                CsvError(number, "{} exceeds {} characters".format(column, limit))
            )
    if errors:
        return None, errors
    return (
        CsvRow(
            row=number,
            name=values["name"],
            address=values["address"],
            phone=values.get("phone") or None,
        ),
        [],
    )
