"""Unit tests for CSV parsing and validation."""
from __future__ import annotations

from app.module.csv_parser import parse_csv_bytes

from .conftest import make_csv


def errors_of(parsed):
    return [(e.row, e.error) for e in parsed.errors]


def test_happy_path_with_optional_phone():
    parsed = parse_csv_bytes(make_csv(["General Hospital,123 Main St,555-1234", "City Clinic,45 Oak Ave,"]))
    assert parsed.valid
    assert len(parsed.rows) == 2
    first, second = parsed.rows
    assert (first.row, first.name, first.phone) == (1, "General Hospital", "555-1234")
    assert second.phone is None  # blank phone -> null


def test_phone_column_itself_is_optional():
    parsed = parse_csv_bytes(make_csv(["A,Addr"], header="name,address"))
    assert parsed.valid
    assert parsed.rows[0].phone is None


def test_quoted_comma_and_crlf_handled():
    data = b'name,address,phone\r\n"St. Mary, West Wing","1 Elm St, Suite 2",555\r\n'
    parsed = parse_csv_bytes(data)
    assert parsed.valid
    assert parsed.rows[0].name == "St. Mary, West Wing"


def test_utf8_bom_tolerated():
    parsed = parse_csv_bytes(b"\xef\xbb\xbf" + make_csv(["A,Addr,1"]))
    assert parsed.valid


def test_empty_file_rejected():
    parsed = parse_csv_bytes(b"")
    assert errors_of(parsed) == [(0, "file is empty")]


def test_header_only_rejected():
    parsed = parse_csv_bytes(make_csv([]))
    assert not parsed.valid
    assert "no data rows" in parsed.errors[0].error


def test_missing_required_column_rejected():
    parsed = parse_csv_bytes(make_csv(["A,1"], header="name,phone"))
    assert errors_of(parsed) == [(0, "missing required column(s): address")]


def test_unknown_column_rejected():
    parsed = parse_csv_bytes(make_csv(["A,B,C"], header="name,address,city"))
    assert "unknown column(s): city" in parsed.errors[0].error


def test_duplicate_column_rejected():
    parsed = parse_csv_bytes(make_csv(["A,B,C"], header="name,name,address"))
    assert "duplicate column(s): name" in parsed.errors[0].error


def test_over_20_rows_rejected():
    rows = ["Hospital {},Addr {},".format(i, i) for i in range(21)]
    parsed = parse_csv_bytes(make_csv(rows))
    assert not parsed.valid
    assert "maximum is 20" in parsed.errors[0].error


def test_exactly_20_rows_accepted():
    rows = ["Hospital {},Addr {},".format(i, i) for i in range(20)]
    parsed = parse_csv_bytes(make_csv(rows))
    assert parsed.valid
    assert len(parsed.rows) == 20


def test_blank_name_and_address_reported_per_row():
    parsed = parse_csv_bytes(make_csv(["A,Addr,1", " ,Addr,1", "B, ,1"]))
    assert errors_of(parsed) == [(2, "name is required"), (3, "address is required")]
    assert len(parsed.rows) == 1  # valid rows still parsed for the validate report


def test_wrong_field_count_reported():
    parsed = parse_csv_bytes(make_csv(["A,Addr,1,extra"]))
    assert errors_of(parsed) == [(1, "expected 3 fields, got 4")]


def test_field_length_limit_enforced():
    parsed = parse_csv_bytes(make_csv(["{},Addr,1".format("x" * 201)]))
    assert errors_of(parsed) == [(1, "name exceeds 200 characters")]


def test_non_utf8_rejected():
    parsed = parse_csv_bytes(b"name,address\n\xff\xfe,\xff\n")
    assert errors_of(parsed) == [(0, "file is not valid UTF-8")]


def test_blank_lines_ignored():
    parsed = parse_csv_bytes(b"name,address,phone\n\nA,Addr,1\n\n")
    assert parsed.valid
    assert len(parsed.rows) == 1


def test_header_is_case_insensitive_and_whitespace_tolerant():
    parsed = parse_csv_bytes(make_csv(["A,Addr,1"], header="Name, Address , PHONE"))
    assert parsed.valid
