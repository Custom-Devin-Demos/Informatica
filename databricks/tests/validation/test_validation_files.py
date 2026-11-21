"""Fixed-width outbound file diffs."""
from biis_lakehouse.common.fixed_width import Field, write_fixed_width
from biis_lakehouse.validation import diff_fixed_width_files

FIELDS = [Field("SSN", 9), Field("AMT", 7, numeric=True), Field("NAME", 5)]
LINES = ["1234567890000042AB   ", "9876543210000100CD   ", "5555555550000007EF   "]


def _write(tmp_path, name, lines, eol="\n", final=True):
    p = tmp_path / name
    p.write_bytes((eol.join(lines) + (eol if final else "")).encode("latin-1"))
    return str(p)


def test_spark_output_matches_legacy_file(ctx, tmp_path):
    df = ctx.spark.createDataFrame(
        [("123456789", "42", "AB"), ("987654321", "100", "CD"), ("555555555", "7", "EF")], "SSN string, AMT string, NAME string"
    )
    out = ctx.path("out", "CPM", "afps.txt")
    write_fixed_width(df, out, FIELDS)  # Spark writes a directory with one part file
    legacy = _write(tmp_path, "legacy.txt", LINES)
    d = diff_fixed_width_files(legacy, out, fields=FIELDS)
    assert d.ok and d.byte_identical, d.summary()
    assert (d.legacy_lines, d.target_lines) == (3, 3)
    assert d.legacy_sha256 == d.target_sha256


def test_line_endings_and_trailing_spaces(tmp_path):
    legacy = _write(tmp_path, "l.txt", LINES, eol="\r\n")
    target = _write(tmp_path, "t.txt", [x.rstrip() for x in LINES], final=False)
    strict = diff_fixed_width_files(legacy, target, fields=FIELDS)
    assert not strict.ok and not strict.byte_identical
    assert strict.diff_count == 3 and strict.bad_length_lines == [1, 2, 3]
    relaxed = diff_fixed_width_files(legacy, target, ignore_trailing_spaces=True)
    assert relaxed.ok and not relaxed.byte_identical


def test_changed_field_reported(tmp_path):
    legacy = _write(tmp_path, "l.txt", LINES)
    changed = LINES.copy()
    changed[1] = "9876543210000101CD   "
    d = diff_fixed_width_files(legacy, _write(tmp_path, "t.txt", changed), fields=FIELDS)
    assert not d.ok and d.diff_count == 1
    (x,) = d.diffs
    assert (x.kind, x.legacy_line, x.first_col, x.field) == ("changed", 2, 16, "AMT")
    assert "AMT" in d.summary()


def test_inserted_and_missing_lines(tmp_path):
    legacy = _write(tmp_path, "l.txt", LINES)
    extra = "1111111110000001ZZ   "
    d = diff_fixed_width_files(legacy, _write(tmp_path, "t.txt", [LINES[0], extra, LINES[1]]), fields=FIELDS)
    assert (d.legacy_lines, d.target_lines) == (3, 3)
    d = diff_fixed_width_files(legacy, _write(tmp_path, "t2.txt", [LINES[0], extra, *LINES[1:]]))
    assert [(x.kind, x.target_line) for x in d.diffs] == [("target_only", 2)]
    d = diff_fixed_width_files(legacy, _write(tmp_path, "t3.txt", LINES[:2]))
    assert [(x.kind, x.legacy_line) for x in d.diffs] == [("legacy_only", 3)]


def test_sort_lines(tmp_path):
    legacy = _write(tmp_path, "l.txt", LINES)
    target = _write(tmp_path, "t.txt", list(reversed(LINES)))
    assert not diff_fixed_width_files(legacy, target).ok
    assert diff_fixed_width_files(legacy, target, sort_lines=True).ok


def test_max_diffs(tmp_path):
    legacy = _write(tmp_path, "l.txt", [f"{i:021d}" for i in range(50)])
    target = _write(tmp_path, "t.txt", [f"{i + 1:021d}" for i in range(50)])
    d = diff_fixed_width_files(legacy, target, max_diffs=5)
    assert d.diff_count == 2 and len(d.diffs) == 2  # one line dropped at start, one added at end
    target = _write(tmp_path, "t2.txt", [f"{i:020d}X" for i in range(50)])
    d = diff_fixed_width_files(legacy, target, max_diffs=5)
    assert d.diff_count == 50 and len(d.diffs) == 5 and "45 more" in d.summary()
