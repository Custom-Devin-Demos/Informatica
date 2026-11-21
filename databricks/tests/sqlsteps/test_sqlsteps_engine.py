import pytest

from biis_lakehouse.sqlsteps.engine import SqlStepError, is_query, parse_sql, render


def test_parse_sql_splits_statements_strips_comments_and_keeps_directives():
    text = """
    -- header comment
    -- @label first
    -- @metric n
    SELECT count(*) -- trailing
      FROM t WHERE c = 'a--b;';
    DELETE FROM t;  -- done
    UPDATE t SET c = NULL
    """
    stmts = parse_sql(text)
    assert [s.sql.split()[0] for s in stmts] == ["SELECT", "DELETE", "UPDATE"]
    assert stmts[0].directives == {"label": "first", "metric": "n"}
    assert "'a--b;'" in stmts[0].sql and "trailing" not in stmts[0].sql
    assert stmts[1].directives == {}


def test_comment_only_file_has_no_statements():
    assert parse_sql("-- not ported\n-- ${x}\n") == []


def test_render_and_is_query():
    assert render("SELECT * FROM ${s}.t", {"s": "cat.nknight"}) == "SELECT * FROM cat.nknight.t"
    with pytest.raises(SqlStepError):
        render("SELECT ${missing}", {})
    assert is_query("  WITH x AS (SELECT 1) SELECT * FROM x")
    assert not is_query("INSERT INTO t SELECT 1")
