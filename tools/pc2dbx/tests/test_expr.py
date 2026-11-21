"""PowerCenter expression -> Spark SQL translation (PLAN.md §3.1)."""
import pytest

from pc2dbx.expr import java_date_format, translate, translate_condition

CASES = [
    ("IIF(A > 1, 'x', 'y')", "IF((A > 1), 'x', 'y')"),
    ("IIF(A > 1, 'x')", "IF((A > 1), 'x', NULL)"),
    ("ISNULL(A)", "(A IS NULL)"),
    ("DECODE(A, 1, 'one', 2, 'two', 'other')", "CASE A WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END"),
    ("DECODE(TRUE, A = 1, 'one', 'other')", "CASE WHEN (A = 1) THEN 'one' ELSE 'other' END"),
    ("SUM(A)", "sum(A)"),
    ("SUM(A, B = 'RA')", "sum(IF((B = 'RA'), A, NULL))"),
    ("COUNT(*)", "count(*)"),
    ("FIRST(A)", "first(A, true)"),
    ("LAST(A)", "last(A, true)"),
    ("SUBSTR(A, 1, 3)", "substring(A, 1, 3)"),
    ("TO_DECIMAL(A, 2)", "CAST(A AS DECIMAL(28, 2))"),
    ("TO_CHAR(A)", "CAST(A AS STRING)"),
    ("TO_CHAR(D, 'YYYYMMDD')", "date_format(D, 'yyyyMMdd')"),
    ("TO_DATE(A, 'MM/DD/YYYY')", "to_timestamp(A, 'MM/dd/yyyy')"),
    ("IS_DATE(A, 'YYYYMMDD')", "(to_timestamp(A, 'yyyyMMdd') IS NOT NULL)"),
    ("ABS(A)", "abs(A)"),
    ("SIGN(A)", "signum(A)"),
    ("TRUNC(SYSDATE)", "date_trunc('DAY', current_timestamp())"),
    ("TRUNC(A)", "CAST(A AS BIGINT)"),
    ("LPAD(A, 5, '0')", "lpad(A, 5, '0')"),
    ("RPAD(A, 5)", "rpad(A, 5)"),
    ("LTRIM(A)", "ltrim(A)"),
    ("RTRIM(A, '0')", "TRIM(TRAILING '0' FROM A)"),
    ("LENGTH(A)", "length(A)"),
    ("UPPER(A)", "upper(A)"),
    ("IS_SPACES(A)", "(rtrim(A) = '')"),
    ("INSTR(A, 'x')", "instr(A, 'x')"),
    ("INSTR(A, 'x', 3)", "locate('x', A, 3)"),
    ("REPLACECHR(0, A, '-/', NULL)", "regexp_replace(A, '(?i)[\\\\-/]', '')"),
    ("REPLACESTR(1, A, 'ab', 'cd', 'x')", "regexp_replace(A, 'ab|cd', 'x')"),
    ("CHR(39)", "chr(39)"),
    ("IN(A, 'X', 'Y')", "(A IN ('X', 'Y'))"),
    ("TO_INTEGER(A)", "CAST(A AS INT)"),
    ("GET_DATE_PART(D, 'MM')", "month(D)"),
    ("GET_DATE_PART(D, 'YYYY')", "year(D)"),
    ("ADD_TO_DATE(D, 'DD', -10)", "timestampadd(DAY, (-10), D)"),
    ("SYSDATE", "current_timestamp()"),
    ("SESSSTARTTIME", "current_timestamp()"),
    ("$$PP_NUM", "${PP_NUM}"),
    ("$Param_Root_Directory", "${Root_Directory}"),
    ("A || B || 'c'", "concat_ws('', A, B, 'c')"),
    ("NOT A = 1 AND B <> 2 OR C != 3", "(((NOT (A = 1)) AND (B <> 2)) OR (C <> 3))"),
    ("'it' || CHR(39) || 's'", "concat_ws('', 'it', chr(39), 's')"),
    ("A -- trailing comment", "A"),
    ("DATE_DIFF(D1, D2, 'DD')", "((unix_timestamp(D1) - unix_timestamp(D2)) / 86400)"),
    ("IIF(DD_INSERT = 0, 1, 2)", "IF((0 = 0), 1, 2)"),
]


@pytest.mark.parametrize("pc,spark", CASES, ids=[c[0] for c in CASES])
def test_translate(pc, spark):
    res = translate(pc)
    assert res.ok, res.reason
    assert res.sql == spark


def test_params_and_setvariable():
    res = translate("SETVARIABLE($$MAP_SUBJECT, 'Pay calendar ' || $$PP_NUM)")
    assert res.ok and res.params == {"PP_NUM"}
    assert res.setvars[0]["variable"] == "MAP_SUBJECT"


def test_update_strategy_tokens():
    assert translate("IIF(ISNULL(K), DD_INSERT, DD_UPDATE)", dd_tokens=True).sql == \
        "IF((K IS NULL), DD_INSERT, DD_UPDATE)"


def test_unconnected_lookup():
    res = translate(":LKP.LKP_LESC_KEY(1) + 1")
    assert res.ok and res.lookups == [{"lookup": "LKP_LESC_KEY", "args": ["1"], "column": "_lkp0"}]


def test_boolean_coercion():
    assert translate_condition("FLAG", {"FLAG": "integer"}).sql == "(FLAG <> 0)"
    assert translate("FLAG = TRUE", {"FLAG": "integer"}).sql == "(FLAG = 1)"
    assert translate("IIF(FLAG, 1, 0)", {"FLAG": "integer"}).sql == "IF((FLAG <> 0), 1, 0)"


@pytest.mark.parametrize("pc", ["FOO_BAR(A)", "TO_DATE(A, 'J')", "IIF(A", "ROUND(SYSDATE)"])
def test_unsupported_is_reported(pc):
    res = translate(pc)
    assert not res.ok and res.reason and res.sql is None


def test_date_format():
    assert java_date_format("MM/DD/YYYY HH24:MI:SS") == "MM/dd/yyyy HH:mm:ss"
    assert java_date_format("YYYYMMDD") == "yyyyMMdd"
    assert java_date_format("MON-DD-YY") == "MMM-dd-yy"


def test_translates_real_estate_expressions():
    import xml.etree.ElementTree as ET

    from .conftest import EXPORTS

    total = failed = 0
    for path in EXPORTS:
        for el in ET.parse(path).getroot().iter("TRANSFORMFIELD"):
            if el.get("EXPRESSION"):
                total += 1
                failed += not translate(el.get("EXPRESSION")).ok
    assert total > 10000
    assert failed <= 5, failed
