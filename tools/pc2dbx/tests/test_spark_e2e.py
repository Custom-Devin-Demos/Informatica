"""Run a generated workflow module end-to-end on a local Delta SparkSession."""
import importlib.util

import pytest

from pc2dbx.codegen import generate_workflow
from pc2dbx.parser import parse_file

from .conftest import EXPORTS


@pytest.fixture
def pay_calendar(tmp_path):
    exp = parse_file(next(p for p in EXPORTS if p.name == "Pay_Calendar"))
    folder = exp.folders[0]
    code, _ = generate_workflow(folder, folder.workflows["wf_Pay_Calendar"])
    path = tmp_path / "wf_pay_calendar_gen.py"
    path.write_text(code)
    spec = importlib.util.spec_from_file_location("wf_pay_calendar_gen", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_pay_calendar_runs(spark, tmp_path, pay_calendar):
    from biis_lakehouse.context import RunContext

    spark.sql("DROP TABLE IF EXISTS histdba.pay_period")
    spark.sql(
        """CREATE TABLE histdba.pay_period (PP_NUM DECIMAL(10,0), PP_END_YEAR DECIMAL(10,0),
           PP_START_DTE TIMESTAMP, PP_END_DTE TIMESTAMP, LV_NUM DECIMAL(10,0), LV_YEAR DECIMAL(10,0),
           PAY_DTE TIMESTAMP, CURR_PP_FLAG STRING) USING delta"""
    )
    spark.sql(
        """INSERT INTO histdba.pay_period VALUES
           (1, 2024, TIMESTAMP'2024-01-01', TIMESTAMP'2024-01-14', 1, 2024, TIMESTAMP'2024-01-19', 'Y'),
           (2, 2024, TIMESTAMP'2024-01-15', TIMESTAMP'2024-01-28', 2, 2024, TIMESTAMP'2024-02-02', 'N')"""
    )
    ctx = RunContext(spark=spark, landing_root=str(tmp_path / "landing"),
                     params={"WF_PP_NUM": "2", "WF_PP_END_YEAR": "2024"})
    for name, task in pay_calendar.TASKS.items():
        result = task(ctx)
        assert isinstance(result, dict), name
        ctx.params.update(result)
    rows = {(int(r.PP_NUM), r.CURR_PP_FLAG) for r in spark.table("histdba.pay_period").collect()}
    assert len(rows) == 2
    assert (tmp_path / "landing" / "out" / "pay_period_verify_file.txt").exists()
