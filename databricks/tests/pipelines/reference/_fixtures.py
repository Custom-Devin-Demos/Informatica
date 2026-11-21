import datetime as dt
from decimal import Decimal

PAY_PERIOD_DDL = (
    "PP_NUM decimal(2,0), PP_END_YEAR decimal(4,0), PP_START_DTE timestamp, PP_END_DTE timestamp, "
    "LV_NUM decimal(2,0), LV_YEAR decimal(4,0), PAY_DTE timestamp, CURR_PP_FLAG string, "
    "HOLIDAY_1 timestamp, HOLIDAY_2 timestamp"
)


def pay_periods(ctx, current=(Decimal(2), Decimal(2024)), extra_current=()):
    """Pay periods 1..4 of 2024 (14-day periods starting 2024-01-07)."""
    rows = []
    for n in range(1, 5):
        start = dt.datetime(2024, 1, 7) + dt.timedelta(days=14 * (n - 1))
        key = (Decimal(n), Decimal(2024))
        flag = "Y" if key == current or key in extra_current else None
        rows.append((Decimal(n), Decimal(2024), start, start + dt.timedelta(days=13), Decimal(n), Decimal(2024),
                     start + dt.timedelta(days=19), flag, None, None))
    tbl = ctx.table_name("PAY_PERIOD", "HISTDBA")
    ctx.spark.createDataFrame(rows, PAY_PERIOD_DDL).write.format("delta").mode("overwrite").saveAsTable(tbl)
    return tbl


def read_lines(ctx, path):
    return [r.value for r in ctx.spark.read.text(path).collect()]
