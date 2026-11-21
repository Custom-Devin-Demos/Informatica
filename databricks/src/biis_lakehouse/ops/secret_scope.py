from __future__ import annotations

from typing import Callable, Optional

from biis_lakehouse.context import RunContext

DEFAULT_SCOPE = "biis"

# (ctx, scope, key) -> value. Replaced in tests; defaults to dbutils.secrets on Databricks.
SecretGetter = Callable[[RunContext, str, str], str]


def _dbutils_secret(ctx: RunContext, scope: str, key: str) -> str:
    from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

    return DBUtils(ctx.spark).secrets.get(scope=scope, key=key)


_getter: SecretGetter = _dbutils_secret


def set_secret_getter(getter: Optional[SecretGetter]) -> None:
    global _getter
    _getter = getter or _dbutils_secret


def get_secret(ctx: RunContext, key: str) -> str:
    return _getter(ctx, ctx.param("SECRET_SCOPE", DEFAULT_SCOPE) or DEFAULT_SCOPE, key)
