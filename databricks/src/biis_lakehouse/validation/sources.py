"""Readers for the legacy side of a reconciliation."""
from __future__ import annotations

from typing import Callable, Optional

from pyspark.sql import DataFrame, SparkSession

ORACLE_DRIVER = "oracle.jdbc.OracleDriver"
SecretGetter = Callable[[str, str], str]


def dbutils_secret_getter(spark: SparkSession) -> SecretGetter:
    from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]

    dbutils = DBUtils(spark)
    return lambda scope, key: dbutils.secrets.get(scope=scope, key=key)


def oracle_jdbc_options(
    url: str,
    *,
    dbtable: Optional[str] = None,
    query: Optional[str] = None,
    secret_scope: str = "biis",
    user_key: str = "oracle_user",
    password_key: str = "oracle_password",
    get_secret: SecretGetter,
    fetchsize: int = 10000,
) -> dict[str, str]:
    """Spark JDBC options for an Oracle extract; credentials always come from the secret scope."""
    if (dbtable is None) == (query is None):
        raise ValueError("pass exactly one of dbtable or query")
    opts = {
        "url": url,
        "driver": ORACLE_DRIVER,
        "user": get_secret(secret_scope, user_key),
        "password": get_secret(secret_scope, password_key),
        "fetchsize": str(fetchsize),
        # Oracle DATE carries time; keep it as timestamp rather than truncating to date.
        "oracle.jdbc.mapDateToTimestamp": "true",
    }
    opts["dbtable" if dbtable else "query"] = dbtable or query  # type: ignore[assignment]
    return opts


def read_oracle(spark: SparkSession, url: str, *, get_secret: Optional[SecretGetter] = None, **kwargs) -> DataFrame:
    """Read an Oracle table/query over JDBC (requires the ojdbc jar on the cluster)."""
    opts = oracle_jdbc_options(url, get_secret=get_secret or dbutils_secret_getter(spark), **kwargs)
    return spark.read.format("jdbc").options(**opts).load()


def read_csv_extract(spark: SparkSession, path: str, *, sep: str = ",", header: bool = True, schema: Optional[str] = None) -> DataFrame:
    """Read a CSV unload (e.g. SQL*Plus spool / SQL Developer export). All columns are strings unless ``schema`` is given."""
    reader = spark.read.option("header", str(header).lower()).option("sep", sep).option("quote", '"').option("escape", '"')
    if schema:
        reader = reader.schema(schema)
    return reader.csv(path)
