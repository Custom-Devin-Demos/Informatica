import os

import pytest

from biis_lakehouse.context import OWNER_SCHEMAS, RunContext


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("biis-lakehouse-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        # The default 1g driver heap thrashes in full GC once the CPM and LES suites share one session.
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        # Maven Central rate-limits some CI/VM egress; the GCS mirror is tried first.
        .config(
            "spark.jars.repositories",
            os.environ.get("SPARK_JARS_REPOSITORIES", "https://maven-central.storage-download.googleapis.com/maven2"),
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    for schema in set(OWNER_SCHEMAS.values()):
        session.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    yield session
    session.stop()


@pytest.fixture
def ctx(spark, tmp_path):
    """Fresh RunContext per test: local landing zone under tmp_path, no Unity Catalog prefix."""
    for schema in set(OWNER_SCHEMAS.values()):
        for t in spark.catalog.listTables(schema):
            spark.sql(f"DROP TABLE IF EXISTS {schema}.{t.name}")
    return RunContext(spark=spark, catalog=None, landing_root=str(tmp_path / "landing"))
