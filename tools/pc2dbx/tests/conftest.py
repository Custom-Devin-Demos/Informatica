import os
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[3]
XML_DIR = REPO / "XML"
PLAN = REPO / "databricks" / "PLAN.md"
LAKEHOUSE_SRC = REPO / "databricks" / "src"
EXPORTS = sorted(p for p in XML_DIR.iterdir() if p.is_file())

if str(LAKEHOUSE_SRC) not in sys.path:
    sys.path.insert(0, str(LAKEHOUSE_SRC))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    delta = pytest.importorskip("delta")
    from pyspark.sql import SparkSession

    from biis_lakehouse.context import OWNER_SCHEMAS

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("pc2dbx-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.jars.repositories",
            os.environ.get("SPARK_JARS_REPOSITORIES", "https://maven-central.storage-download.googleapis.com/maven2"),
        )
    )
    session = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    for schema in set(OWNER_SCHEMAS.values()):
        session.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")
    yield session
