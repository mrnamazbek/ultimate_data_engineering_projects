"""Unit tests for Spark job helper logic (no Spark cluster needed)."""

import sys
import os
from unittest.mock import MagicMock

# Mock pyspark before importing streaming_job so the test runs without Spark
for mod in [
    "pyspark",
    "pyspark.sql",
    "pyspark.sql.functions",
    "pyspark.sql.types",
]:
    sys.modules.setdefault(mod, MagicMock())

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "spark_job"))


def test_jdbc_url_format():
    """JDBC URL should include host, port and db name."""
    import importlib
    import streaming_job

    importlib.reload(streaming_job)
    assert streaming_job.PG_HOST in streaming_job.JDBC_URL
    assert streaming_job.PG_PORT in streaming_job.JDBC_URL
    assert streaming_job.PG_DB in streaming_job.JDBC_URL
    assert streaming_job.JDBC_URL.startswith("jdbc:postgresql://")


def test_jdbc_props_keys():
    """JDBC props must contain user, password and driver."""
    import streaming_job

    assert "user" in streaming_job.JDBC_PROPS
    assert "password" in streaming_job.JDBC_PROPS
    assert "driver" in streaming_job.JDBC_PROPS
