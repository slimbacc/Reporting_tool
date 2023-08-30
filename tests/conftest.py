"""imports"""
import pytest
from pyspark.sql import SparkSession


# Define a fixture named "spark"
@pytest.fixture(scope="session")
def spark():
    """create spark session for test file"""
    spark = SparkSession.builder.master("local[*]") \
        .config("spark.driver.memory", "3G") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.execution.arrow.enabled", "true") \
        .config("spark.sql.execution.arrow.fallback.enabled", "true") \
        .config("spark.sql.repl.eagerEval.enabled", "true") \
        .appName("Spark session for local tests") \
        .getOrCreate()
    yield spark
    spark.stop()
