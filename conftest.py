import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark():
    spark = SparkSession.builder.appName("Testing PySpark Example").config("spark.sql.shuffle.partitions", "1").getOrCreate()
    yield spark
    spark.stop()
