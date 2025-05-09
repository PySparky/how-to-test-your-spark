from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from unittest.mock import patch

def spark_read_table(spark):
    return spark.read.table("hello").select(
        "column1"
    )



# mock all
def test_spark_read_table():
    # Create a mock SparkSession
    mock_spark = MagicMock(spec=SparkSession)

    # Create a mock DataFrame
    mock_df = MagicMock()
    mock_selected_df = MagicMock()

    # Chain the mocks: spark.read.table("hello").select("column1")
    mock_spark.read.table.return_value = mock_df
    mock_df.select.return_value = mock_selected_df

    result = spark_read_table(mock_spark)

    # Assertions
    mock_spark.read.table.assert_called_once_with("hello")
    mock_df.select.assert_called_once_with("column1")
    assert result == mock_selected_df

# Thanks! That error means the mock didn't actually override the call to spark.read.table("hello") — so Spark still tried to read a real table called "hello", which doesn't exist.
# This happens because spark.read is a Java object (DataFrameReader), and patch.object doesn’t work as expected on PySpark Java gateways.
# def test_spark_read_table_select_column1(spark):
#     # Create a test DataFrame
#     test_data = [("a", 1), ("b", 2)]
#     test_df = spark.createDataFrame(test_data, ["column1", "column2"])

#     # Patch spark.read.table to return the test_df
#     with patch.object(spark.read, 'table', return_value=test_df):
#         result_df = spark_read_table(spark)

#         # Collect result
#         result = result_df.collect()

#         # Expect only column1 to be selected
#         assert result == [row for row in spark.createDataFrame([("a",), ("b",)], ["column1"]).collect()]

def test_spark_read_table3(spark):
    # Create mock input DataFrame
    mock_df = spark.createDataFrame([("Alice", 1), ("Bob", 2)], ["name", "column1"])

    # Mock spark.read.table to return the mock DataFrame
    mock_spark = MagicMock()
    mock_spark.read.table.return_value = mock_df

    # Run the function
    result_df = spark_read_table(mock_spark)

    # Create expected DataFrame and compare
    expected_df = spark.createDataFrame([(1,), (2,)], ["column1"])
    assert result_df.collect() == expected_df.collect()