def extract(spark, table_name):
    return spark.read.table(table_name)


def test_extract(spark):
    # Create a temporary table for testing
    data = [("Alice", 1), ("Bob", 2)]
    columns = ["name", "id"]
    df = spark.createDataFrame(data, columns)
    df.createOrReplaceTempView("test_table")

    # Call the extract function
    result_df = extract(spark, "test_table")

    # Expected DataFrame
    expected_data = [("Alice", 1), ("Bob", 2)]
    expected_df = spark.createDataFrame(expected_data, columns)

    # Assert the result
    assert result_df.collect() == expected_df.collect()
