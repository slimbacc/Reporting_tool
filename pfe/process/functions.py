"""imports"""
from pyspark.sql import DataFrame


def rename_common_columns(
        df1: DataFrame,
        df2: DataFrame,
        join_columns: list) -> list:
    """Find common column names between two DataFrames."""
    columns_df1 = set(df1.columns)
    columns_df2 = set(df2.columns)
    common_columns = columns_df1.intersection(columns_df2)
    common_columns = [column for column in common_columns
                      if column not in join_columns[1:]]
    for column_name in common_columns:
        df1 = df1.withColumnRenamed(
            column_name,
            f"{column_name}_{join_columns[0]}"
        )
    return df1
