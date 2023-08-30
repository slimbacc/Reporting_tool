"""imports"""
from pyspark.sql import DataFrame
from pfe.common.reader import (
    create_df_with_schema,
    read_from_parquet,
)
from pfe.data.maag_master_agrem.maag_master_agrem_schema import (
    maag_master_agrem_SCHEMA
)


class maag_master_agrem_Reader:
    """tab init path"""
    def __init__(self, path: str) -> None:

        self.path = path

    def read(self) -> DataFrame:
        """tab reader"""
        maag_master_agrem_df: DataFrame = read_from_parquet(
            self.path
        ).select(*maag_master_agrem_SCHEMA.fieldNames())

        return create_df_with_schema(
            maag_master_agrem_df,
            maag_master_agrem_SCHEMA,
        )
