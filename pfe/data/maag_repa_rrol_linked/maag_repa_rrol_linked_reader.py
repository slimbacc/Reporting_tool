"""import necessary libraries and classes"""
from pyspark.sql import DataFrame
from pfe.common.reader import (
    create_df_with_schema,
    read_from_parquet,
)
from pfe.data.maag_repa_rrol_linked.maag_repa_rrol_linked_schema import (
    maag_repa_rrol_linked_SCHEMA,
)


class maag_repa_rrol_linked_Reader:
    """Class representing something"""
    def __init__(self, path: str) -> None:

        self.path = path

    def read(self) -> DataFrame:
        """function returning dataframe with given schema"""
        maag_repa_rrol_linked_df: DataFrame = read_from_parquet(
            self.path
        ).select(*maag_repa_rrol_linked_SCHEMA.fieldNames())

        return create_df_with_schema(
            maag_repa_rrol_linked_df,
            maag_repa_rrol_linked_SCHEMA,
        )
