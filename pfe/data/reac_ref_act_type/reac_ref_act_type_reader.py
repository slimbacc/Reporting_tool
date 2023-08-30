"""import necessary libraries and classes"""
from pyspark.sql import DataFrame
from pfe.common.reader import (
    create_df_with_schema,
    read_from_parquet,
)
from pfe.data.reac_ref_act_type.reac_ref_act_type_schema import (
    reac_ref_act_type_SCHEMA,
)


class reac_ref_act_type_Reader:
    """Class representing something"""
    def __init__(self, path: str) -> None:

        self.path = path

    def read(self) -> DataFrame:
        """function returning dataframe with given schema"""
        reac_ref_act_type_df: DataFrame = read_from_parquet(
            self.path
        ).select(*reac_ref_act_type_SCHEMA.fieldNames())

        return create_df_with_schema(
            reac_ref_act_type_df,
            reac_ref_act_type_SCHEMA,
        )
