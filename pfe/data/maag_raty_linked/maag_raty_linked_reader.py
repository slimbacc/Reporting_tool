from pyspark.sql import DataFrame
from pfe.common.reader import (
    create_df_with_schema,
    read_from_parquet,
)
from pfe.data.maag_raty_linked.maag_raty_linked_schema import (
    maag_raty_linked_SCHEMA,
)


class maag_raty_linked_Reader:
    """class"""
    def __init__(self, path: str) -> None:

        self.path = path

    def read(self) -> DataFrame:
        """read function"""
        maag_raty_linked_df: DataFrame = read_from_parquet(
            self.path
        ).select(*maag_raty_linked_SCHEMA.fieldNames())

        return create_df_with_schema(
            maag_raty_linked_df,
            maag_raty_linked_SCHEMA,
        )
