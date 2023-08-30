"""import necessary libraries and classes"""
from pyspark.sql import DataFrame
from pfe.common.reader import (
    create_df_with_schema,
    read_from_parquet,
)

from pfe.data.rtpa_ref_third_party.rtpa_ref_third_party_schema import (
    rtpa_ref_third_party_SCHEMA,
)


class rtpa_ref_third_party_Reader:
    """Class representing something"""
    def __init__(self, path: str) -> None:

        self.path = path

    def read(self) -> DataFrame:
        """function returning dataframe with given schema"""
        rtpa_ref_third_party_df: DataFrame = read_from_parquet(
            self.path
        ).select(*rtpa_ref_third_party_SCHEMA.fieldNames())

        return create_df_with_schema(
            rtpa_ref_third_party_df,
            rtpa_ref_third_party_SCHEMA,
        )
