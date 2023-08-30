""" basic modules """
from datetime import date
from pyspark.sql.types import StringType, StructField, StructType, IntegerType
from pyspark.sql.types import DateType, LongType
# rtpa_ref_third_party fields

n_applic_infq: int = 'n_applic_infq'
c_thir_part_refer: str = 'c_thir_part_refer'
l_thir_part_name: str = 'l_thir_part_name'
n_ident_ret: int = 'n_ident_ret'
numera: int = 'numera'
nsiren: str = 'nsiren'
eventdate: date = 'eventdate'
source: str = 'source'


rtpa_ref_third_party_SCHEMA: StructType = StructType(
    [
        StructField(n_applic_infq, IntegerType()),
        StructField(c_thir_part_refer, StringType()),
        StructField(l_thir_part_name, StringType()),
        StructField(n_ident_ret, LongType()),
        StructField(numera, IntegerType()),
        StructField(nsiren, StringType()),
        StructField(eventdate, DateType()),
        StructField(source, StringType())
    ]
)
