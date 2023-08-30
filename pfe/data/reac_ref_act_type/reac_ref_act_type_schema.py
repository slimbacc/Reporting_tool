""" basic modules """
from datetime import date
from pyspark.sql.types import StringType, StructField, StructType, IntegerType
from pyspark.sql.types import DateType
# reac_ref_act_type fields

n_applic_infq: int = 'n_applic_infq'
l_act_type: str = 'l_act_type'
c_act_type: str = 'c_act_type'
eventdate: date = 'eventdate'


reac_ref_act_type_SCHEMA: StructType = StructType(
    [
        StructField(n_applic_infq, IntegerType()),
        StructField(l_act_type, StringType()),
        StructField(c_act_type, StringType()),
        StructField(eventdate, DateType())
    ]
)
