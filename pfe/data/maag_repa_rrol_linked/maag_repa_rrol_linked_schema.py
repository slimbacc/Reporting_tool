"""imports"""
from datetime import date
from pyspark.sql.types import StringType, StructField, StructType, IntegerType
from pyspark.sql.types import DateType
# maag_repa_rrol_linked fields

n_applic_infq: int = 'n_applic_infq'
c_mast_agrem_refer: str = 'c_mast_agrem_refer'
c_part_refer: str = 'c_part_refer'
c_role: str = 'c_role'
d_str_actr_agrem: date = 'd_str_actr_agrem'
d_end_actr_agrem: date = 'd_end_actr_agrem'
eventdate: date = 'eventdate'


maag_repa_rrol_linked_SCHEMA: StructType = StructType(
    [
        StructField(n_applic_infq, IntegerType()),
        StructField(c_mast_agrem_refer, StringType()),
        StructField(c_part_refer, StringType()),
        StructField(c_role, StringType()),
        StructField(d_str_actr_agrem, DateType()),
        StructField(d_end_actr_agrem, DateType()),
        StructField(eventdate, DateType())
    ]
)
