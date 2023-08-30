"""imports"""
from datetime import date
from pyspark.sql.types import StructType, IntegerType, FloatType, DateType
from pyspark.sql.types import StringType, StructField
# maag_raty_linked fields

n_applic_infq: int = 'n_applic_infq'
c_mast_agrem_refer: str = 'c_mast_agrem_refer'
C_M: str = 'C_M'
M_ORIG: float = 'M_ORIG'
M_CONVT_EURO: float = 'M_CONVT_EURO'
M_ORIG_SHAR: float = 'M_ORIG_SHAR'
M_CONVT_EURO_SHAR: float = 'M_CONVT_EURO_SHAR'
eventdate: date = 'eventdate'


maag_raty_linked_SCHEMA: StructType = StructType(
    [
        StructField(n_applic_infq, IntegerType()),
        StructField(c_mast_agrem_refer, StringType()),
        StructField(C_M, StringType()),
        StructField(M_ORIG, FloatType()),
        StructField(M_CONVT_EURO, FloatType()),
        StructField(M_ORIG_SHAR, FloatType()),
        StructField(M_CONVT_EURO_SHAR, FloatType()),
        StructField(eventdate, DateType())
    ]
)
