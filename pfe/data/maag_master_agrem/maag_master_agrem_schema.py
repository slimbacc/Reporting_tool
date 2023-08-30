"""imports"""
from datetime import date

from pyspark.sql.types import (
    BooleanType,
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

n_applic_infq: int = "n_applic_infq"
c_act_type: str = "c_act_type"
c_mast_agrem_refer: str = "c_mast_agrem_refer"
c_act_mng_stg: str = "c_act_mng_stg"
d_act_mng_stg: date = "d_act_mng_stg"
c_act_mng_step: str = "c_act_mng_step"
d_act_mng_step: date = "d_act_mng_step"
d_plan_init_end: date = "d_plan_init_end"
d_plan_actua_end: date = "d_plan_actua_end"
c_ident_loca: str = "c_ident_loca"
c_prnt_mast_agrem_refer: int = "c_prnt_mast_agrem_refer"
l_mast_agrem_name: str = "l_mast_agrem_name"
n_mast_agrem_vali_per: int = "n_mast_agrem_vali_per"
d_crdt_committee_aprv: date = "d_crdt_committee_aprv"
D_NOTIF: date = "D_NOTIF"
C_CRCY: str = "C_CRCY"
C_FUND_CHANL: str = "C_FUND_CHANL"
B_POOL: bool = "B_POOL"
cenmes: int = "cenmes"
cnuprj: str = "cnuprj"
nsssdc: int = "nsssdc"
eventdate: date = "eventdate"


maag_master_agrem_SCHEMA: StructType = StructType(
    [
        StructField(n_applic_infq, IntegerType()),
        StructField(c_act_type, StringType()),
        StructField(c_mast_agrem_refer, StringType()),
        StructField(c_act_mng_stg, StringType()),
        StructField(d_act_mng_stg, DateType()),
        StructField(c_act_mng_step, StringType()),
        StructField(d_act_mng_step, DateType()),
        StructField(d_plan_init_end, DateType()),
        StructField(d_plan_actua_end, DateType()),
        StructField(c_ident_loca, StringType()),
        StructField(c_prnt_mast_agrem_refer, IntegerType()),
        StructField(l_mast_agrem_name, StringType()),
        StructField(n_mast_agrem_vali_per, IntegerType()),
        StructField(d_crdt_committee_aprv, DateType()),
        StructField(D_NOTIF, DateType()),
        StructField(C_CRCY, StringType()),
        StructField(C_FUND_CHANL, StringType()),
        StructField(B_POOL, BooleanType()),
        StructField(cenmes, IntegerType()),
        StructField(cnuprj, StringType()),
        StructField(nsssdc, IntegerType()),
        StructField(eventdate, DateType()),
    ]
)
