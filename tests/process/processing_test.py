"""imports"""
from datetime import date, datetime

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from pfe.common.reader import create_df_with_schema, read_from_parquet, write_to_parquet
from pfe.data.maag_master_agrem.maag_master_agrem_schema import maag_master_agrem_SCHEMA
from pfe.data.maag_raty_linked.maag_raty_linked_schema import maag_raty_linked_SCHEMA
from pfe.data.maag_repa_rrol_linked.maag_repa_rrol_linked_schema import (
    maag_repa_rrol_linked_SCHEMA,
)
from pfe.data.output.output_schema import output_SCHEMA
from pfe.data.reac_ref_act_type.reac_ref_act_type_schema import reac_ref_act_type_SCHEMA
from pfe.data.rtpa_ref_third_party.rtpa_ref_third_party_schema import (
    rtpa_ref_third_party_SCHEMA,
)
from pfe.process.processing import ProcessingJobs
from tests.conftest import spark


@pytest.mark.usefixtures("spark")
class TestProcessing:
    def test_create_dataset_processing(self, spark: SparkSession) -> None:
        """create dummy datasets"""
        maag_master_agrem_sample = [
            (
                1,
                "type1",
                "ref1",
                "stage1",
                datetime.strptime("2023-08-01", "%Y-%m-%d"),
                "step1",
                datetime.strptime("2023-08-05", "%Y-%m-%d"),
                datetime.strptime("2023-08-10", "%Y-%m-%d"),
                datetime.strptime("2023-08-15", "%Y-%m-%d"),
                "loc1",
                123,
                "name1",
                5,
                datetime.strptime("2023-08-20", "%Y-%m-%d"),
                datetime.strptime("2023-08-25", "%Y-%m-%d"),
                "USD",
                "channel1",
                True,
                456,
                "proj1",
                789,
                datetime.strptime("2023-08-30", "%Y-%m-%d"),
            ),
            (
                2,
                "type2",
                "ref2",
                "stage2",
                datetime.strptime("2023-08-02", "%Y-%m-%d"),
                "step2",
                datetime.strptime("2023-08-06", "%Y-%m-%d"),
                datetime.strptime("2023-08-11", "%Y-%m-%d"),
                datetime.strptime("2023-08-16", "%Y-%m-%d"),
                "loc2",
                124,
                "name2",
                6,
                datetime.strptime("2023-08-21", "%Y-%m-%d"),
                datetime.strptime("2023-08-26", "%Y-%m-%d"),
                "EUR",
                "channel2",
                False,
                457,
                "proj2",
                790,
                datetime.strptime("2023-09-01", "%Y-%m-%d"),
            ),
            # ... more rows ...
        ]
        maag_raty_linked_sample = [
            (
                1,
                "ref1",
                "C_M_value",
                100.0,
                90.0,
                80.0,
                70.0,
                datetime.strptime("2023-08-01", "%Y-%m-%d"),
            ),
            (
                2,
                "ref2",
                "C_M_value",
                200.0,
                180.0,
                160.0,
                140.0,
                datetime.strptime("2023-08-02", "%Y-%m-%d"),
            ),
            # ... more rows ...
        ]
        maag_repa_rrol_linked_sample = [
            (
                1,
                "ref1",
                "part_ref1",
                "role1",
                datetime.strptime("2023-08-01", "%Y-%m-%d"),
                datetime.strptime("2023-08-05", "%Y-%m-%d"),
                datetime.strptime("2023-08-10", "%Y-%m-%d"),
            ),
            (
                2,
                "ref2",
                "part_ref2",
                "role2",
                datetime.strptime("2023-08-02", "%Y-%m-%d"),
                datetime.strptime("2023-08-06", "%Y-%m-%d"),
                datetime.strptime("2023-08-11", "%Y-%m-%d"),
            ),
            # ... more rows ...
        ]
        reac_ref_act_type_sample = [
            (1, "activity1", "type1", datetime.strptime("2023-08-01", "%Y-%m-%d")),
            (2, "activity2", "type2", datetime.strptime("2023-08-02", "%Y-%m-%d")),
            # ... more rows ...
        ]
        rtpa_ref_third_party_sample = [
            (
                1,
                "ref1",
                "name1",
                123456789,
                100,
                "siren1",
                datetime.strptime("2023-08-01", "%Y-%m-%d"),
                "source1",
            ),
            (
                2,
                "ref2",
                "name2",
                987654321,
                200,
                "siren2",
                datetime.strptime("2023-08-02", "%Y-%m-%d"),
                "source2",
            ),
            # ... more rows ...
        ]

        # Create RDDs from sample data
        maag_master_agrem_rdd = spark.sparkContext.parallelize(maag_master_agrem_sample)
        maag_raty_linked_rdd = spark.sparkContext.parallelize(maag_raty_linked_sample)
        maag_repa_rrol_linked_rdd = spark.sparkContext.parallelize(maag_repa_rrol_linked_sample)
        reac_ref_act_type_rdd = spark.sparkContext.parallelize(reac_ref_act_type_sample)
        rtpa_ref_third_party_rdd = spark.sparkContext.parallelize(rtpa_ref_third_party_sample)

        # Create DataFrames from RDDs using the schemas
        maag_master_agrem_df = spark.createDataFrame(
            maag_master_agrem_rdd, maag_master_agrem_SCHEMA
        )
        reac_ref_act_type_df = spark.createDataFrame(
            reac_ref_act_type_rdd, reac_ref_act_type_SCHEMA
        )
        maag_repa_rrol_linked_df = spark.createDataFrame(
            maag_repa_rrol_linked_rdd, maag_repa_rrol_linked_SCHEMA
        )
        maag_raty_linked_df = spark.createDataFrame(maag_raty_linked_rdd, maag_raty_linked_SCHEMA)
        rtpa_ref_third_party_df = spark.createDataFrame(
            rtpa_ref_third_party_rdd, rtpa_ref_third_party_SCHEMA
        )
        path = "init_path"
        my_instance = ProcessingJobs(path, path, path, path, path, path)

        sample_dataframe = my_instance._create_dataset_processing(
            maag_master_agrem_df,
            reac_ref_act_type_df,
            maag_repa_rrol_linked_df,
            maag_raty_linked_df,
            rtpa_ref_third_party_df,
        )
        expected_data_rdd = [
            Row(
                c_thir_part_refer="part_ref2",
                c_mast_agrem_refer="ref2",
                n_applic_infq_join3=2,
                c_act_type="type2",
                l_act_type="activity2",
                eventdate_join1=datetime.strptime("2023-08-02", "%Y-%m-%d").date(),
                c_act_mng_stg="stage2",
                d_act_mng_stg=datetime.strptime("2023-08-02", "%Y-%m-%d").date(),
                c_act_mng_step="step2",
                d_act_mng_step=datetime.strptime("2023-08-06", "%Y-%m-%d").date(),
                d_plan_init_end=datetime.strptime("2023-08-11", "%Y-%m-%d").date(),
                d_plan_actua_end=datetime.strptime("2023-08-16", "%Y-%m-%d").date(),
                c_ident_loca="loc2",
                c_prnt_mast_agrem_refer=124,
                l_mast_agrem_name="name2",
                n_mast_agrem_vali_per=6,
                d_crdt_committee_aprv=(datetime.strptime("2023-08-21", "%Y-%m-%d").date()),
                D_NOTIF=datetime.strptime("2023-08-26", "%Y-%m-%d").date(),
                C_CRCY="EUR",
                C_FUND_CHANL="channel2",
                B_POOL=False,
                cenmes=457,
                cnuprj="proj2",
                nsssdc=790,
                eventdate_join2=datetime.strptime("2023-09-01", "%Y-%m-%d").date(),
                c_role="role2",
                d_str_actr_agrem=(datetime.strptime("2023-08-02", "%Y-%m-%d").date()),
                d_end_actr_agrem=(datetime.strptime("2023-08-06", "%Y-%m-%d").date()),
                eventdate_join3=datetime.strptime("2023-08-11", "%Y-%m-%d").date(),
                n_applic_infq_join4=2,
                C_M="C_M_value",
                M_ORIG=200.0,
                M_CONVT_EURO=180.0,
                M_ORIG_SHAR=160.0,
                M_CONVT_EURO_SHAR=140.0,
                eventdate_join4=datetime.strptime("2023-08-02", "%Y-%m-%d").date(),
                n_applic_infq=None,
                l_thir_part_name=None,
                n_ident_ret=None,
                numera=None,
                nsiren=None,
                eventdate=None,
                source=None,
            ),
            Row(
                c_thir_part_refer="part_ref1",
                c_mast_agrem_refer="ref1",
                n_applic_infq_join3=1,
                c_act_type="type1",
                l_act_type="activity1",
                eventdate_join1=datetime.strptime("2023-08-01", "%Y-%m-%d").date(),
                c_act_mng_stg="stage1",
                d_act_mng_stg=datetime.strptime("2023-08-01", "%Y-%m-%d").date(),
                c_act_mng_step="step1",
                d_act_mng_step=datetime.strptime("2023-08-05", "%Y-%m-%d").date(),
                d_plan_init_end=datetime.strptime("2023-08-10", "%Y-%m-%d").date(),
                d_plan_actua_end=(datetime.strptime("2023-08-15", "%Y-%m-%d").date()),
                c_ident_loca="loc1",
                c_prnt_mast_agrem_refer=123,
                l_mast_agrem_name="name1",
                n_mast_agrem_vali_per=5,
                d_crdt_committee_aprv=(datetime.strptime("2023-08-20", "%Y-%m-%d").date()),
                D_NOTIF=datetime.strptime("2023-08-25", "%Y-%m-%d").date(),
                C_CRCY="USD",
                C_FUND_CHANL="channel1",
                B_POOL=True,
                cenmes=456,
                cnuprj="proj1",
                nsssdc=789,
                eventdate_join2=datetime.strptime("2023-08-30", "%Y-%m-%d").date(),
                c_role="role1",
                d_str_actr_agrem=(datetime.strptime("2023-08-01", "%Y-%m-%d").date()),
                d_end_actr_agrem=(datetime.strptime("2023-08-05", "%Y-%m-%d").date()),
                eventdate_join3=datetime.strptime("2023-08-10", "%Y-%m-%d").date(),
                n_applic_infq_join4=1,
                C_M="C_M_value",
                M_ORIG=100.0,
                M_CONVT_EURO=90.0,
                M_ORIG_SHAR=80.0,
                M_CONVT_EURO_SHAR=70.0,
                eventdate_join4=datetime.strptime("2023-08-01", "%Y-%m-%d").date(),
                n_applic_infq=None,
                l_thir_part_name=None,
                n_ident_ret=None,
                numera=None,
                nsiren=None,
                eventdate=None,
                source=None,
            ),
        ]
        expected_df = spark.createDataFrame(expected_data_rdd, output_SCHEMA)

        # Check if the two dataframes are the same
        assert_df_equality(sample_dataframe, expected_df, ignore_row_order=True)

    def test_write_to_parquet(self, spark: SparkSession) -> None:
        """test write_to_parquet function"""
        data = [
            {
                "col1": 1,
                "col2": "a",
                "col3": None,
                "col4": 1.23,
                "col5": date(2023, 8, 1),
            },
            {"col1": 2, "col2": "b", "col3": 10.5, "col4": None, "col5": None},
            {
                "col1": 3,
                "col2": "c",
                "col3": 15.7,
                "col4": 2.56,
                "col5": date(2023, 8, 15),
            },
        ]
        # Define schema with additional columns
        schema = StructType(
            [
                StructField("col1", IntegerType(), nullable=True),
                StructField("col2", StringType(), nullable=True),
                StructField("col3", FloatType(), nullable=True),
                StructField("col4", DoubleType(), nullable=True),
                StructField("col5", DateType(), nullable=True),
            ]
        )
        # Create DataFrame
        df_write = spark.createDataFrame(data, schema=schema)

        # Write test data to a temporary Parquet file
        tmp_dir_path = "C:/exercice/tests/test_temp/"
        write_to_parquet(df_write, tmp_dir_path)

        # Read the written data using your function
        df_read = spark.read.parquet(tmp_dir_path)
        df_read = create_df_with_schema(df_read, schema)
        # Data integrity assertions
        assert_df_equality(df_read, df_write, ignore_row_order=True)

    def test_read_from_parquet(self, spark: SparkSession) -> None:
        """test read_from_parquet function"""
        data = [
            {
                "col1": 1,
                "col2": "a",
                "col3": None,
                "col4": 1.23,
                "col5": date(2023, 8, 1),
            },
            {"col1": 2, "col2": "b", "col3": 10.5, "col4": None, "col5": None},
            {
                "col1": 3,
                "col2": "c",
                "col3": 15.7,
                "col4": 2.56,
                "col5": date(2023, 8, 15),
            },
        ]
        # Define schema with additional columns
        schema = StructType(
            [
                StructField("col1", IntegerType(), nullable=True),
                StructField("col2", StringType(), nullable=True),
                StructField("col3", FloatType(), nullable=True),
                StructField("col4", DoubleType(), nullable=True),
                StructField("col5", DateType(), nullable=True),
            ]
        )
        # Create DataFrame
        df_write = spark.createDataFrame(data, schema=schema)

        # Write test data to a temporary Parquet file
        tmp_dir_path = "C:/exercice/tests/test_temp/"
        df_write.write.mode("overwrite").parquet(tmp_dir_path)

        # Read the written data using your function
        df_read = read_from_parquet(tmp_dir_path)
        df_read = create_df_with_schema(df_read, schema)

        # Data integrity assertions
        assert_df_equality(df_read, df_write, ignore_row_order=True)
