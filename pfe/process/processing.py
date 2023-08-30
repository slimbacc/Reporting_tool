"""Imports"""
from pyspark.sql import DataFrame
from pfe.common.reader import write_to_parquet
from pfe.process.functions import rename_common_columns
from pfe.config.config import app_config
from pfe.data.maag_master_agrem.maag_master_agrem_reader import (
    maag_master_agrem_Reader,
)
from pfe.data.reac_ref_act_type.reac_ref_act_type_reader import (
    reac_ref_act_type_Reader,
)
from pfe.data.maag_repa_rrol_linked.maag_repa_rrol_linked_reader import (
    maag_repa_rrol_linked_Reader
)
from pfe.data.maag_raty_linked.maag_raty_linked_reader import (
    maag_raty_linked_Reader
)
from pfe.data.rtpa_ref_third_party.rtpa_ref_third_party_reader import (
    rtpa_ref_third_party_Reader
)


class ProcessingJobs:
    """init processing job"""
    def __init__(
        self,
        maag_master_agrem_input_path: str,
        reac_ref_act_type_input_path: str,
        maag_repa_rrol_linked_input_path: str,
        maag_raty_linked_input_path: str,
        rtpa_ref_third_party_input_path: str,
        processing_output_path: str,
    ) -> None:

        self.maag_master_agrem_input_path: str = maag_master_agrem_input_path
        self.reac_ref_act_type_input_path: str = reac_ref_act_type_input_path
        self.maag_repa_rrol_linked_input_path: str = (
            maag_repa_rrol_linked_input_path
        )
        self.maag_raty_linked_input_path: str = maag_raty_linked_input_path
        self.rtpa_ref_third_party_input_path: str = (
            rtpa_ref_third_party_input_path
        )
        self.processing_output_path: str = processing_output_path

    def run(self) -> None:
        """processing job"""
        maag_master_agrem_df: DataFrame = (
            self._get_data_from_maag_master_agrem(
                self.maag_master_agrem_input_path
            )
        )
        reac_ref_act_type_df: DataFrame = (
            self._get_data_from_reac_ref_act_type(
                self.reac_ref_act_type_input_path
            )
        )
        maag_repa_rrol_linked_df: DataFrame = (
            self._get_data_from_maag_repa_rrol_linked(
                self.maag_repa_rrol_linked_input_path
            )
        )
        maag_raty_linked_df: DataFrame = (
            self._get_data_from_maag_raty_linked(
                self.maag_raty_linked_input_path
            )
        )
        rtpa_ref_third_party_df: DataFrame = (
            self._get_data_from_rtpa_ref_third_party(
                self.rtpa_ref_third_party_input_path
            )
        )
        final_dataset: DataFrame = self._create_dataset_processing(
            maag_master_agrem_df,
            reac_ref_act_type_df,
            maag_repa_rrol_linked_df,
            maag_raty_linked_df,
            rtpa_ref_third_party_df
            )
        self._write_dataset_to_parquet(
            final_dataset,
            self.processing_output_path
        )

    def _get_data_from_maag_master_agrem(self, path: str) -> DataFrame:
        maag_master_agrem_reader: maag_master_agrem_Reader = (
            maag_master_agrem_Reader(path)
        )
        maag_master_agrem = maag_master_agrem_reader.read()
        return maag_master_agrem

    def _get_data_from_reac_ref_act_type(self, path: str) -> DataFrame:
        reac_ref_act_type_reader: reac_ref_act_type_Reader = (
            reac_ref_act_type_Reader(path)
        )
        reac_ref_act_type = reac_ref_act_type_reader.read()
        return reac_ref_act_type

    def _get_data_from_maag_repa_rrol_linked(self, path: str) -> DataFrame:
        maag_repa_rrol_linked_reader: maag_repa_rrol_linked_Reader = (
            maag_repa_rrol_linked_Reader(path)
        )
        maag_repa_rrol_linked = maag_repa_rrol_linked_reader.read()
        return maag_repa_rrol_linked

    def _get_data_from_maag_raty_linked(self, path: str) -> DataFrame:
        maag_raty_linked_reader: maag_raty_linked_Reader = (
            maag_raty_linked_Reader(path)
        )
        maag_raty_linked = maag_raty_linked_reader.read()
        return maag_raty_linked

    def _get_data_from_rtpa_ref_third_party(self, path: str) -> DataFrame:
        rtpa_ref_third_party_reader: rtpa_ref_third_party_Reader = (
            rtpa_ref_third_party_Reader(path)
        )
        rtpa_ref_third_party = rtpa_ref_third_party_reader.read()
        return rtpa_ref_third_party

    def _create_dataset_processing(
            self,
            maag_master_agrem_df: DataFrame,
            reac_ref_act_type_df: DataFrame,
            maag_repa_rrol_linked_df: DataFrame,
            maag_raty_linked_df: DataFrame,
            rtpa_ref_third_party_df: DataFrame) -> DataFrame:
        join_columns1 = ["join1", "c_act_type", "n_applic_infq"]
        join_columns2 = ["join2", "c_mast_agrem_refer", "n_applic_infq"]
        join_columns3 = ["join3", "c_mast_agrem_refer"]
        join_columns4 = ["join4", 'c_thir_part_refer']
        # Rename duplicate columns other than join columns
        reac_ref_act_type_df = rename_common_columns(
            reac_ref_act_type_df,
            maag_master_agrem_df,
            join_columns1
        )
        # Perform the left join using join_columns1
        join1_df = reac_ref_act_type_df.join(
            maag_master_agrem_df,
            on=join_columns1[1:],
            how="left"
        )
        print("JOIN 1 COMPLETED")
        # write_to_parquet(join1_df, output_path + "join1_df/")
        # Rename duplicate columns other than join columns
        join1_df = rename_common_columns(
            join1_df,
            maag_repa_rrol_linked_df,
            join_columns2
        )
        # Perform the left join using join_columns2
        join2_df = join1_df.join(
            maag_repa_rrol_linked_df,
            on=join_columns2[1:],
            how="left"
        )
        print("JOIN 2 COMPLETED")
        # write_to_parquet(join2_df, output_path + "join2_df/")
        # Rename duplicate columns other than join columns
        join2_df = rename_common_columns(
            join2_df,
            maag_raty_linked_df,
            join_columns3
        )
        # Perform the left join using join_columns3
        join3_df = join2_df.join(
            maag_raty_linked_df,
            on=join_columns3[1:],
            how="left"
        )
        print("JOIN 3 COMPLETED")
        # write_to_parquet(join3_df, output_path + "join3_df/")
        # Rename column
        join3_df = join3_df.withColumnRenamed(
            "c_part_refer",
            "c_thir_part_refer"
        )
        # Rename duplicate columns other than join columns
        join3_df = rename_common_columns(
            join3_df,
            rtpa_ref_third_party_df,
            join_columns4
        )
        # Perform the left join using join_columns4
        join4_df = join3_df.join(
            rtpa_ref_third_party_df,
            on=join_columns4[1:],
            how="left"
        )
        print("JOIN 4 COMPLETED")
        return join4_df

    def _write_dataset_to_parquet(
            self,
            df: DataFrame,
            output_path: str) -> None:
        write_to_parquet(df, output_path)


def run_job() -> None:
    """run the processing job"""
    file_path_maag_master_agrem = app_config.file_path_maag_master_agrem
    file_path_reac_ref_act_type = app_config.file_path_reac_ref_act_type
    file_path_maag_repa_rrol_linked = (
        app_config.file_path_maag_repa_rrol_linked
    )
    file_path_maag_raty_linked = app_config.file_path_maag_raty_linked
    file_path_rtpa_ref_third_party = app_config.file_path_rtpa_ref_third_party
    output_file_path_processing = app_config.output_file_path_processing

    maag_master_agrem_path: str = file_path_maag_master_agrem
    reac_ref_act_type_path: str = file_path_reac_ref_act_type
    maag_repa_rrol_linked_path: str = file_path_maag_repa_rrol_linked
    maag_raty_linked_path: str = file_path_maag_raty_linked
    rtpa_ref_third_party_path: str = file_path_rtpa_ref_third_party
    processing_output_path: str = output_file_path_processing

    job: ProcessingJobs = ProcessingJobs(
        maag_master_agrem_path,
        reac_ref_act_type_path,
        maag_repa_rrol_linked_path,
        maag_raty_linked_path,
        rtpa_ref_third_party_path,
        processing_output_path
    )
    job.run()
