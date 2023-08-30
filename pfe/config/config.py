"""imports"""
from dataclasses import dataclass


@dataclass
class Config:
    """contains paths of input and output dfs"""
    file_path_maag_master_agrem: str
    file_path_reac_ref_act_type: str
    file_path_maag_repa_rrol_linked: str
    file_path_rtpa_ref_third_party: str
    file_path_maag_raty_linked: str
    output_file_path_processing: str


app_config = Config(
    file_path_maag_master_agrem=(
        "C:/exercice/pfe/data/tables/maag_master_agrem/"
    ),
    file_path_reac_ref_act_type=(
        "C:/exercice/pfe/data/tables/reac_ref_act_type/"
    ),
    file_path_maag_repa_rrol_linked=(
        "C:/exercice/pfe/data/tables/maag_repa_rrol_linked/"
    ),
    file_path_rtpa_ref_third_party=(
        "C:/exercice/pfe/data/tables/rtpa_ref_third_party/"
    ),
    file_path_maag_raty_linked=(
        "C:/exercice/pfe/data/tables/maag_raty_linked/"
    ),
    output_file_path_processing=(
        "C:/exercice/pfe/data/tables/output/"
    )
)
