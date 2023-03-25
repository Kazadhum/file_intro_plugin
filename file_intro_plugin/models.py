from pydantic import BaseModel
from typing import List, Optional, Dict, Any


class PluginModel(BaseModel):




    file: str
    # value_row: str
    # value_column: str
    # acceptable_threshold: float
    # good_threshold: float
    # great_threshold: float
    # required_quality_level: Optional[str]
    # use_latest_row: bool
    # use_latest_column: bool
    # columns_to_verify: Optional[List[str]]
    # column_verification_upper_threshold: Optional[float]
    # column_verification_lower_threshold: Optional[float]

    # verification_comparison_operator: str
    introspection_target_columns: List[Dict[str, Dict[str, Any]]]