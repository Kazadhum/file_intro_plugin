from pydantic import BaseModel
from typing import List


class PluginModel(BaseModel):

    files: List[str]
    value_row: str
    value_column: str
    acceptable_threshold: float
    good_threshold: float
    great_threshold: float
    required_quality_level: str
    use_latest_row: bool
    use_latest_column: bool