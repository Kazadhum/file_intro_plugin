from pydantic import BaseModel
from typing import List, Optional, Dict, Any


class IntrospectionParameters(BaseModel):
    validation_min: Optional[float]
    validation_max: Optional[float]
    use_latest_row: Optional[bool]
    value_row: Optional[str] or Optional[int]
    acceptable_min: Optional[float]
    acceptable_max: Optional[float]
    contains_str: Optional[str]
    does_not_contain_str: Optional[str]

class PluginModel(BaseModel):

    file: str
    # introspection_target_columns: List[Dict[str, Dict[str, Any]]]
    introspection_target_columns: List[Dict[str, IntrospectionParameters]]  