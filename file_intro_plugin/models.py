from pydantic import BaseModel
from typing import List


class PluginModel(BaseModel):

    files: List[str]