from rigel.loggers import get_logger
from rigel.plugins import Plugin as PluginBase
from rigel.models.application import Application
from rigel.models.builder import ModelBuilder
from rigel.models.plugin import PluginRawData
from rigel.models.rigelfile import RigelfileGlobalData
from typing import Any, Dict
from .models import PluginModel
import pandas


LOGGER = get_logger()

class FileIntrospectionPlugin(PluginBase):

    def __init__(
        self,
        raw_data: PluginRawData,
        global_data: RigelfileGlobalData,
        application: Application,
        providers_data: Dict[str, Any]
    ) -> None:
        super().__init__(
            raw_data,
            global_data,
            application,
            providers_data
        )

        self.model = ModelBuilder(PluginModel).build([], self.raw_data)

    def setup(self) -> None:
        # LOGGER.info("Running Setup") #Debug line
        pass

    def run(self) -> None:

        # Read csv results file
        results_df = pandas.read_csv(self.model.files[0], index_col="Collection #")
        LOGGER.info(f"OUTPUT:\n {results_df}")
        
        # Get the target value, which is the value used to evaluate the results
        value_to_compare = results_df.loc[self.model.value_row, self.model.value_column]

        LOGGER.info(f"Row:{self.model.value_row} | Column: {self.model.value_column}\n {value_to_compare}")

        # Quality levels, values defined in Rigelfile
        if value_to_compare <= self.model.acceptable_threshold:
            results_quality = "acceptable"
        else:
            results_quality = "bad"

        if value_to_compare <= self.model.good_threshold:
            results_quality = "good"

        if value_to_compare <= self.model.great_threshold:
            results_quality = "great"

        LOGGER.info(f"The results were {results_quality}!")

        #Comparing quality level vs. required quality level

        LOGGER.info(f"Required quality level: {self.model.required_quality_level}")

        if self.model.required_quality_level == "acceptable":
            if results_quality != "bad":
                LOGGER.info("Test passed!")
            else:
                LOGGER.info("Test failed!")

        elif self.model.required_quality_level == "good":
            if (results_quality == "acceptable" or results_quality == "good"):
                LOGGER.info("Test passed!")
            else:
                LOGGER.info("Test failed!")

        elif self.model.required_quality_level == "great":
            if results_quality == "great":
                LOGGER.info("Test passed!")
            else:
                LOGGER.info("Test failed!")

    def stop(self) -> None:
        # LOGGER.info("Running Stop") # Debug line
        pass