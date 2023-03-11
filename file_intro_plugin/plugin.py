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
        LOGGER.info("Running Setup")

    def run(self) -> None:

        # Read csv results file
        results_df = pandas.read_csv(self.model.files[0], index_col="Collection #")
        LOGGER.info(f"OUTPUT:\n {results_df}")
        
        # Get the target value, which is the value used to evaluate the results
        average_rms = results_df.loc["Averages", "RMS (pix)"]

        LOGGER.info(f"Average RMS:\n {average_rms}")

        # Value thresholds for the various levels of quality
        acceptable_threshold = 3
        good_threshold = 1
        excellent_threshold = 0.5

        # Quality levels
        if average_rms <= 3:
            results_quality = "acceptable"
        else:
            results_quality = "bad"

        if average_rms <= 1:
            results_quality = "good"

        if average_rms <= 0.5:
            results_quality = "great"

        LOGGER.info(f"The results were {results_quality}!")

    def stop(self) -> None:
        LOGGER.info("Running Stop")