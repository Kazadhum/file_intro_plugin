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
        results_df = pandas.read_csv(self.model.file, index_col="Collection #")
        LOGGER.info(f"OUTPUT:\n {results_df}")
        
        

        # Check for value abnormalities
        verification_flag = True

        if self.model.columns_to_verify != None:
            if self.model.verification_comparison_operator == ">":
                for column in self.model.columns_to_verify:
                    LOGGER.info(f"Verifying column '{column}'")
                    column_max = results_df[column].max()

                    if column_max > self.model.column_verification_threshold:
                        verification_flag = False

            if self.model.verification_comparison_operator == "<":
                for column in self.model.columns_to_verify:
                    LOGGER.info(f"Verifying column '{column}'")
                    
                    column_min = results_df[column].min()

                    if column_min < self.model.column_verification_threshold:
                        verification_flag = False

            LOGGER.info(f"Are the values verified? {verification_flag}")

            if not verification_flag:
                LOGGER.error("Abnormal values! Get new values!")
                exit()
        
        
        
        
        # Decide which value to use for comparison according to the Rigelfile
        if (self.model.use_latest_row and self.model.use_latest_column):
            
            value_to_compare = results_df.loc[results_df.index[-1], results_df.columns[-1]]

            LOGGER.info(f"Row: {results_df.index[-1]} | Column: {results_df.columns[-1]}\n {value_to_compare}")

        else:
            if self.model.use_latest_row:
                
                value_to_compare = results_df.loc[results_df.index[-1], self.model.value_column]

                LOGGER.info(f"Row: {results_df.index[-1]} | Column: {self.model.value_column}\n {value_to_compare}")

            elif self.model.use_latest_column:
                
                value_to_compare = results_df.loc[self.model.value_row, results_df.columns[-1]]
                
                LOGGER.info(f"Row: {self.model.value_row} | Column: {results_df.columns[-1]}\n {value_to_compare}")

            else:
                
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


        # Set "acceptable" as default requirement in case none is passed
        if self.model.required_quality_level == None:
            self.model.required_quality_level = "acceptable"

        #Comparing quality level vs. required quality level    
        LOGGER.info(f"Required quality level: {self.model.required_quality_level}")

        if self.model.required_quality_level == "acceptable":
            if results_quality != "bad":
                LOGGER.info("Test passed!")
            else:
                LOGGER.info("Test failed!")

        elif self.model.required_quality_level == "good":
            if (results_quality == "good" or results_quality == "great"):
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