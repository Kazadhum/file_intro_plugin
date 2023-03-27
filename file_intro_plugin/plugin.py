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

        pass

    def run(self) -> None:

        # Read csv results file
        results_df = pandas.read_csv(self.model.file, index_col="Collection #")
        
        LOGGER.info(f"\n{40*'#'}\nFile Introspection\n{40*'#'}\n\n{40*'-'}\nCSV File:\n{40*'-'}\n\n {results_df}\n")

        rows_in_dataframe = results_df.index.tolist()

        for column in self.model.introspection_target_columns:
            
            column_name = list(column.keys())[0]
            
            # Check if column is not in csv file
            columns_in_file = results_df.columns.values.tolist()
            if (columns_in_file.count(column_name) < 1):
                LOGGER.warning(f"The column specified for verification - {column_name} - does not exist in the .csv file. Check for spelling errors!\n")
            
            else: # If the column exists

                #Get values from rigelfile
                column_keys = list(column[column_name].keys())
                
                # Check for the existence of these fields and give default values if they don't exist
                if (column_keys.count('validation_min') < 1):
                    validation_min = None
                elif ([int, float].count(type(column[column_name]['validation_min'])) < 1): # Check for the types of values inputted
                    LOGGER.warning(f"'validation_min' is not an integer or float! Check your Rigelfile!")
                    validation_min = None
                else:
                    validation_min = column[column_name]['validation_min']
                
                if (column_keys.count('validation_max') < 1):
                    validation_max = None
                elif ([int, float].count(type(column[column_name]['validation_max'])) < 1):
                    LOGGER.warning(f"'validation_max' is not an integer or float! Check your Rigelfile!")
                    validation_max = None
                else:
                    validation_max = column[column_name]['validation_max']
                
                if (column_keys.count('use_latest_row') < 1):
                    use_latest_row = False
                elif (type(column[column_name]['use_latest_row']) != bool):
                    LOGGER.warning(f"'validation_max' is not a boolean! Check your Rigelfile!")
                    use_latest_row = False
                else:
                    use_latest_row = column[column_name]['use_latest_row']
                
                if (column_keys.count('value_row') < 1):
                    value_row = None
                elif (rows_in_dataframe.count(column[column_name]['value_row']) < 1): # Check if this row exists in the dataframe
                    LOGGER.warning(f"'value_row' does not exist in the dataframe!")
                    value_row = None
                elif (type(column[column_name]['value_row']) != str):
                    LOGGER.warning(f"'value_row' is not a string! Check your Rigelfile!")
                    value_row = None
                else:
                    value_row = column[column_name]['value_row']

                if (column_keys.count('acceptable_min') < 1):
                    acceptable_min = None
                elif ([int, float].count(type(column[column_name]['acceptable_min'])) < 1):
                    LOGGER.warning(f"'acceptable_min' is not an integer or float! Check your Rigelfile!")
                    acceptable_min = None
                else:
                    acceptable_min = column[column_name]['acceptable_min']

                if (column_keys.count('acceptable_max') < 1):
                    acceptable_max = None
                elif ([int, float].count(type(column[column_name]['acceptable_max'])) < 1):
                    LOGGER.warning(f"'acceptable_max' is not an integer or float! Check your Rigelfile!")
                    acceptable_max = None
                else:
                    acceptable_min = column[column_name]['acceptable_max']

                if (column_keys.count('contains_str') < 1):
                    contains_str = None
                elif (type(column[column_name]['contains_str']) != str):
                    LOGGER.warning(f"'contains_str' is not a string! Check your Rigelfile!")
                    contains_str = None
                else:
                    contains_str = column[column_name]['contains_str']

                if (column_keys.count('does_not_contain_str') < 1):
                    does_not_contain_str = None
                elif (type(column[column_name]['does_not_contain_str']) != str):
                    LOGGER.warning(f"'does_not_contain_str' is not a string! Check your Rigelfile!")
                    does_not_contain_str = None
                else:
                    does_not_contain_str = column[column_name]['does_not_contain_str']

                # Check for contradictions in the rigelfile
                # use_latest_row can't be True if value_row is specified
                if (use_latest_row == True and value_row != None):
                    LOGGER.error(f"'use_latest_row' cannot be True while 'value_row' is not None. Choose which row you want to use for comparison.")
                    exit()
                
                # can't check for both strings and numeric values
                if ((acceptable_min != None or acceptable_max != None) and (contains_str != None or does_not_contain_str != None)):
                    LOGGER.error(f"You can't choose to do introspection for both numeric and non-numeric values simultaneously. Only use the 'acceptable_min' and 'acceptable_max' fields for numeric values; only use the 'contains_str' and/or 'does_not_contain_str' fields for non-numeric values.")
                    exit()
                

                # VALIDATION PROCESS - for numerical values
                LOGGER.info(f"{40*'-'}\nValue Verification for column '{column_name}':\n{40*'-'}\n")

                is_verified = True
                
                #Get maximum and minimum values
                column_min = results_df[column_name].min()
                column_max = results_df[column_name].max()
                
                #Validate the values
                if (validation_min != None and column_min <= validation_min):
                    is_verified = False
                if (validation_max != None and column_max >= validation_max):
                    is_verified = False

                if is_verified:
                    LOGGER.info(f"{column_name} is verified!\n")
                else:
                    LOGGER.error(f"{column_name} has abnormal values! Get new values!\n")
                    exit()
                

                # INTROSPECTION PROCESS
                LOGGER.info(f"{40*'-'}\nIntrospection for column '{column_name}':\n{40*'-'}\n")


                if (use_latest_row == True):
                    value_to_compare = results_df.loc[results_df.index[-1], column_name]
                else:
                    value_to_compare = results_df.loc[value_row, column_name]

                LOGGER.info(f"Value to compare: {value_to_compare}")
                
                introspection_success = True

                if (acceptable_min != None or acceptable_max != None): # Numerical introspection
                    if (value_to_compare <= acceptable_min or value_to_compare >= acceptable_max):
                        introspection_success = False
                
                elif (contains_str != None or does_not_contain_str != None):
                    pass
                
                




        ################################
        # OLD CODE
        ################################

        # # Check for value abnormalities (optional process)
        # verification_flag = True
        
        # if self.model.columns_to_verify != None:

        #     LOGGER.info(f"{40*'-'}\nValue Verification\n{40*'-'}\n")

        #     column_names = results_df.columns.values.tolist()

        #     for column in self.model.columns_to_verify:
        #         # Check if the specified column exists in the .csv file

        #         if (column_names.count(column) < 1):
        #             LOGGER.warning(f"The column specified for verification - {column} -  does not exist in the .csv file. Check for spelling errors.\n")
        #         else:
        #             LOGGER.info(f"Verifying column '{column}'\n")
                    
        #             # Get max and min values in the column
        #             column_max = results_df[column].max()
        #             column_min = results_df[column].min()
                    
        #             # RefactoringuUpper and lower limits, if they are specified in Rigelfile
        #             upper_limit = self.model.column_verification_upper_threshold
        #             lower_limit = self.model.column_verification_lower_threshold

        #             # Issue a warning if no limit is specified
        #             if (upper_limit == None and lower_limit == None):
        #                 LOGGER.warning("No thresholds specified for value verification. Please edit your Rigelfile to include at least one threshold (upper or lower).\n")
        #             else:
        #                 pass
                    
        #             if (upper_limit != None and column_max >= upper_limit):
        #                 verification_flag = False
        #             else:
        #                 pass

        #             if (lower_limit != None and column_min <= lower_limit):
        #                 verification_flag = False
        #             else:
        #                 pass



        #     LOGGER.info(f"Are the values verified? {verification_flag}\n")

        #     if not verification_flag:
        #         LOGGER.error("Abnormal values! Get new values!")
        #         exit()
        
        
        
        
        # # Decide which value to use for comparison according to the Rigelfile
        # if (self.model.use_latest_row and self.model.use_latest_column):
            
        #     value_to_compare = results_df.loc[results_df.index[-1], results_df.columns[-1]]

        #     LOGGER.info(f"{40*'-'}\nValue used for comparison:\n{40*'-'}\n\nRow: {results_df.index[-1]} | Column: {results_df.columns[-1]} | Value: {value_to_compare}\n")

        # else:
        #     if self.model.use_latest_row:
                
        #         value_to_compare = results_df.loc[results_df.index[-1], self.model.value_column]

        #         LOGGER.info(f"{40*'-'}\nValue used for comparison:\n{40*'-'}\n\nRow: {results_df.index[-1]} | Column: {self.model.value_column} | Value: {value_to_compare}\n")

        #     elif self.model.use_latest_column:
                
        #         value_to_compare = results_df.loc[self.model.value_row, results_df.columns[-1]]
                
        #         LOGGER.info(f"{40*'-'}\nValue used for comparison:\n{40*'-'}\n\nRow: {self.model.value_row} | Column: {results_df.columns[-1]} | Value: {value_to_compare}\n")

        #     else:
                
        #         value_to_compare = results_df.loc[self.model.value_row, self.model.value_column]

        #         LOGGER.info(f"{40*'-'}\nValue used for comparison:\n{40*'-'}\n\nRow:{self.model.value_row} | Column: {self.model.value_column} | Value: {value_to_compare}\n")


        # # Quality levels, values defined in Rigelfile
        # if value_to_compare <= self.model.acceptable_threshold:
        #     results_quality = "acceptable"
        # else:
        #     results_quality = "bad"

        # if value_to_compare <= self.model.good_threshold:
        #     results_quality = "good"

        # if value_to_compare <= self.model.great_threshold:
        #     results_quality = "great"



        # LOGGER.info(f"{40*'-'}\nIntrospection Results:\n{40*'-'}\n\nThe results were {results_quality}!\n")


        # # Set "acceptable" as default requirement in case none is passed
        # if self.model.required_quality_level == None:
        #     self.model.required_quality_level = "acceptable"

        # #Comparing quality level vs. required quality level    
        # LOGGER.info(f"Required quality level: {self.model.required_quality_level}\n")

        # if self.model.required_quality_level == "acceptable":
        #     if results_quality != "bad":
        #         LOGGER.info("Test passed!\n")
        #     else:
        #         LOGGER.info("Test failed!\n")

        # elif self.model.required_quality_level == "good":
        #     if (results_quality == "good" or results_quality == "great"):
        #         LOGGER.info("Test passed!\n")
        #     else:
        #         LOGGER.info("Test failed!\n")

        # elif self.model.required_quality_level == "great":
        #     if results_quality == "great":
        #         LOGGER.info("Test passed!\n")
        #     else:
        #         LOGGER.info("Test failed!\n")

    def stop(self) -> None:
        # LOGGER.info("Running Stop") # Debug line
        pass

    