from rigel.loggers import get_logger
from rigel.plugins import Plugin as PluginBase
from rigel.models.application import Application
from rigel.models.builder import ModelBuilder
from rigel.models.plugin import PluginRawData
from rigel.models.rigelfile import RigelfileGlobalData
from typing import Any, Dict
from .models import PluginModel
import pandas
import os.path
import time


LOGGER = get_logger()

class FileIntrospectionPlugin(PluginBase):

    def __init__(
        self,
        raw_data: PluginRawData,
        global_data: RigelfileGlobalData,
        application: Application,
        providers_data: Dict[str, Any]
        shared_data: Dict[str, Any] = {}
    ) -> None:
        super().__init__(
            raw_data,
            global_data,
            application,
            providers_data,
            shared_data
        )

        self.model = ModelBuilder(PluginModel).build([], self.raw_data)

    def setup(self) -> None:
        LOGGER.info("DEBUG SETUP")
        pass

    def start(self) -> None:

        # Read csv results file
        while (not os.path.exists(self.model.file)):
            time.sleep(1)
        

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

                LOGGER.info(f"{column[column_name].validation_min}")

                #Get values from rigelfile
                # column_keys = list(column[column_name].keys())
                
                # Check for the existence of these fields and give default values if they don't exist
                
                validation_min = column[column_name].validation_min
                validation_max = column[column_name].validation_max
                use_latest_row = column[column_name].use_latest_row
                value_row = column[column_name].value_row
                acceptable_min = column[column_name].acceptable_min
                acceptable_max = column[column_name].acceptable_max
                contains_str = column[column_name].contains_str
                does_not_contain_str = column[column_name].does_not_contain_str

                # Check for contradictions in the Rigelfile
                # use_latest_row can't be True if value_row is specified
                if (use_latest_row == True and value_row != None):
                    LOGGER.error(f"'use_latest_row' cannot be True while 'value_row' is not None. Choose which row you want to use for comparison.")
                    exit()
                
                # can't check for both strings and numeric values
                if ((acceptable_min != None or acceptable_max != None) and (contains_str != None or does_not_contain_str != None)):
                    LOGGER.error(f"You can't choose to do introspection for both numeric and non-numeric values simultaneously. Only use the 'acceptable_min' and 'acceptable_max' fields for numeric values; only use the 'contains_str' and/or 'does_not_contain_str' fields for non-numeric values.")
                    exit()
                
                if (acceptable_max == None and acceptable_min == None and contains_str == None and does_not_contain_str == None):
                    LOGGER.error(f"No introspection parameters defined! For numeric introspection, insert values for 'acceptable_max' and/or 'acceptable_min'. For non-numerical introspection, insert values for 'contains_str' and/or 'does_not_contain_str'.")
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

                LOGGER.info(f"Value to compare: {value_to_compare}\n")
                
                introspection_success = True
                
                
                # Numerical introspection
                if (acceptable_min != None and value_to_compare <= acceptable_min):
                    introspection_success = False
                if (acceptable_max != None and value_to_compare >= acceptable_max):
                    introspection_success = False

                # Non-numerical introspection
                if ((contains_str != None or does_not_contain_str != None) and type(value_to_compare) != str):
                    LOGGER.error(f"Your chosen value to compare is not a string!\n")
                    exit()

                if (contains_str != None and not contains_str in value_to_compare):
                    introspection_success = False
                if (does_not_contain_str != None and does_not_contain_str in value_to_compare):
                    introspection_success = False
                

                if introspection_success:
                    LOGGER.info(f"Introspection for column '{column_name}' passed!\n")
                else:
                    LOGGER.info(f"Introspection for column '{column_name}' failed!\n")

                

    def process(self) -> None:
        LOGGER.info("DEBUG PROCESS")
        pass


    def stop(self) -> None:
        LOGGER.info("DEBUG STOP") # Debug line
        pass

    