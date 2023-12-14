
from utils.gsheets_connectors import GoogleSheets
import pandas as pd
import numpy as np
import os
import io
import  service_settings.infra_settings as infra
import service_settings.forecast_settings as service
from utils.logger_config import get_logger
logger = get_logger(__name__)


class LoadForecastOutputsStep:

    def __init__(self,
                 df_cv_metrics: pd.DataFrame,
                 df_raw_forecast: pd.DataFrame, 
                 df_selected_multiple_forecast: pd.DataFrame, 
                 df_selected_single_forecast: pd.DataFrame, 
                 forecast_data_point: pd.DataFrame, 
                 df_input: pd.DataFrame):
        
        """
        Initializes the LoadForecastOutputsStep class. 

        Args:
            df_cv_metrics: DataFrame containing cross-validation metrics.
            df_raw_forecast: DataFrame containing raw forecast data (5 quantiles) from all models trained.
            df_selected_multiple_forecast: DataFrame containing selected multiple forecasts.
            df_selected_single_forecast: DataFrame containing selected single forecast.
            forecast_data_point: DataFrame containing forecast data points.
            df_input: DataFrame containing the training features.
        """
       
        self.cv_metrics = df_cv_metrics
        self.df_selected_multiple_forecast = df_selected_multiple_forecast
        self.df_selected_single_forecast = df_selected_single_forecast
        self.raw_forecast = df_raw_forecast
        self.forecast_data_point = forecast_data_point
        self.df_input = df_input
        self.load_results = GoogleSheets() 

        self._map_product_names()


    def _map_product_names(self):
        """
        Maps product names in the dataframes using a predefined mapping.
        """
        try:
            mapping_product_name = service.POSPROCESSING_PRODUCT_MAPPING

            # Mapping with the original product name for other outputs from the service
            self.cv_metrics['product_name'] = self.cv_metrics['product_name'].map(mapping_product_name)
            self.raw_forecast['product_name'] = self.raw_forecast['product_name'].map(mapping_product_name)
            self.forecast_data_point['product_name'] = self.forecast_data_point['product_name'].map(mapping_product_name)
            self.df_input['product_name'] = self.df_input['product_name'].map(mapping_product_name)
        except Exception as e:
            logger.error(f"Error during product name mapping: {str(e)}")
            raise


    def load_outputs(self):

        """
        Load all the outputs from the forecast services in differents tables according with the type of use:
        There are 2 main of use:

        * Historic outputs: Append results for each service run to get an historical trace for posterior performance analysis
        * Current outputs: Daily update of the tables connected to a user dashboards. It contains the current values suggested.
        """

        outputs = {
            "historic_outputs": {
                "metrics": {
                    "dataframe": self.cv_metrics,
                    "url": infra.H_TRAINING_METRICS_URL_TABLE,
                    "worksheet": infra.H_TRAINING_METRICS_WORKSHEET_TABLE
                },
                "historic_raw": {
                    "dataframe": self.raw_forecast,
                    "url": infra.H_RAW_FORECAST_URL_TABLE,
                    "worksheet": infra.H_RAW_FORECAST_WORKSHEET_TABLE
                },
                "historic_mult_selected": {
                    "dataframe": self.df_selected_multiple_forecast,
                    "url": infra.H_MULTIPLE_MODELS_FORECAST_URL_TABLE,
                    "worksheet": infra.H_MULTIPLE_MODELS_FORECAST_WORKSHEET_TABLE
                },
                "historic_s_selected": {
                    "dataframe": self.df_selected_single_forecast,
                    "url": infra.H_SINGE_MODEL_FORECAST_URL_TABLE,
                    "worksheet": infra.H_SINGLE_MODEL_FORECAST_WORKSHEET_TABLE 
                }
              },
            "current_outputs": {
                "current_m_selected": {
                    "dataframe": self.df_selected_multiple_forecast,
                    "url": infra.C_MULTIPLE_MODELS_FORECAST_URL_TABLE,
                    "worksheet_name": infra.C_MULTIPLE_MODELS_FORECAST_WORKSHEET_TABLE,
                    "w": 1,
                    "col": 1,
                    "clear_sheet": True
                }, "current_s_selected": {
                    "dataframe": self.df_selected_single_forecast,
                    "url": infra.C_SINGE_MODEL_FORECAST_URL_TABLE,
                    "worksheet_name": infra.C_SINGLE_MODEL_FORECAST_WORKSHEET_TABLE, 
                    "w": 1,
                    "col": 1,
                    "clear_sheet": True
                },"current_raw": {
                    "dataframe": self.raw_forecast,
                    "url": infra.C_RAW_FORECAST_URL_TABLE,
                    "worksheet_name": infra.C_RAW_FORECAST_WORKSHEET_TABLE,
                    "w": 1,
                    "col": 1,
                    "clear_sheet": True
                },"df_input": {
                    "dataframe": self.df_input,
                    "url": infra.C_FEATURES_URL_TABLE,
                    "worksheet_name": infra.C_FEATURES_WORKSHEET_TABLE,
                    "w": 1,
                    "col": 1,
                    "clear_sheet": True
                },"current_metrics": {
                    "dataframe": self.cv_metrics,
                    "url": infra.C_TRAINING_METRICS_URL_TABLE,
                    "worksheet_name":infra.C_TRAINING_METRICS_WORKSHEET_TABLE,
                    "w": 1,
                    "col": 1,
                    "clear_sheet": True
                }
              }
        }

        for item, item_outputs in outputs.items():
            if item == "historic_outputs":
                for key, output in item_outputs.items():
                    dataframe = output["dataframe"]
                    url = output["url"]
                    worksheetname = output["worksheet"]
                    self.load_results.append_sheets(dataframe, url, worksheetname)

            elif item == "current_outputs":
                for key, output in item_outputs.items():
                    dataframe = output["dataframe"]
                    url = output["url"]
                    worksheetname = output["worksheet_name"]
                    w = output["w"]
                    col = output["col"]
                    clear_sheet = output["clear_sheet"]
                    self.load_results.export_sheets(dataframe, url, worksheetname, w, col, clear_sheet)

