# class ForecastCriteria   se llama en la clase #ForecastSelection

from datetime import datetime
import pandas as pd 
import numpy as np
import service_settings.forecast_settings as service
from utils.logger_config import get_logger
logger = get_logger(__name__)



class ForecastSelectionStep:


    def __init__(self, df_raw_forecast_dataframe: pd.DataFrame):

        """
        Initializes the ForecastSelection class.

        Args:
            df_raw_forecast_dataframe (pd.DatFrame): dataframe with all the forecast results and metrics from all trained models
        """
        self.raw_forecast_dataframe = df_raw_forecast_dataframe


    def select_final_output(self):

        """
        This is a key method throughout the service.
        Here, 2 approaches are used to select forecast results:

        Approach 1 DAY-PRODUCT-MODEL: For each product and forecast day, the value of the model with the best SMAPE for that specific day is selected.
        Different models for different products and days. The result is the data frame: df_multiple_model_forecast.

        Approach 2 PRODUCT-MODEL: For each product, the model values ​​with the best average SMAPE in training are selected.
        A single model for each product. The output is the data frame: df_single_model_forecast.
        """
        try:

            raw_input = self.raw_forecast_dataframe.copy()
            raw_selection_input = raw_input[
                #["forecast_date", "product_name", "rolling_forecast", "rolling_smape", "N-Hits_q50",
                #"N-Hits_q10", "N-Hits_smape", "DeepAR_q50", "DeepAR_q10", "DeepAR_smape", "best_global_smape_model"]]
            
                ["forecast_date", "product_name", "rolling_forecast", "rolling_smape", "N-Hits_q50",
                 "N-Hits_q10", "N-Hits_smape", "TFT-S_q50", "TFT-S_q10", "TFT-S_smape","TFT-L_q50",
                "TFT-L_q10", "TFT-L_smape", "DeepAR_q50", "DeepAR_q10", "DeepAR_smape", "best_global_smape_model"]]

            today = datetime.now().date()
            raw_selection_input["update_date"] = today

            models_mapping = {'rolling_smape': 'RollingW',
                               'N-Hits_smape': 'N-Hits',
                               'DeepAR_smape': 'DeepAR',
                               'TFT-S_smape': 'TFT-S',
                               'TFT-L_smape': 'TFT-L',
                             }

            mapping_product_name =  service.POSPROCESSING_PRODUCT_MAPPING

            def apply_mapping(df, model_mapping, product_mapping, calculate_function):
                result = df.apply(calculate_function, axis=1)
                result['model'] = result['model'].map(model_mapping)
                result['product_name'] = result['product_name'].map(product_mapping)
                return result
            
            df_selected_multiple_forecast = apply_mapping(raw_selection_input, models_mapping, mapping_product_name, self.calculate_multiple_values)
            df_selected_single_forecast = apply_mapping(raw_selection_input, models_mapping, mapping_product_name, self.calculate_single_values)

            # This is a filtered dataframe with only the last date of the forecast (day 5)
            forecast_data_point = df_selected_multiple_forecast[df_selected_multiple_forecast["forecast_date"] == df_selected_multiple_forecast["forecast_date"].max()]

        except Exception as e:
            logger.error(f"An error occurred during the select_final_output method: {str(e)}")

        return  df_selected_multiple_forecast, df_selected_single_forecast, forecast_data_point


    def calculate_multiple_values(self, row):
        """DAY-PRODUCT-MODEL """
        best_smape = min(row['TFT-L_smape'],row['TFT-S_smape'], row['N-Hits_smape'],row['DeepAR_smape'], row['rolling_smape'])
        #best_smape = min(row['N-Hits_smape'],row['DeepAR_smape'], row['rolling_smape'])

        if best_smape == row['TFT-L_smape']:
            q50 = row['TFT-L_q50']
            q10 = row['TFT-L_q10']
            smape = row['TFT-L_smape']
            model = 'TFT-L_smape'
        elif best_smape == row['TFT-S_smape']:
            q50 = row['TFT-S_q50']
            q10 = row['TFT-S_q10']
            smape = row['TFT-S_smape']
            model = 'TFT-S_smape'
        elif best_smape == row['N-Hits_smape']:
            q50 = row['N-Hits_q50']
            q10 = row['N-Hits_q10']
            smape = row['N-Hits_smape']
            model = 'N-Hits_smape'
        elif best_smape == row['DeepAR_smape']:
            q50 = row['DeepAR_q50']
            q10 = row['DeepAR_q10']
            smape = row['DeepAR_smape']
            model = 'DeepAR_smape'
        else:
            q50 = row['rolling_forecast']
            q10 = row['rolling_forecast'] * (1 - row['rolling_smape'])
            smape = row['rolling_smape']
            model = 'rolling_smape'
        

        today = datetime.now().date()

        return pd.Series({
            'update_date': row['update_date'],
            'forecast_date': row['forecast_date'],
            'product_name': row['product_name'],
            'q50': q50,
            'q10': q10,
            'smape': smape,
            'model': model,
            'update_date': today
        })


    def calculate_single_values(self, row):
        """PRODUCT-MODEL"""
        best_model_m = row['best_global_smape_model']

        if best_model_m == row['TFT-L_smape']:
            q50 = row['TFT-L_q50']
            q10 = row['TFT-L_q10']
            smape = row['TFT-L_smape']
            model = 'TFT-L_smape'
        elif best_model_m == row['TFT-S_smape']:
            q50 = row['TFT-S_q50']
            q10 = row['TFT-S_q10']
            smape = row['TFT-S_smape']
            model = 'TFT-S_smape'
        elif best_model_m == row['N-Hits_smape']:
            q50 = row['N-Hits_q50']
            q10 = row['N-Hits_q10']
            smape = row['N-Hits_smape']
            model = 'N-Hits_smape'
        elif best_model_m == row['DeepAR_smape']:
            q50 = row['DeepAR_q50']
            q10 = row['DeepAR_q10']
            smape = row['DeepAR_smape']
            model = 'DeepAR_smape'
        else:
            q50 = row['rolling_forecast']
            q10 = row['rolling_forecast'] * (1 - row['rolling_smape'])
            smape = row['rolling_smape']
            model = 'rolling_smape'

        today = datetime.now().date()

        return pd.Series({
            'update_date': row['update_date'],
            'forecast_date': row['forecast_date'],
            'product_name': row['product_name'],
            'q50': q50,
            'q10': q10,
            'smape': smape,
            'model': model,
            'update_date': today
        })
