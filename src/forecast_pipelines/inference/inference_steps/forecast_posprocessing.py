
import numpy as np
import pandas as pd
from datetime import datetime
from utils.logger_config import get_logger
logger = get_logger(__name__)


class ForecastPosprocessingStep:


    def __init__(self,new_prediction_data: pd.DataFrame,
                 dict_cross_val_metrics: dict,
                 dict_forecast_results: dict, 
                 training_cutoff: int):

        """
        Initializes the ForecastPosprocessing class.

        Args:
            new_prediction_data: ...
            cross_val_metrics: ...
            dict_forecast_results: ...
            training_cutoff: ...
        """

        self.dict_cross_val_metrics = dict_cross_val_metrics
        self.dict_forecast_results = dict_forecast_results
        self.new_prediction_data = new_prediction_data
        self.training_cutoff = training_cutoff



    def posprocess_forecast_results(self): 

        """
        Post-processes the forecast results.

        Returns:
            tuple: Rolling forecast, global metrics, cross-validation metrics, and raw forecast dataframe.
        """
        try:
            df_rolling_forecast = self.create_rolling_forecast()
            df_global_metrics, df_cv_metrics = self.create_global_metrics()
            df_raw_forecast_dataframe = self.raw_forecast_creation(df_rolling_forecast, df_global_metrics)
        
        except Exception as e:
            logger.error(f"An error occurred during the posprocess_forecast_results: {str(e)}")


        return  df_rolling_forecast, df_global_metrics, df_cv_metrics, df_raw_forecast_dataframe




    def create_rolling_forecast(self):

        """Create the rolling windows forecast dataframe applying the rolling window strategy"""

        try:
            rollw = self.new_prediction_data.copy()
            rollw['created_at'] = pd.to_datetime(rollw['created_at'])
            rollw['rolling_forecast'] = rollw.groupby('product_name')['avg_price'].shift(5) #5 days rolling 
            df_forecast_rolling = rollw[lambda x: x.time_idx > self.training_cutoff]
            df_forecast_rolling.rename(columns={'created_at': 'forecast_date'}, inplace=True)
            df_forecast_rolling = df_forecast_rolling[["forecast_date","product_name","central_mayorista","rolling_forecast"]]
            df_forecast_rolling.to_csv("rollingx.csv")

        except Exception as e:
            logger.error(f"An error occurred during the create_rolling_forecast method: {str(e)}")
            
        return df_forecast_rolling



    def create_global_metrics(self):

        """
          Estimate the global metrics (best smape for each model).
          It is used to assign the best model for each product-forecast day in the next method.

          Args:
              metrics (Dataframe): include all the historical metrics dataframe from cross val (5 weeks of error metrics)
              training_cutoff (Dataframe):
          Return:
              cv_metrics (Dataframe): include all the daily error metrics from all the models to track models performance in a dashboard
              global_metrics (Dataframe): include the AVG smape values from the las 5 weeks to be used in the forecast values selection method

        """

        try:
            # model-day-product metrics
            df_cv_metrics = pd.concat(self.dict_cross_val_metrics.values(), axis=0, ignore_index=True)
            
            df_cv_metrics.to_csv("df_cv_metrics.csv")#QUITAR

             # model-product metrics
            global_metrics = df_cv_metrics.groupby(['forecast_idx', 'product_name', 'central_mayorista'])[['TFT-L_smape','TFT-S_smape', 'DeepAR_smape','N-Hits_smape', 'rolling_smape']].mean().reset_index()
            
            #TODO: add central_mayorista in merge op
            def calculate_best_smape_model(global_metrics):
               global_metrics = global_metrics.copy()
               grouped_data = global_metrics.groupby(['product_name','central_mayorista'])

               def find_best_model(group):
                   avg_smape = group[['TFT-L_smape','TFT-S_smape', 'DeepAR_smape', 'N-Hits_smape', 'rolling_smape']].mean()
                   best_model = avg_smape.idxmin().split('_')[-2]  # Obtén el nombre del modelo
                   return pd.Series([f'{best_model}_smape'], index=['best_global_smape_model'])
               
               result_df = grouped_data.apply(find_best_model).reset_index()
               result_df.to_csv("result_df.csv") #QUITAR
               final_df = global_metrics.merge(result_df, on=['product_name','central_mayorista'])
               final_df.to_csv("final_df.csv")  #QUITAR

               return final_df
            
            global_metrics = calculate_best_smape_model(global_metrics)
            
            global_metrics.to_csv("global_metrics.csv")#QUITAR

            

        except Exception as e:
            logger.error(f"An error occurred during the create_global_metrics method: {str(e)}")

        return global_metrics, df_cv_metrics


    def raw_forecast_creation(self, rolling_forecast, global_metrics):

        """
        Este metodo crear el dataframe global con todos los resultados de los forecast y metricas (raw forecast),
        tanto para modelos como para rolling. Recibe los diccionarios y realiza operaciones merge
        para unificar todos los resultados.
        Args:
          rolling_forecast (Dataframe):
          models_forecasting (Dict of Dataframes): #Atributo de clase
          global_metrics (Dict):
        """
        
        rollingx = rolling_forecast.copy()

        try:
            for model_name, model_output in self.dict_forecast_results.items():
                model_output = model_output.rename(columns={
                "q50": f"{model_name}_q50",
                "q25": f"{model_name}_q25",
                "q10": f"{model_name}_q10",
                "q75": f"{model_name}_q75",
                "q90": f"{model_name}_q90",
                })

                rollingx.rename(columns={'created_at': 'forecast_date'}, inplace=True)
                rollingx = model_output.merge(rollingx, on=['forecast_date', 'product_name','central_mayorista'], how='left')

                unique_dates = sorted(rollingx['forecast_date'].dt.date.unique())
                date_idx_dict = {forecast_date: idx + 1 for idx, forecast_date in enumerate(unique_dates)}
                rollingx['forecast_idx'] = rollingx['forecast_date'].dt.date.map(date_idx_dict)

                raw_forecast_dataframe = pd.merge(rollingx, global_metrics, on=["forecast_idx","product_name", "central_mayorista"], how="left")
                
                today = datetime.now().date()
                raw_forecast_dataframe["update_date"] = today
        except Exception as e:
            logger.error(f"An error occurred during the raw_forecast_creation method: {str(e)}")

        return raw_forecast_dataframe



