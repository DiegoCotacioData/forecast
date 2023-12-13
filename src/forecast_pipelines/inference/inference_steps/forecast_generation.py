import pandas as pd
import numpy as np
from typing import Dict
from utils.logger_config import get_logger
logger = get_logger(__name__)




class ForecastingCreationStep:

    def __init__(self, forecast_dataloaders: dict, decoder_data: pd.DataFrame, training_artifacts: dict) -> None:

        """
        Initializes the ForecastingCreation class.

        Args:
            forecast_dataloaders (Dict): Dictionary of forecast dataloaders.
            decoder_data (pd.DataFrame): Decoder data.
            training_artifacts (Dict): Dictionary of training artifacts.
        """
        
        self.forecast_dataloaders = forecast_dataloaders
        self.decoder_data = decoder_data
        self.training_artifacts = training_artifacts
  


    def create_forecast(self):

      """
        Create the forecast for each model

        Args:
            training artifacts (Dict): contains the trained models
            decoder_data (Dataframe): contains the days to predict and the future features (future known values)
            forecast_dataloader (Pytorch object): the dataloader config for inference

        Return:
            forecast_output (Dict): contains all the forecast dataframes from all the models
        """

      dict_forecast_results = {}

      try:

        for model_name, artifacts in self.training_artifacts.items():

            model = artifacts["best_model"]

            # predict
            forecast_results = model.predict(
                self.forecast_dataloaders[model_name],
                mode="raw",
                return_index=True,
                return_x=True,
            )

            # dataframe for 5 Quantiles outputs).
            preds_df = self.decoder_data[["created_at", "product_name", "central_mayorista"]].copy()
            preds_df[["q10", "q25", "q50", "q75", "q90"]] = np.nan

            # Extract the 5 Quantiles from the Tensor object returned
            q_preds = forecast_results.output.prediction.cpu().numpy()[:, :, [2, 3, 4, 5, 6]]

            # Assign the extracted values in the dataframe considering the matches between tensor index and new pred_df dataframe
            for i, row in forecast_results.index.iterrows():
                condition = (
                    (preds_df["central_mayorista"] == row["central_mayorista"]) &
                    (preds_df["product_name"] == row["product_name"])
                )
                preds_df.loc[condition, ["q10", "q25", "q50", "q75", "q90"]] = q_preds[i]

        
            float_columns = preds_df.select_dtypes(include=['float64']).columns
            preds_df[float_columns] = preds_df[float_columns].round(0)
            preds_df.rename(columns={'created_at': 'forecast_date'}, inplace=True)

            dict_forecast_results[model_name] = preds_df
      
      except Exception as e:
            logger.error(f"An error occurred during the create_forecast method: {str(e)}")

      return dict_forecast_results 


