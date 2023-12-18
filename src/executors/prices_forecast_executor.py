import os, sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

import pandas as pd
import numpy as np
from forecast_pipelines.feature.feature_pipeline import FeaturePipeline
from forecast_pipelines.training.training_pipeline import TrainingPipeline
from forecast_pipelines.inference.inference_pipeline import InferencePipeline
from service_settings.models_settings import BaseModelConfig
from utils.logger_config import get_logger
import service_settings.forecast_settings as service
logger = get_logger(__name__)



class RunForecastService:

    def __init__(self):

        """ Instanciate the models configuration """

        self.models_config = BaseModelConfig().models()
        self.max_prediction_length = service.MAX_PREDICTION_LENGTH
        logger.info("Models instanciated")


    def run_service(self):

        """
        Run the price forecast service.

        - Feature Pipeline:
        Extracts, preprocesses, formats and performs feature engineering, returning the features dataframe for training.

        - Training Pipeline: 
        Defines the architecture of the models and trains them using the time series cross validation (sliding) technique.
        Training metrics (SMAPE) are used in the inference pipeline for model selection.

        - Inference Pipeline: 
        Prepares the inference dataframe and performs inferences with the trained models.
        With the results, the pipeline perform a model selection strategy under 2 approaches: Day-Model-Product and Model-Product.
        Finally it uploads the results to the databases in GSheets.

        """
        try:
            feature_pipeline = FeaturePipeline()
            features_df = feature_pipeline.run_pipeline()
            logger.info("Feature pipeline completed")

            training_pipeline = TrainingPipeline(features_df, self.models_config, self.max_prediction_length)  
            cross_val_metrics, data, training_cutoff, training_artifacts = training_pipeline.run_pipeline()
            logger.info("Training pipeline completed")

            inference_pipeline = InferencePipeline(training_artifacts, cross_val_metrics,features_df,data,training_cutoff)
            inference_pipeline.run_pipeline()
            logger.info("Inference pipeline completed")

        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            


if __name__=="__main__":
    prices_forecast_service = RunForecastService()
    prices_forecast_service.run_service()