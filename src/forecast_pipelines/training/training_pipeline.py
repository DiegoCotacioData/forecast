import os
import warnings
warnings.filterwarnings("ignore")
#os.chdir("../../..")
import lightning.pytorch as pl
from lightning.pytorch.callbacks import EarlyStopping, LearningRateMonitor, ModelCheckpoint
from lightning.pytorch.loggers import TensorBoardLogger
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import torch
from pytorch_forecasting import Baseline, NHiTS,TemporalFusionTransformer, TimeSeriesDataSet, DeepAR
from pytorch_forecasting.data import NaNLabelEncoder
from pytorch_forecasting.data.examples import generate_ar_data
from pytorch_forecasting.metrics import MAE, SMAPE, MQF2DistributionLoss, QuantileLoss, NormalDistributionLoss
from pytorch_forecasting.data import TorchNormalizer, GroupNormalizer, EncoderNormalizer
from datetime import datetime
#from typing import list

from forecast_pipelines.training.training_steps.training_artifacts import TrainingArtifactsStep
from forecast_pipelines.training.training_steps.models_training import ModelsTrainingStep
from forecast_pipelines.training.training_steps.models_evaluation import ModelsEvaluationStep
from utils.logger_config import get_logger
logger = get_logger(__name__)



class TrainingPipeline:

    
    def __init__(self, df_input: pd.DataFrame, models_config, max_prediction_length: int) -> None:

        """
        Initializes the TrainingPipeline class.
        Args:
            df_input (pd.DataFrame): Features DataFrame.
            models_config (List): Configuration for models.
            max_prediction_length (int): Maximum prediction length.
        """
        self.max_prediction_length = max_prediction_length
        self.df_input = df_input
        self.models_config = models_config 

    def run_pipeline(self):

        """
        Runs the training pipeline.
        Returns:
            tuple: Cross-validation metrics, training data, training cutoff, and training artifacts.
        """
        logger.info("Training Pipeline started")

        try:

            # Define the cutting dates for sliding cross validation (Set to 2 weeks)
            self.df_input['created_at'] = pd.to_datetime(self.df_input['created_at'])
            cutoff_dates = [
                #self.df_input['created_at'].max() - pd.DateOffset(days=18), #uncomment if needed
                #self.df_input['created_at'].max() - pd.DateOffset(days=12), #uncomment if needed
                #self.df_input['created_at'].max() - pd.DateOffset(days=6),
                self.df_input['created_at'].max(),
            ]

            dict_cross_val_metrics = {}

            for fold, cutoff_date in enumerate(cutoff_dates):

                data_subset = self.df_input[self.df_input['created_at'] <= cutoff_date]

                artifacts = TrainingArtifactsStep(data_subset, self.max_prediction_length, self.models_config)
                training_data, training_cutoff, training_artifacts = artifacts.create_training_artifacts()

                model_trainer = ModelsTrainingStep(training_artifacts, self.models_config)
                training_artifacts = model_trainer.train_models(fold)
        
                evaluation = ModelsEvaluationStep(training_data, training_artifacts, training_cutoff)
                training_metrics = evaluation.evaluate_models()

                dict_cross_val_metrics[f"Fold{fold + 1}"] = training_metrics.copy()

        except Exception as e:
          logger.error(f"An error occurred during training pipeline: {str(e)}")

        logger.info("Training Pipeline completed")
          

        return dict_cross_val_metrics, training_data, training_cutoff, training_artifacts


