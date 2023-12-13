
import os
import warnings
warnings.filterwarnings("ignore")
import lightning.pytorch as pl  # main
from lightning.pytorch.callbacks import EarlyStopping, LearningRateMonitor
from lightning.pytorch.loggers import TensorBoardLogger
import numpy as np
import pandas as pd
import torch
from pytorch_forecasting import Baseline, NHiTS,TemporalFusionTransformer, TimeSeriesDataSet, DeepAR
from pytorch_forecasting.data import NaNLabelEncoder
from pytorch_forecasting.metrics import MAE, SMAPE, MQF2DistributionLoss, QuantileLoss, NormalDistributionLoss
from pytorch_forecasting.data import TorchNormalizer, GroupNormalizer, EncoderNormalizer
from datetime import datetime
import shutil
from utils.logger_config import get_logger
logger = get_logger(__name__)



class ModelsTrainingStep:

    def __init__(self, training_artifacts, models_config):

        """
        Args:
            training_artifacts (dict): Features DataFrame.
            models_config (list): Configuration for models.
            
        """
        self.training_artifacts = training_artifacts
        self.models_config = models_config
        

    def train_models(self, fold):

        """
        Train all models include in the model_config dict by a for bucle.
        Returns:
            training_artifacts (dict): Update the dict by adding the trained models and models checkpoints
        """
        try:
       
            for model_instance in self.models_config:

                logger = TensorBoardLogger("src/forecast_pipelines/training_checkpoints")
                early_stop_callback = EarlyStopping(monitor="val_loss",min_delta=1e-4, patience=10,verbose=False, mode="min")
                lr_logger = LearningRateMonitor()

                # Artifact: Models
                model = model_instance.model_class.from_dataset(
                    self.training_artifacts[model_instance.model_name]["training_dataset"],
                    **model_instance.model_params)
            
                # Artifact: Trainer
                trainer = pl.Trainer(
                    enable_checkpointing=True,
                    logger = logger,
                    callbacks = [lr_logger, early_stop_callback],
                    **model_instance.trainer_params)

                # Train
                trainer.fit(
                    model,
                    train_dataloaders=self.training_artifacts[model_instance.model_name]["train_dataloader"],
                    val_dataloaders=self.training_artifacts[model_instance.model_name]["val_dataloader"])

                #Artifact: Model Checkpoints
                best_model_path = trainer.checkpoint_callback.best_model_path
                best_model = model_instance.model_class.load_from_checkpoint(best_model_path)

                self.training_artifacts[model_instance.model_name]["best_model"] = best_model 
            
        except Exception as e:
            logger.error(f"An error occurred in training models method: {e}")
            raise

        return self.training_artifacts

