
from datetime import datetime
import numpy as np
import pandas as pd
from pytorch_forecasting import Baseline, NHiTS,TemporalFusionTransformer, TimeSeriesDataSet, DeepAR
from pytorch_forecasting.data import TorchNormalizer, GroupNormalizer, EncoderNormalizer
import logging
from pytorch_forecasting import Baseline, NHiTS,TemporalFusionTransformer, TimeSeriesDataSet, DeepAR
import service_settings.forecast_settings as service
from utils.logger_config import get_logger
logger = get_logger(__name__)



class TrainingArtifactsStep:

    def __init__(self, data_subset, max_prediction_length, models_config):
        
        self.data_subset = data_subset
        self.max_prediction_length = max_prediction_length
        self.models_config = models_config

    def create_training_artifacts(self):

        data, training_cutoff = self.create_training_data()
        training_artifacts = self.create_dataloaders(data, training_cutoff)
        return data, training_cutoff, training_artifacts


    def create_training_data(self):

        """
        Create the training dataset adding the time indices (time_idx) and specify the cutoff data point
        to divide training from validation data
        Args:
            max_prediction_len (int): The forecasting horizon in days (5).
        Returns:
            time_index (dataframe column): Sequential numeric index from 0 to n.
            training_cutoff (int): Index for the cutoff between train and val.
        """

        try:
            self.data_subset['created_at'] = pd.to_datetime(self.data_subset['created_at'])
            last_date = self.data_subset['created_at'].max()

            data_cutoff = last_date - pd.DateOffset(days= service.WINDOW_DAYS)

            data = self.data_subset[self.data_subset['created_at'] >= data_cutoff]
            data = data[['created_at', 'central_mayorista', 'product_name'] + [col for col in data.columns if col not in ['created_at', 'central_mayorista', 'product_name']]]

            data['time_idx'] = data.groupby('created_at').ngroup()

            training_cutoff = data['time_idx'].max() -  self.max_prediction_length

            data['avg_price'] = data['avg_price'].astype('float32')

            return data, training_cutoff

        except Exception as e:
            logger.error(f"An error occurred in create_training_data: {e}")
            raise




    def create_dataloaders(self, data, training_cutoff):

        """
        Create dict object that contains the Train and Validation datasets and dataloader for models training.

        Args:
            models_config (dict): The dict with dataset params, model class, models params
            training_cutoff (int): The cut off point between train and validation data

        Returns:
            training_artifacts (dict): The dict that store the 4 training objects required:
               1. Training Pytorch TimeSeriesDataset
               2. Validation Pytorch TimeSeriesDataset
               3. Training Pytorch Dataloader
               4. Validation PytorchDataloader
        """

        try: 

            training_artifacts = {}

            for model_metadata in self.models_config:

                if model_metadata.model_class == DeepAR:
                    data["avg_price"] = data["avg_price"].astype(float)

                training_dataset = TimeSeriesDataSet(
                    data[lambda x: x.time_idx <= training_cutoff],
                    time_idx="time_idx",
                    target="avg_price",
                    group_ids=["central_mayorista", "product_name"],
                    target_normalizer=GroupNormalizer(
                        groups=["central_mayorista", "product_name"], transformation="softplus"
                    ),
                    **model_metadata.dataset_params
                )

                validation_dataset = TimeSeriesDataSet.from_dataset(
                    training_dataset,
                    data,
                    min_prediction_idx= training_cutoff + 1
                )

                train_dataloader = training_dataset.to_dataloader(train=True, batch_size= service.BATCH_SIZE, num_workers=0)
                val_dataloader = validation_dataset.to_dataloader(train=False, batch_size= service.BATCH_SIZE, num_workers=2)

                training_artifacts[model_metadata.model_name] = {
                    "training_dataset": training_dataset,
                    "validation_dataset": validation_dataset,
                    "train_dataloader": train_dataloader,
                    "val_dataloader": val_dataloader
                }

        except Exception as e:
            logger.error(f"An error occurred in create_dataloaders: {e}")
            raise


        return training_artifacts 





