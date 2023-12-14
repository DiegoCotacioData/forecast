
from pytorch_forecasting import Baseline, NHiTS,TemporalFusionTransformer, TimeSeriesDataSet, DeepAR
from pytorch_forecasting.data import TorchNormalizer, GroupNormalizer, EncoderNormalizer
from pytorch_forecasting.metrics import MAE, SMAPE, MQF2DistributionLoss, QuantileLoss, NormalDistributionLoss
from typing import Tuple
from pytorch_forecasting import Baseline, NHiTS,TemporalFusionTransformer, TimeSeriesDataSet, DeepAR
import service_settings.forecast_settings as service


context_length = service.MAX_ENCODER_LENGTH
prediction_length = service.MAX_PREDICTION_LENGTH
accelerator = service.MODEL_ACCELERATOR


class BaseModelConfig:

    def __init__(self):

        self.dataset_params = None
        self.model_class = None
        self.model_params = None
        self.trainer_params = None
        self.model_name = None

    def models(self) -> Tuple:
        """Method to return tuple of models"""
        return NHitsModel(), DeepARModel()


class NHitsModel(BaseModelConfig):

    def __init__(self):
        super().__init__()
        self.dataset_params = {
            "max_encoder_length": context_length,
            "max_prediction_length": prediction_length,
            "allow_missing_timesteps": True,
            "time_varying_unknown_reals": ["avg_price"],
        }
        self.model_params = {
            "learning_rate": 0.001,
            "log_interval": 5,
            "dropout": 0.1,
            "log_val_interval": 1,
            "weight_decay": 1e-2,
            "backcast_loss_ratio": 0.0,
            "hidden_size": 70,
            "optimizer": "AdamW",
            "loss": QuantileLoss()
        }
        self.trainer_params = {
            "max_epochs": 1,
            "accelerator": accelerator,
            "enable_model_summary": True,
            "gradient_clip_val": 1.0,
            "limit_train_batches": 1,
        }
        self.model_class = NHiTS
        self.model_name = "N-Hits"

class DeepARModel(BaseModelConfig):

    def __init__(self):
        super().__init__()
        self.dataset_params = {
            "max_encoder_length": context_length,
            "min_encoder_length": context_length // 2,
            "max_prediction_length": prediction_length,
            "static_categoricals": ["central_mayorista", "product_name"],
            "time_varying_known_categoricals": [],
            "time_varying_known_reals": [],
            "time_varying_unknown_categoricals": [],
            "time_varying_unknown_reals": ['avg_price'],
            "add_relative_time_idx": True,
            "allow_missing_timesteps": True,
        }
        self.model_params = {
            "learning_rate": 0.001,
            "cell_type": "LSTM",
            "hidden_size": 30,
            "rnn_layers": 4,
            "dropout": 0.1,
            "loss": NormalDistributionLoss(),
            "optimizer": "Adam",
        }
        self.trainer_params = {
            "max_epochs": 1,
            "accelerator": accelerator,
            "enable_model_summary": True,
            "gradient_clip_val": 0.1,
            "limit_train_batches": 1,
        }
        self.model_class = DeepAR
        self.model_name = "DeepAR"
