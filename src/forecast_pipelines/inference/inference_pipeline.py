
import pandas as pd
from forecast_pipelines.inference.inference_steps.decoder_generation import InferenceDatasetCreationStep
from forecast_pipelines.inference.inference_steps.forecast_generation import ForecastingCreationStep
from forecast_pipelines.inference.inference_steps.forecast_posprocessing import ForecastPosprocessingStep
from forecast_pipelines.inference.inference_steps.forecast_selection import ForecastSelectionStep
from forecast_pipelines.inference.inference_steps.forecast_load import LoadForecastOutputsStep
from service_settings import forecast_settings
from utils.logger_config import get_logger
logger = get_logger(__name__)




class InferencePipeline:

    def __init__(self, 
                 training_artifacts: dict,
                 dict_cross_val_metrics: dict,
                 features_df: pd.DataFrame,
                 training_data: pd.DataFrame,
                 training_cutoff: int
                 ) -> None:
        
        """
        Initializes the InferencePipeline class.

        Args:
            forecast_dataloaders 
            training_artifacts (Dict): Contains the trained models 
            dict_cross_val_metrics (Dict): Contains de training metrics for forecast selection strategy
            features_df (pd.DataFrame): Training features to be save
            training_data (Dict): Training dataframe needed to create inference dataframe
            training_cutoff (Int): Int to create inference dataframe
            max_encoder_length (Int): forecast context window
            max_prediction_length (Int):  forecast horizon
            batch_size (Int):  training parameter
        """
        self.max_encoder_length = forecast_settings.MAX_ENCODER_LENGTH
        self.max_prediction_length = forecast_settings.MAX_PREDICTION_LENGTH
        self.batch_size = forecast_settings.BATCH_SIZE

        self.training_artifacts = training_artifacts
        self.training_data = training_data
        self.training_cutoff = training_cutoff
        self.dict_cross_val_metrics = dict_cross_val_metrics
        self.features_df  = features_df

     
    def run_pipeline(self):
        """
        Runs the inference pipeline.
        """

        logger.info("Inicio pipeline de inferencia") #tempora
        try:
            logger.info("Inicio creacion de dataset de inferencia")#tempora
            inference_dataset = InferenceDatasetCreationStep(self.max_encoder_length,
                                                        self.max_prediction_length,
                                                        self.batch_size,
                                                        self.training_artifacts,
                                                        self.training_data)
            
            forecast_dataloaders, decoder_data, new_prediction_data = inference_dataset.create_prediction_data()
            logger.info("Decoder generation step completed")
            
            logger.error("Inicio creacion de forecast")#tempora
            forecast_generation =  ForecastingCreationStep(forecast_dataloaders,
                                                       decoder_data,
                                                       self.training_artifacts )
            
            dict_forecast_results = forecast_generation.create_forecast()
            logger.info("Forecast generation step completed")

            logger.info("Inicio posproceso forecast")#tempora
            posprocess_step = ForecastPosprocessingStep(new_prediction_data,
                                                    self.dict_cross_val_metrics,
                                                    dict_forecast_results,
                                                    self.training_cutoff)
            
            df_rolling_forecast, df_global_metrics, df_cv_metrics, df_raw_forecast_dataframe= posprocess_step.posprocess_forecast_results()
            logger.info(" Forecast posprocessing step completed")

            logger.info("Inicio seleccion forecast")#tempora
            select_step = ForecastSelectionStep(df_raw_forecast_dataframe)

            df_selected_multiple_forecast, df_selected_single_forecast, forecast_data_point = select_step.select_final_output()
            logger.info("Forecast selection step completed")

            
            load = LoadForecastOutputsStep(df_cv_metrics,
                                       df_raw_forecast_dataframe,
                                       df_selected_multiple_forecast,  
                                       df_selected_single_forecast,
                                       forecast_data_point,
                                       self.features_df)                    
            load.load_outputs()
            logger.info("Forecast export step completed")


        except Exception as e:
            logger.error(f"An error occurred during inference pipeline: {str(e)}")


