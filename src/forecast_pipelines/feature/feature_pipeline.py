
from .features_steps.data_extraction import ExtractInputDataStep
from .features_steps.data_transformation import TransformInputDataStep
from .features_steps.feature_engineering import FeatureEngineeringStep
from .features_steps.features_merging import CreateForecastFeatures
from utils.logger_config import get_logger
logger = get_logger(__name__)



class FeaturePipeline:

   
    def run_pipeline(self):

        """
        Runs the feature pipeline.

        Returns:
            pd.DataFrame: Processed features for training pipeline.
        """
        try:

            extract = ExtractInputDataStep()
            raw_sipsa_prices = extract.extract_raw_data()  #TODO: return raw_volumes_abasto, raw_volumes_expo
            logger.debug("Data extraction step completed")


            transform = TransformInputDataStep(raw_sipsa_prices)
            sipsa_prices_transf = transform.transform_raw_data() #TODO: return volumnes_abasto_transf, volumes_expo_transf
            logger.debug("Data transformation step completed")


            feature_eng = FeatureEngineeringStep(sipsa_prices_transf)
            prices_features = feature_eng.apply_feature_engineering() #TODO: return volumnes_abasto_features, vol_expo_features
            logger.debug(" Feature engineering step completed")


            #TODO: merge de todos los dataframes 
            #features_df = CreateForecastFeatures()
            #features = features_df.create_features_dataframe()
            features = prices_features
            logger.debug("All the feature pipeline steps finished successfully")

        except Exception as e:
            logger.error(f"An error occurred during feature pipeline: {str(e)}")

        logger.info("Feature pipeline completed")

        return  features
