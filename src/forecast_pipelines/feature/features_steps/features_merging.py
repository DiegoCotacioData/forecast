
import pandas as pd
from utils.logger_config import get_logger
logger = get_logger(__name__)

# Clase sin uso de momento

class CreateForecastFeatures:

    def __init__(self, prices_features: pd.DataFrame): 
                 #volumnes_abasto_features : pd.DataFrame,
                 #volumes_expo_features: pd.DataFrame):
        """
        Initializes the CreateForecastFeatures class.

        Args:
            prices_fe: Features derived from SIPSA prices data.
            vol_1_fe: Features derived volumnes_abasto_features
            vol_2_fe: Features derived volumes_expo_features
        """
        self.prices_fe = prices_features
        #self.vol_1_fe = volumnes_abasto_features
        #self.vol_2_fe = volumes_expo_features

    def create_features_dataframe(self):
        """
        Creates a features dataframe by merging the input dataframes.

        Returns:
            pd.DataFrame: Merged features dataframe.
        """
        try:
            features = self.merge_dataframes()
            return features
        except Exception as e:
            logger.error(f"Error during feature creation: {str(e)}")
            raise

    def merge_dataframes(self):
        """Merges the input dataframes to create the features dataframe."""
        try:
            merge_df = pd.merge(self.prices_fe, self.vol_1_fe, how='inner', left_on=['weekly_index', 'product_name'], right_on=['weekly_index', 'product_name'])
            merge_df = pd.merge(merge_df, self.vol_2_fe, how='inner', left_on=['monthly_index', 'product_name'], right_on=['monthly_index', 'product_name'])
            return merge_df
        except Exception as e:
            logger.error(f"Error during dataframe merge: {str(e)}")
            raise
