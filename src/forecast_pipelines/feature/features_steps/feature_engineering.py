import pandas as pd
import numpy as np
import holidays
from utils.logger_config import get_logger
logger = get_logger(__name__)

from .methods.feature_engineering_methods import PricesFeatureEngineering


class FeatureEngineeringStep:

  def __init__(self, sipsa_prices_transf): 

      """
      Initializes the FeatureEngineeringStep class.

      Args:
        sipsa_prices_transf: Transformed SIPSA prices data.
        # abasto_volumes_transf: Transformed abasto volumes data. (TODO: Uncomment and add if needed)
        # expo_volumes_transf: Transformed expo volumes data. (TODO: Uncomment and add if needed)
      """ 
      self.sipsa_prices_transf = sipsa_prices_transf
      #self.abasto_volumes_transf = abasto_volumes_transf
      #self.expo_volumes_transf = abasto_volumes_transf


  def apply_feature_engineering(self):
      
      """
      Applies feature engineering.

      Returns:
         pd.DataFrame: Features derived from SIPSA prices data.
         # pd.DataFrame: Features derived from abasto volumes data. (TODO: Uncomment and add if needed)
         # pd.DataFrame: Features derived from expo volumes data. (TODO: Uncomment and add if needed)
      """
      try:
        prices_features = self.prices_feature_engineering()
        #abasto_volumes_features = self.abasto_volumes_feature_engineering()
        #expo_volumes_features = self.expo_volumes_feature_engineering()
      except Exception as e:
            logger.error(f"An error occurred in feature engineering method: {e}")
            raise

      return prices_features  #abasto_volumes_features, expo_volumes_features
 

  def prices_feature_engineering(self):
      """ Apply feature engineering to prices dataframe """
      
      fe = PricesFeatureEngineering(self.sipsa_prices_transf)
      prices_features = fe.apply_feature_engineering()

      return  prices_features


  #TODO: definir metodos de feature engineering para volumenes de abasto y expo

  #def abasto_volumes_feature_engineering(self):
      #pass

  #def expo_volumes_feature_engineering(self):
      #pass
  








