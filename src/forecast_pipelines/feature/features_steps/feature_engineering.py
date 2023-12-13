import pandas as pd
import numpy as np
import holidays

from .methods.feature_engineering_methods import PricesFeatureEngineering


class FeatureEngineeringStep:

  def __init__(self, sipsa_prices_transf): #TODO: #abasto_volumes_transf , expo_volumes_transf 
      
      self.sipsa_prices_transf = sipsa_prices_transf
      #self.abasto_volumes_transf = abasto_volumes_transf
      #self.expo_volumes_transf = abasto_volumes_transf


    # Main method
  def apply_feature_engineering(self):

      prices_features = self.prices_feature_engineering()
      #abasto_volumes_features = self.abasto_volumes_feature_engineering()
      #expo_volumes_features = self.expo_volumes_feature_engineering()

      return prices_features  #abasto_volumes_features, expo_volumes_features


 
   # Local metthods

  def prices_feature_engineering(self):
      
      fe = PricesFeatureEngineering(self.sipsa_prices_transf)
      prices_features = fe.apply_feature_engineering()

      return  prices_features


  #TODO: definir metodos de feature engineering para volumenes de abasto y expo

  #def abasto_volumes_feature_engineering(self):
      #pass

  #def expo_volumes_feature_engineering(self):
      #pass
  








