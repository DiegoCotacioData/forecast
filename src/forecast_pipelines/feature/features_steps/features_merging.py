
import pandas as pd


class CreateForecastFeatures:

   def __init__(self, prices_fe, vol_1_fe, vol_2_fe):

      self.prices_fe = prices_fe
      self.vol_1_fe = vol_1_fe
      self.vol_2_fe = vol_2_fe

   # Main method
   def create_features_dataframe(self):

      features = self.merge_dataframes()

      return features


    
   def merge_dataframes(self):
      
      merge_df = pd.merge(self.prices_fe, self.vol_1_fe, how='inner', left_on=['weekly_index', 'product_name'], right_on=['weekly_index', 'product_name'])
      
      merge_df = pd.merge(merge_df, self.vol_2_fe, how='inner', left_on=['monthly_index', 'product_name'], right_on=['monthly_index', 'product_name'])

      return merge_df

