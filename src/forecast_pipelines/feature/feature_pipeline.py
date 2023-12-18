import os, sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.'))
from utils.gsheets_connectors import GoogleSheets
import pandas as pd
import service_settings.infra_settings as infra
from utils.logger_config import get_logger
logger = get_logger(__name__)


class FeaturePipeline:

    def __init__(self):
        self.extractor = GoogleSheets()


    def run_pipeline(self):
        """
        Runs the feature pipeline.
        It includes the next steps: Data extraction and merge of the training features

        Returns:
            pd.DataFrame: Processed features for training pipeline.
        """
        try:
            features_df = self.extract_raw_data()
            features = self.format_dtypes(features_df)
            #features_df = self.create_features_dataframe(prices_features, abasto_vol_features, expo_vols_features) # temporal
            logger.info("All the feature pipeline steps finished successfully")

            return features

        except Exception as e:
            logger.error(f"An error occurred during feature pipeline: {str(e)}")


    def extract_raw_data(self):
        """
        Extracts raw data from Google Sheets.
        """
        try:
            prices_features = self.extractor.import_single_sheet(infra.PRICES_URL_TABLE, infra.PRICES_WORKSHEETNAME)
            #abasto_vol_features = self.extract.import_single_sheet(infra_settings.ABASTO_VOLS_URL_TABLE, infra_settings.ABASTO_VOLS_WORKSHEETNAME)
            #expo_vols_features = self.extract.import_single_sheet(infra_settings.EXPO_VOLS_URL_TABLE, infra_settings.EXPO_VOLS_WORKSHEETNAME)
            return prices_features
        except Exception as e:
            logger.error(f"Error during data extraction: {str(e)}")
            raise


    def format_dtypes(self, df):
       
       """ format dtypes of extracted prices features dataframe """
       df['created_at'] = pd.to_datetime(df['created_at'])
       df['product_name'] = df['product_name'].astype(str)
       df['central_mayorista'] = df['central_mayorista'].astype(str)
       df['avg_price'] = df['avg_price'].astype(float)
       df['max_price'] = df['max_price'].astype(float)
       df['min_price'] = df['min_price'].astype(float)
       df['day_of_week'] = df['day_of_week'].astype(int)
       df['day_of_month'] = df['day_of_month'].astype(int)
       df['var_last_day'] = df['var_last_day'].astype(float)
       df['var_same_day_last_week'] = df['var_same_day_last_week'].astype(float)
       df['year'] = df['year'].astype(int)
       df['week_of_year'] = df['week_of_year'].astype(int)
       df['weekly_avg_price'] = df['weekly_avg_price'].astype(float)
       df['weekly_var'] = df['weekly_var'].astype(float)
       df['weekly_var_type'] = df['weekly_var_type'].astype(int)
       df['week_price_last_1year'] = df['week_price_last_1year'].astype(float)
       df['next_week_price_last_1year'] = df['next_week_price_last_1year'].astype(float)
       df['week_price_last_2year'] = df['week_price_last_2year'].astype(float)
       df['next_week_price_last_2year'] = df['next_week_price_last_2year'].astype(float)
       df['price_1_year_back_perc'] = df['price_1_year_back_perc'].astype(float)
       df['price_2_year_back_perc'] = df['price_2_year_back_perc'].astype(float)
       df['month_of_year'] = df['month_of_year'].astype(int)
       df['monthly_avg_price'] = df['monthly_avg_price'].astype(float)
       df['monthly_var'] = df['monthly_var'].astype(float)
       df['monthly_var_type'] = df['monthly_var_type'].astype(int)
       df['holiday'] = df['holiday'].astype(int)
       df['market_open_day'] = df['market_open_day'].astype(int)
       df['market_close_day'] = df['market_close_day'].astype(int)
       df['threshold_price'] = df['threshold_price'].astype(float)
       df['min_to_avg'] = df['min_to_avg'].astype(float)
       df['max_to_avg'] = df['max_to_avg'].astype(float)
       return df

    
    def merge_dataframes(self, prices_features, abasto_vol_features, expo_vols_features):
        """Merges the input dataframes to create the features dataframe.
        
        #TODO: To include in the pipeline once the resto of data sources are being extracted
        """
        try:
            merge_df = pd.merge(prices_features, abasto_vol_features, how='inner', left_on=['weekly_index', 'product_name'], right_on=['weekly_index', 'product_name'])
            merge_df = pd.merge(merge_df, expo_vols_features, how='inner', left_on=['monthly_index', 'product_name'], right_on=['monthly_index', 'product_name'])
            return merge_df
        except Exception as e:
            logger.error(f"Error during dataframe merge: {str(e)}")
            raise