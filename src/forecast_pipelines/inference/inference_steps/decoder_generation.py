
import pandas as pd
import numpy as np
from pytorch_forecasting import Baseline, NHiTS,TemporalFusionTransformer, TimeSeriesDataSet, DeepAR

from ...feature.features_steps.methods.feature_engineering_methods import PricesFeatureEngineering
from utils.logger_config import get_logger
logger = get_logger(__name__)



class InferenceDatasetCreationStep:


    def __init__(self,
                 max_encoder_length: int,
                 max_prediction_length: int,
                 batch_size: int,
                 training_artifacts: dict,
                 data: pd.DataFrame) -> None:

        self.max_encoder_length = max_encoder_length
        self.max_prediction_length = max_prediction_length
        self.batch_size = batch_size
        self.training_artifacts = training_artifacts
        self.data= data



    def create_prediction_data(self):

        """
        Create the decoder dataset and dataloader to make predictions

        Args:
            max_encoder_length (Int): context window
            max_prediction_length (Int): forecast horizon

        Return: 
            forecast_dataloaders (Pytorch Dataloaders):
            decoder_data (Dataframe): it is the inference dataframe
            new_prediction_data (Dataframe): 

        """

        forecast_dataloaders = {}

        for model_name, training in self.training_artifacts.items():
          training_dataset = training['training_dataset']

          # Create Encoder: The same number of index from training data
          encoder_data = self.data[ self.data["time_idx"] >  self.data["time_idx"].max() - self.max_encoder_length ]

          # Create Decoder:  The dataframe with all the features for the forecast horizon (5 day in the future)
          last_data =  self.data[ self.data["time_idx"] ==  self.data["time_idx"].max()]
          
          decoder_data = pd.concat(
              [last_data.assign(created_at=lambda x: x.created_at + pd.DateOffset(days=i)) for i in range(1, self.max_prediction_length + 1)],
              ignore_index=True)

          # Add index to decoder new days
          max_encoder_time_idx = encoder_data["time_idx"].max()
          min_decoder_time_idx = (decoder_data["created_at"] - decoder_data["created_at"].min()).dt.days + 1
          decoder_data["time_idx"] = max_encoder_time_idx + min_decoder_time_idx

          # A conditional that replaces Sundays with the next day if there is one in the Decoder. Sundays do not exist in our use case.
          if (decoder_data['created_at'].dt.dayofweek == 6).any():

              first_sunday_index = np.where(decoder_data['created_at'].dt.dayofweek == 6)[0][0]
              unique_dates = decoder_data['created_at'].unique()
              date_mapping = {date: date + pd.Timedelta(days=1) for date in unique_dates}
              decoder_data['created_at'].iloc[first_sunday_index:] = decoder_data['created_at'].iloc[first_sunday_index:].map(date_mapping)
          else:
              pass

          # Apply Feature engineering for known future variables in decoder
          decoder_data = self.apply_decoder_feature_engineering(decoder_data)

          prediction_data = pd.concat([encoder_data, decoder_data], ignore_index=True).fillna(0)
          prediction_data['week_idx'] = prediction_data['created_at'] - pd.to_timedelta(prediction_data['created_at'].dt.weekday, unit='d')
          
          prediction_data = self.calculate_weekly_signals(prediction_data, encoder_data)
          new_prediction_data = self.normalize_new_percentiles(prediction_data, encoder_data)


          max_date_encoder = encoder_data["created_at"].max()
          condition =  new_prediction_data['created_at'] > max_date_encoder
          new_prediction_data.loc[condition, 'avg_price'] = 0
          
          new_prediction_data.drop('week_idx', axis=1, inplace=True)

          # Create the TimeSeriesDataset Object required for inference
          new_data = TimeSeriesDataSet.from_dataset(training_dataset, new_prediction_data, predict=True, stop_randomization=True)

          # Create the Dataloader Object required for inference
          forecast_dataloader = new_data.to_dataloader(train=False, batch_size=self.batch_size, num_workers=8)  # Output

          # Store the inference dataloaders in the dict
          forecast_dataloaders[model_name] = forecast_dataloader


        return forecast_dataloaders, decoder_data, new_prediction_data




    # Custom feature engineering methods for decoder

    def apply_decoder_feature_engineering(self, decoder_data):

        """ Apply some feature engineering calculations for decoder dataframe """
        feature_ops = PricesFeatureEngineering(decoder_data)
        feature_ops.calculate_day_of_week()
        feature_ops.calculate_day_of_month()
        feature_ops.calculate_year()
        feature_ops.calculate_week_of_year()
        feature_ops.calculate_month_of_year()
        feature_ops.add_holidays_column()
        feature_ops.calculate_market_open_day()
        feature_ops.calculate_market_close_day()
        return feature_ops.df

    
    def calculate_weekly_signals(self, prediction_data, decoder_data):
        """ Estimate weekly features """

        df = prediction_data.copy()
        decoder = decoder_data.copy()

        df['created_at'] = pd.to_datetime(df['created_at'])
        decoder['created_at'] = pd.to_datetime(decoder['created_at'])

        weekly_avg_price = df.groupby(['product_name', 'week_idx'])['avg_price'].mean()
        weekly_var = weekly_avg_price.groupby('product_name').pct_change() * 100
        weekly_var_type = weekly_var.groupby('product_name', group_keys=False).apply(lambda x: x.apply(lambda y: 1 if y > 0 else (-1 if y < 0 else 0)))
        #weekly_var_type = weekly_var.groupby('product_name').apply(lambda x: x.apply(lambda y: 1 if y > 0 else (-1 if y < 0 else 0)))
        week_price_last_1year = df.groupby(['product_name', 'week_idx'])['avg_price'].mean().shift(52)
        next_week_price_last_1year = df.groupby(['product_name','week_idx'])['avg_price'].mean().shift(51)
        week_price_last_2year = df.groupby(['product_name', 'week_idx'])['avg_price'].mean().shift(104)
        next_week_price_last_2year = df.groupby(['product_name','week_idx'])['avg_price'].mean().shift(103)

        weekly_df = pd.DataFrame({'weekly_avg_price': weekly_avg_price,
                                  'weekly_var': weekly_var,
                                  'weekly_var_type': weekly_var_type,
                                  'week_price_last_1year':week_price_last_1year,
                                  'next_week_price_last_1year':next_week_price_last_1year,
                                  'week_price_last_2year': week_price_last_2year,
                                  'next_week_price_last_2year': next_week_price_last_2year,
                                    })

        df = pd.merge(df, weekly_df, on=['product_name', 'week_idx'], how='left', suffixes=('', '_new'))

        condition = df['created_at'] > decoder['created_at'].max()

        df.loc[condition, ['weekly_avg_price', 'weekly_var', 'weekly_var_type', 'week_price_last_1year', 'week_price_last_2year','next_week_price_last_1year','next_week_price_last_2year']] = df.loc[condition, ['weekly_avg_price_new', 'weekly_var_new', 'weekly_var_type_new', 'week_price_last_1year_new', 'week_price_last_2year_new','next_week_price_last_1year','next_week_price_last_2year']].values

        df = df.drop(columns=['weekly_avg_price_new', 'weekly_var_new', 'weekly_var_type_new',
                                'week_price_last_1year_new', 'week_price_last_2year_new',
                                'next_week_price_last_1year_new','next_week_price_last_1year_new'])

        return df



    

    def normalize_new_percentiles(self, prediction_data, encoder_data):


        original = prediction_data.copy()
        df = prediction_data.copy()

        def apply_transformations(df, time_col, price_col, class_col, years_back):
            df_normalized = df.copy()
            percentiles = df_normalized.groupby([class_col, time_col])[price_col].quantile([0, 0.25, 0.5, 0.75, 1]).unstack()

            def assign_percentile(row):
                year = row[time_col]
                price = row[price_col]
                class_name = row[class_col]
                return (price - percentiles.at[(class_name, year), 0]) / (percentiles.at[(class_name, year), 1] - percentiles.at[(class_name, year), 0])

            col_name = f"price_{years_back}_year_back_perc"
            df_normalized[col_name] = df_normalized.apply(assign_percentile, axis=1)
            return df_normalized

        df = apply_transformations(df, "year", "week_price_last_1year", "product_name", 1)
        df = apply_transformations(df, "year", "week_price_last_2year", "product_name", 2)

        max_date_encoder = encoder_data["created_at"].max()
        condition = original["created_at"] > max_date_encoder
        original.loc[condition, ["price_1_year_back_perc", "price_2_year_back_perc"]] = df.loc[condition, ["price_1_year_back_perc", "price_2_year_back_perc"]]

        return original



