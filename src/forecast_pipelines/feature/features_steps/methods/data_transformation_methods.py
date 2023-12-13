

#import logging
from typing import Any
#import requests
#import json
import logging
from datetime import datetime, timedelta, date
import pandas as pd
import numpy as np
#import holidays



# Transformation methods for exrtrated dataframes


# Abasto_vols methods


# Expo_vols methods




# Prices methods

class PricesSchemaOps:

    def __init__(self, raw_sipsa_prices):

        self.raw_prices = raw_sipsa_prices.sort_values(by=['product_name','created_at'])
        

    # Main Method

    def apply_schema_transformations(self):

        prices_formated = self.transform_data_types(self.raw_prices)
        prices_df_transf = self.transform_index(prices_formated)

        return prices_df_transf

    # Local Methods

    def transform_data_types(self, raw_prices):
        try:
            raw_prices['avg_price'] = raw_prices['avg_price'].astype(int)
            raw_prices['max_price'] = raw_prices['max_price'].astype(int)
            raw_prices['min_price'] = raw_prices['min_price'].astype(int)
            raw_prices['created_at'] = pd.to_datetime(raw_prices['created_at'])
            return raw_prices
        except Exception as e:
            print("An error occurred during data types transformation:", str(e))
            return None


    def transform_index(self, prices_formated):
        try:
            #prices_formated['created_at'] = pd.to_datetime(prices_formated['created_at'])
            df_ref = self.create_reference_dates(prices_formated)

            if 'created_at' in prices_formated.columns and 'product_name' in prices_formated.columns:
                df_idx = pd.merge(df_ref, prices_formated, on=['created_at', 'product_name'], how='left')
                prices_dataframe = df_idx
                return prices_dataframe
            else:
                raise ValueError("Las columnas 'created_at' y 'product_name' deben existir en el DataFrame principal.")

        except Exception as e:
            print("Se produjo un error:", str(e))
            return None
        

    def create_reference_dates(self, df):
      try:
        start_date = date(2013, 1, 1)
        #df['created_at'] = pd.to_datetime(df['created_at'])
        end_date = df['created_at'].max().date()
        date_range = pd.date_range(start=start_date, end=end_date)
        filtered_dates = [date for date in date_range if date.weekday() != 6]
       
        # TODO: instanciar la lista en settings
        products = ['Limón Tahití', 'Naranja Valencia',
                    'Aguacate Hass', 'Aguacate papelillo',
                    'Plátano hartón verde llanero','Plátano hartón verde',              # Modificar lista a partir de clases de product_name del df
                    'Maracuyá','Papaya tainung',
                    'Lulo', 'Papa parda pastusa','Cebolla cabezona blanca',
                    'Piña gold', 'Zanahoria']

        df_list = []
        for product in products:
            df_base = pd.DataFrame({'created_at': filtered_dates})
            df_base['product_name'] = product
            df_list.append(df_base)

        df_base_ref = pd.concat(df_list, ignore_index=True)
        return df_base_ref

      except Exception as e:
        print("Se produjo un error en create_reference_dates:", str(e))



class PricesImputerOps:

    def __init__(self, prices_transf):

        self.df = prices_transf

 
     # Main Method

    def apply_imputer_transformations(self):

        df_imputed = self.custom_nan_imputer(self.df)
        final_df_imputed = self.impute_zeros(df_imputed)

        return final_df_imputed

      # Local Methods

    def custom_nan_imputer(self, df):
        df_sorted = df.sort_values(['product_name', 'created_at'])
        df_sorted['weekday'] = df_sorted['created_at'].dt.weekday

        categorical_cols = df.select_dtypes(include=['object']).columns
        numeric_cols = df.select_dtypes(include=['number']).columns

        for cat_col in categorical_cols:
            df_sorted[cat_col] = df_sorted.groupby('product_name')[cat_col].ffill()

        for num_col in numeric_cols:
            df_sorted[num_col] = df_sorted.groupby('product_name')[num_col]\
                .transform(lambda x: x.ffill() if x.iloc[0] in [4, 5] else x.interpolate())

        df_sorted = df_sorted.drop(columns=['weekday'])

        return df_sorted

    @staticmethod
    def impute_zeros(df):
        df['min_price'] = np.where(df['min_price'] == 0, df['avg_price'] - (df['max_price'] - df['avg_price']), df['min_price'])

        return df
