import pandas as pd
import numpy as np
import holidays






# abasto_vol feature engineering class

# expo_vol feature engineering class
 
# sipsa prices feature engineering class

class PricesFeatureEngineering:

    def __init__(self, df_imputed):
        self.df = df_imputed
        self.df['created_at'] = pd.to_datetime(self.df['created_at'])

        # Main Method

    def apply_feature_engineering(self):

        # daily signals
        self.calculate_day_of_week()
        self.calculate_day_of_month()
        self.calculate_var_last_day()
        self.calculate_var_same_day_last_week()
        self.calculate_year()
        #self.daily_price_last_years()

        # weekly signals
        self.calculate_week_idx()
        self.calculate_week_of_year()
        self.calculate_weekly_signals()
        self.normalize_using_percentiles()


        # monthly signals
        self.calculate_month_of_year()
        self.calculate_month_idx()
        self.calculate_monthly_signals()

        # context signals
        self.add_holidays_column()
        self.mapear_categorias()
        self.calculate_market_open_day()
        self.calculate_market_close_day()
        self.calculate_threshold_price()
        self.calculate_min_to_avg()
        self.calculate_max_to_avg()

        #others
        self.cleaning_ops()

        return self.df


        # Local Methods

    def calculate_day_of_week(self):
        self.df['day_of_week'] = self.df['created_at'].dt.dayofweek  # Feature

    def calculate_day_of_month(self):
        self.df['day_of_month'] = self.df['created_at'].dt.day      # Feature

    def calculate_year(self):
        self.df['year'] = self.df['created_at'].dt.year # Feature

    def calculate_month_of_year(self):
        self.df['month_of_year'] = self.df['created_at'].dt.month     # Feature

    def calculate_week_of_year(self):
        self.df['week_of_year'] = self.df['created_at'].dt.isocalendar().week.astype(int)     # Feature

    def calculate_market_open_day(self):
        self.df['market_open_day'] = (self.df['created_at'].dt.dayofweek == 0).astype(int)     # Feature

    def calculate_market_close_day(self):
        self.df['market_close_day'] = (self.df['created_at'].dt.dayofweek == 5).astype(int)     # Feature

    def calculate_threshold_price(self):
        self.df['threshold_price'] = self.df['max_price'] - self.df['min_price']     # Feature

    def calculate_min_to_avg(self):
        self.df['min_to_avg'] = self.df['avg_price'] - self.df['min_price']     # Feature

    def calculate_max_to_avg(self):
        self.df['max_to_avg'] = self.df['max_price'] - self.df['avg_price']     # Feature

    def calculate_var_last_day(self):
        self.df['var_last_day'] = (self.df['avg_price'] - self.df['avg_price'].shift(1)) / self.df['avg_price'].shift(1)     # Feature

    def calculate_var_same_day_last_week(self):
        self.df['var_same_day_last_week'] = (self.df['avg_price'] - self.df['avg_price'].shift(7)) / self.df['avg_price'].shift(7)     # Feature

    def calculate_week_idx(self):
        self.df['week_idx'] = self.df['created_at'] - pd.to_timedelta(self.df['created_at'].dt.weekday, unit='d')     # Feature

    def calculate_weekly_signals(self):
        df = self.df.copy()
        df['created_at'] = pd.to_datetime(df['created_at'])
        df.sort_values(by='created_at', inplace=True)
        df.set_index('created_at', inplace=True)

        weekly_avg_price = df.groupby(['product_name', 'week_idx'])['avg_price'].mean()     # Feature
        weekly_var = weekly_avg_price.groupby('product_name').pct_change() * 100     # Feature
        weekly_var_type = weekly_var.groupby('product_name', group_keys=False).apply(lambda x: x.apply(lambda y: 1 if y > 0 else (-1 if y < 0 else 0)))
        #weekly_var_type = weekly_var.groupby('product_name').apply(lambda x: x.apply(lambda y: 1 if y > 0 else (-1 if y < 0 else 0)))     # Feature
        week_price_last_1year = df.groupby(['product_name','week_idx'])['avg_price'].mean().shift(52)
        next_week_price_last_1year = df.groupby(['product_name','week_idx'])['avg_price'].mean().shift(51)
        week_price_last_2year = df.groupby(['product_name','week_idx'])['avg_price'].mean().shift(104)
        next_week_price_last_2year = df.groupby(['product_name','week_idx'])['avg_price'].mean().shift(103)

        weekly_df = pd.DataFrame({'weekly_avg_price': weekly_avg_price,
                                  'weekly_var': weekly_var,
                                  'weekly_var_type': weekly_var_type,
                                  'week_price_last_1year':week_price_last_1year,
                                  'next_week_price_last_1year':next_week_price_last_1year,
                                  'week_price_last_2year': week_price_last_2year,
                                  'next_week_price_last_2year': next_week_price_last_2year,
                                  })

        df.reset_index(inplace=True)
        df = pd.merge(df, weekly_df, on=['product_name', 'week_idx'], how='left')

        self.df = df



    def daily_price_last_years(self):
        df = self.df.copy()
        def calculate_avg_last_years(row, col_name, years_back):
            years_ago = row['created_at'] - pd.DateOffset(years=years_back)
            avg_last_years = df[(df['created_at'] == years_ago) & (df['product_name'] == row['product_name'])][col_name].values
            return avg_last_years[0] if len(avg_last_years) > 0 else row[col_name]

        df['price_last_1year'] = df.apply(lambda row: calculate_avg_last_years(row, 'avg_price', 1), axis=1)
        df['price_last_2year'] = df.apply(lambda row: calculate_avg_last_years(row, 'avg_price', 2), axis=1)
        df.ffill(inplace=True)

    def normalize_using_percentiles(self):

        df = self.df.copy()
        dfx = df.copy()

        def apply_transformations(dfx, time_col, price_col, class_col, years_back):
            df_normalized = dfx
            percentiles = dfx.groupby([class_col, time_col])[price_col].quantile([0, 0.25, 0.5, 0.75, 1]).unstack()

            def assign_percentile(row):
                year = row[time_col]
                price = row[price_col]
                class_name = row[class_col]
                return (price - percentiles.at[(class_name, year), 0]) / (percentiles.at[(class_name, year), 1] - percentiles.at[(class_name, year), 0])

            col_name = f"price_{years_back}_year_back_perc"
            df_normalized[col_name] = df_normalized.apply(assign_percentile, axis=1)
            return df_normalized

        df = apply_transformations(df, "year", "week_price_last_1year", "product_name", 1)
        df = apply_transformations(df, "year", "next_week_price_last_1year", "product_name", 1)
        df = apply_transformations(df, "year", "week_price_last_2year", "product_name", 2)
        df = apply_transformations(df, "year", "next_week_price_last_2year", "product_name", 2)

        self.df = df


    def calculate_month_idx(self):
        # Calcula el primer día del mes para la columna 'created_at'
        self.df['month_idx'] = self.df['created_at'] - pd.to_timedelta(self.df['created_at'].dt.day - 1, unit='d')     # Feature


    def calculate_monthly_signals(self):
        df = self.df.copy()
        df['created_at'] = pd.to_datetime(df['created_at'])
        df.sort_values(by='created_at', inplace=True)
        df.set_index('created_at', inplace=True)

        # Features
        monthly_avg_price = df.groupby(['product_name', 'month_idx'])['avg_price'].mean()     # Feature
        monthly_var = monthly_avg_price.groupby('product_name').pct_change() * 100     # Feature
        monthly_var_type  = monthly_var.groupby('product_name', group_keys=False).apply(lambda x: x.apply(lambda y: 1 if y > 0 else (-1 if y < 0 else 0)))
        #monthly_var_type = monthly_var.groupby('product_name').apply(lambda x: x.apply(lambda y: 1 if y > 0 else (-1 if y < 0 else 0)))     # Feature

        monthly_df = pd.DataFrame({'monthly_avg_price': monthly_avg_price,
                                  'monthly_var': monthly_var,
                                  'monthly_var_type': monthly_var_type,
                                  })

        df.reset_index(inplace=True)
        df = pd.merge(df, monthly_df, on=['product_name', 'month_idx'], how='left')

        self.df = df


    def add_holidays_column(self):
        unique_dates = self.df['created_at'].dt.date.unique()
        co_holidays = holidays.Colombia(years=unique_dates[0].year)

        def is_holiday(date):
            return 1 if date in co_holidays else 0

        self.df['holiday'] = self.df['created_at'].dt.date.apply(is_holiday)     # Feature
       
    # TODO: Montar en forecast settings
    def mapear_categorias(self):
        mapping_product_name = {
            'Naranja Valencia': 'item_01',
            'Aguacate papelillo': 'item_02',
            'Aguacate Hass': 'item_03',
            'Limón Tahití': 'item_04',
            'Plátano hartón verde llanero': 'item_05',
            'Plátano hartón verde': 'item_06',
            'Maracuyá': 'item_07',
            'Papaya tainung': 'item_08',
            'Lulo': 'item_09',
            'Papa parda pastusa': 'item_10',
            'Cebolla cabezona blanca': 'item_11',
            'Piña gold': 'item_12',
            'Zanahoria': 'item_13',
            }

        mapping_central_mayorista = {'Bogotá, D.C., Corabastos': 'location_01'}

        self.df['product_name'] = self.df['product_name'].map(mapping_product_name)     # Feature
        self.df['central_mayorista'] = self.df['central_mayorista'].map(mapping_central_mayorista)     # Feature

    def cleaning_ops(self):
        min_date = self.df['created_at'].min()
        min_month = min_date.strftime('%Y-%m')

        self.df = self.df[~self.df['created_at'].dt.strftime('%Y-%m').eq(min_month)]


        if 'week_idx' in self.df.columns:
            self.df.drop('week_idx', axis=1, inplace=True)
        if 'month_idx' in self.df.columns:
            self.df.drop('month_idx', axis=1, inplace=True)