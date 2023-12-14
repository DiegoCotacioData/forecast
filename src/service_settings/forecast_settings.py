import os

# GLOBAL ARGS:

MAX_PREDICTION_LENGTH = 5   # Forecast Horizont
MAX_ENCODER_LENGTH = 620    # Context Window 
BATCH_SIZE = 28           
WINDOW_DAYS = 790           # First training dataframe data point

MODEL_ACCELERATOR = "cpu"  # change to GPU for prod

APP_VERSION_NAME = "..."
APP_VERSION = '1.0.0'


FORECAST_PRODUCTS = {}      # pending
PRODUCTS_MAPPING = {}       # pending
LOCATION_MAPPING = {}       # pending

POSPROCESSING_PRODUCT_MAPPING = {
                'item_01':'Naranja Valencia',
                'item_02':'Aguacate papelillo',
                'item_03':'Aguacate Hass',
                'item_04':'Limón Tahití',
                'item_05':'Plátano hartón verde llanero',
                'item_06':'Plátano hartón verde',
                'item_07':'Maracuyá',
                'item_08':'Papaya tainung',
                'item_09':'Lulo',
                'item_10':'Papa parda pastusa',
                'item_11':'Cebolla cabezona blanca',
                'item_12':'Piña gold',
                'item_13':'Zanahoria'}