




# ETL_PIPELINES
SIPSA_PRICES_API_URL = "https://sen.dane.gov.co/variacionPrecioMayoristaSipsa_ws/rest/SipsaServices/selectAllInfoProduct/"
PRICES_URL_TABLE = "https://docs.google.com/spreadsheets/d/14AYqVEoY24XlKpLgoDHUnasAGeOYENjp-0IrtlOVId4/edit#gid=0"
PRICES_WORKSHEETNAME = "sipsa_prices"

ABASTO_VOLS_URL_TABLE ="https://docs.google.com/spreadsheets/d/14AYqVEoY24XlKpLgoDHUnasAGeOYENjp-0IrtlOVId4/edit#gid=2004764627"
ABASTO_VOLS_WORKSHEETNAME ="sipsa_vol_abasto"

EXPO_VOLS_URL_TABLE ="https://docs.google.com/spreadsheets/d/14AYqVEoY24XlKpLgoDHUnasAGeOYENjp-0IrtlOVId4/edit#gid=1990994690"
EXPO_VOLS_WORKSHEETNAME="dane_vol_expo"



# FORECAST PIPELINES DATABASES:
H_TRAINING_METRICS_URL_TABLE = "https://docs.google.com/spreadsheets/d/14joRv_RBKA8vE0_e5WbnoTt40NpippNHRpuI_-_yIic/edit#gid=0"
H_TRAINING_METRICS_WORKSHEET_TABLE = "FH_TrainingMetrics"

H_RAW_FORECAST_URL_TABLE ="https://docs.google.com/spreadsheets/d/14joRv_RBKA8vE0_e5WbnoTt40NpippNHRpuI_-_yIic/edit#gid=563170116"
H_RAW_FORECAST_WORKSHEET_TABLE = "FH_Raw"

H_MULTIPLE_MODELS_FORECAST_URL_TABLE = "https://docs.google.com/spreadsheets/d/14joRv_RBKA8vE0_e5WbnoTt40NpippNHRpuI_-_yIic/edit#gid=1722357233"
H_MULTIPLE_MODELS_FORECAST_WORKSHEET_TABLE = "FH_MultipleSelected"

H_SINGE_MODEL_FORECAST_URL_TABLE = "https://docs.google.com/spreadsheets/d/14joRv_RBKA8vE0_e5WbnoTt40NpippNHRpuI_-_yIic/edit#gid=533875973"
H_SINGLE_MODEL_FORECAST_WORKSHEET_TABLE = "FH_SingleSelected"

# Current tables: tables connected to dashboards and tools
C_MULTIPLE_MODELS_FORECAST_URL_TABLE = "https://docs.google.com/spreadsheets/d/14joRv_RBKA8vE0_e5WbnoTt40NpippNHRpuI_-_yIic/edit#gid=1753187536"
C_MULTIPLE_MODELS_FORECAST_WORKSHEET_TABLE = "CurrentMultipleForecastSelected"

C_SINGE_MODEL_FORECAST_URL_TABLE = "https://docs.google.com/spreadsheets/d/14joRv_RBKA8vE0_e5WbnoTt40NpippNHRpuI_-_yIic/edit#gid=19977883"
C_SINGLE_MODEL_FORECAST_WORKSHEET_TABLE = "CurrentSingleForecastSelected"

C_RAW_FORECAST_URL_TABLE = "https://docs.google.com/spreadsheets/d/14joRv_RBKA8vE0_e5WbnoTt40NpippNHRpuI_-_yIic/edit#gid=1896141804"
C_RAW_FORECAST_WORKSHEET_TABLE = "CurrentForecastRaw"

C_FEATURES_URL_TABLE = "https://docs.google.com/spreadsheets/d/14joRv_RBKA8vE0_e5WbnoTt40NpippNHRpuI_-_yIic/edit#gid=1880022842"
C_FEATURES_WORKSHEET_TABLE = "H_SIPSA"

C_TRAINING_METRICS_URL_TABLE ="https://docs.google.com/spreadsheets/d/14joRv_RBKA8vE0_e5WbnoTt40NpippNHRpuI_-_yIic/edit#gid=1200174104"
C_TRAINING_METRICS_WORKSHEET_TABLE = "CurrentMetrics"