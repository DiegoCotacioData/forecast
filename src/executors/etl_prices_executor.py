import os, sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from service_settings.etl_settings import SipsaPricesPipeline



class SipsaPricesExecutor:
    def __init__(self):
        self.pipeline = SipsaPricesPipeline()

    def run_pipeline(self):
        etl_method = self.pipeline.etl_methods
        df_extracted = etl_method.extract()
        df_transformed = etl_method.transform(df_extracted)
        df_features = etl_method.feature_engineering(df_transformed)
        etl_method.load(df_features )

if __name__ == "__main__":
    executor = SipsaPricesExecutor()
    executor.run_pipeline()