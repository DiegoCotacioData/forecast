import os
import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.'))
from typing import Any
import pandas as pd


class AbastoVolumesETL:
    """
        Initializes the AbastoVolumnesETL class that extracts the weekly volumes data of
        agricultural products traded in supply centers and market places.

        #TODO: Develop the ETL pipeline
    """


    def __init__(self, datasource_name: str,
                 input_data_source_url: str,
                 output_data_base_url: str,
                 output_worksheet_name: str,
                 waiting_time: str) -> None:
        

        self.datasource_name = datasource_name
        self.input_data_source_url = input_data_source_url
        self.output_data_base_url = output_data_base_url
        self.output_worksheet_name = output_worksheet_name
        self.waiting_time = waiting_time


    def extract(self):
        pass

    def transform(self):
        pass

    def feature_engineering(self):
        pass

    def load(self):
        pass
