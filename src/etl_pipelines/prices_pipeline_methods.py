
import os
import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.'))

import logging
from typing import Any
import json
import logging
from datetime import datetime
import pandas as pd
from utils.endpoint_request import EndpointRequestor
from utils.gsheets_connectors import GoogleSheets
from service_settings import etl_settings as etl
from service_settings import infra_settings as infra
from other_methods.prices_methods import PricesPreprocessingOps, PricesFeatureEngineering


class SipsaPricesMethods:

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

        self.sipsa_request_headers = etl.SIPSA_REQUEST_HEADERS
        self.endpoint_requestor = EndpointRequestor(self.input_data_source_url, self.sipsa_request_headers, 'SIPSA')


    def extract(self) -> pd.DataFrame:

        """
        Extracts data from SIPSA API.
        Returns:
            pd.DataFrame: Extracted SIPSA data.
        """

        product_sipsa_codes = {
            'aguacate_hass': '89',
            'aguacate_papelillo': '90',
            'naranja_valencia': '86',
            'limon_tahiti': '81',
            'platano_harton_primera': '189',
            'platano_harton_extra': '187',
            'maracuya': '133',
            'papaya tainung': '6002',
            'lulo': '116',
            'papa parda pastusa': '167',
            'cebolla cabezona blanca': '18',
            'piña gold': '144',
            'zanahoria': '54'
        }
        product_dataframes_list = []

        for product, product_sipsa_id in product_sipsa_codes.items():
            sipsa_product_data = json.dumps(self.get_sipsa_product_data(product_sipsa_id))
            sipsa_response = self.endpoint_requestor.post(payload=sipsa_product_data)

            product_dataframes_list.append(pd.DataFrame(sipsa_response))
            logging.info(f'[SIPSA_PIPELINE]| Data for {product} has been extracted successfully.')

        if not product_dataframes_list:
            logging.warning('[SIPSA_PIPELINE]| No dataframes to concatenate.')
            raise Exception('No dataframes to concatenate.')

        raw_sipsa_df = pd.concat(product_dataframes_list)
        logging.info('[SIPSA_PIPELINE]| Data extraction completed successfully.')

        return raw_sipsa_df

    def get_sipsa_product_data(self, product_sipsa_id: str) -> dict:
        start_date_sipsa_format = '20130101'
        end_date = datetime.today()  # - timedelta(days=1)
        end_date_sipsa_format = end_date.strftime('%Y%m%d')

        sipsa_payload = {
            'depcod': '11',
            'codmun': '11001',
            'codart': product_sipsa_id,
            'archivoCsv': f'1111001{product_sipsa_id}',
            'fechaIni': start_date_sipsa_format,
            'fechaFin': end_date_sipsa_format,
            'tipoReporte': 'day'
        }
        return sipsa_payload
    

    def transform(self, raw_sipsa_df: pd.DataFrame) -> pd.DataFrame:

        """
        Cleans and transforms the raw SIPSA data.
        Args:
            raw_sipsa_df (pd.DataFrame): Raw SIPSA data.
        Returns:
            pd.DataFrame: Transformed SIPSA data.
        """
        data_transformation = PricesPreprocessingOps(raw_sipsa_df)
        transformed_df = data_transformation.apply_prices_preprocessing()

        return transformed_df


    
    def feature_engineering(self, df_transformed):

        """
        Perform feature engineering to formated prices dataframe
        """

        feature_engineering = PricesFeatureEngineering(df_transformed)
        df_features = feature_engineering.apply_feature_engineering()

        return df_features



    def load(self, df_features):

        """
        Loads the transformed SIPSA data into Google Sheets.
        Args:
            df_transformed: Transformed SIPSA data.
        """
        load_results = GoogleSheets()

        output_info = {
            "dataframe": df_features,
            "url": self.output_data_base_url,
            "worksheet_name": self.output_worksheet_name,
            "w": 1,
            "col": 1,
            "clear_sheet": True
        }

        dataframe = output_info["dataframe"]
        url = output_info["url"]
        worksheet_name = output_info["worksheet_name"]
        w = output_info["w"]
        col = output_info["col"]
        clear_sheet = output_info["clear_sheet"]
        load_results.export_sheets(dataframe, url, worksheet_name, w, col, clear_sheet)

        