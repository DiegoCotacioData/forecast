import logging
from typing import Any
import requests
import json
import logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import sys
import os
os.getcwd() 
sys.executable

print(os.getcwd())


from etl_utils import EndpointRequestor
from etl_utils import GoogleSheets
import etl_settings as etl





class SipsaPricesETL:

    """
        Initializes the SipsaPricesETL class that extracts the daily prices of
        agricultural products traded in supply centers and market places.
    """


    def __init__(self):
        self.product_sipsa_codes = etl.PRODUCTS_SIPSA_CODES
        self.sipsa_request_headers = etl.SIPSA_REQUEST_HEADERS
        self.sipsa_url = etl.SIPSA_API_URL
        self.endpoint_requestor = EndpointRequestor(self.sipsa_url, self.sipsa_request_headers, 'SIPSA')


    def run(self):

        """ Runs the SipsaPricesETL pipeline. """
        df_extracted = self.extract()
        df_transformed = self.transform(df_extracted)
        self.load(df_transformed)


    def extract(self) -> pd.DataFrame:

        """
        Extracts data from SIPSA API.
        Returns:
            pd.DataFrame: Extracted SIPSA data.
        """
        product_dataframes_list = []

        for product, product_sipsa_id in self.product_sipsa_codes.items():
            sipsa_product_data = json.dumps(self.get_sipsa_product_data(product_sipsa_id))
            sipsa_response = self.endpoint_requestor.post(payload=sipsa_product_data)

            product_dataframes_list.append(pd.DataFrame(sipsa_response))
            logging.info(f'[SIPSA_PIPELINE]| Data for {product} has been extracted successfully.')

        if not product_dataframes_list:
            logging.warning('[SIPSA_PIPELINE]| No dataframes to concatenate.')
            raise Exception('No dataframes to concatenate.')

        sipsa_data = pd.concat(product_dataframes_list)
        logging.info('[SIPSA_PIPELINE]| Data extraction completed successfully.')

        return sipsa_data



    def transform(self, raw_sipsa_df: pd.DataFrame) -> pd.DataFrame:

        """
        Cleans and transforms the raw SIPSA data.
        Args:
            raw_sipsa_df (pd.DataFrame): Raw SIPSA data.
        Returns:
            pd.DataFrame: Transformed SIPSA data.
        """

        logging.info(f'[SIPSA_PIPELINE]| ready to clean data.')
        #raw_sipsa_df.drop(['NOM_ABASTO', 'COD_ART', 'VAL_MAX', 'VAL_MIN', 'VAR_DIARIA'], axis=1, inplace=True)
        raw_sipsa_df.drop(['COD_ART','VAR_DIARIA'], axis=1, inplace=True)
        raw_sipsa_df.rename(
            columns={'PROM_DIARIO': 'avg_price',
                     'Date': 'created_at',
                     'NOM_ABASTO': 'central_mayorista',
                     'NOM_ART': 'product_name',
                     'VAL_MAX': 'max_price',
                     'VAL_MIN': 'min_price',
                     },
            inplace=True
        )
        raw_sipsa_df['created_at_datetime'] = pd.to_datetime(raw_sipsa_df['created_at']).dt.date
        raw_sipsa_df['created_at'] = raw_sipsa_df['created_at_datetime'].astype(str)
        sipsa_df = raw_sipsa_df[['created_at', 'central_mayorista',
                                  'product_name', 'avg_price',
                                  'max_price','min_price']].copy()
        logging.info(f'[SIPSA_PIPELINE]| data has been cleaned successfully.')

        return sipsa_df


    def get_sipsa_product_data(self, product_sipsa_id: str) -> dict:
        start_date_sipsa_format = '20130101'
        end_date = datetime.today() #- timedelta(days=1)
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
    

    def load(self, df_transformed):

        """
        Loads the transformed SIPSA data into Google Sheets.

        Args:
            df_transformed: Transformed SIPSA data.
        """

        load_results = GoogleSheets()

        output_info = {
            "dataframe": df_transformed,
            "url": etl.PRICES_URL_TABLE,
            "worksheet_name": etl.PRICES_WORKSHEETNAME,
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


if __name__=="__main__":

  prices_pipeline = SipsaPricesETL()
  prices_pipeline.run()