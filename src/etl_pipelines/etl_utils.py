import logging
from typing import List

import pygsheets
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import concurrent.futures
import pandas as pd
import logging
import requests
from typing import Any



class EndpointRequestor:
    def __init__(self, url: str, headers: dict, pipeline: str):
        self.url = url
        self.headers = headers
        self.pipeline = pipeline

    def post(self, payload: Any):
        try:
            logging.info(f'[Endpoint Requestor]| ready to send POST request to {self.url}')
            response = requests.request('POST', self.url, headers=self.headers, data=payload)
            if response.ok:
                logging.info(f'[Endpoint Requestor]| request has been successfully sent to {self.url}')
                return response.json()
        except Exception as ex:
            logging.error(f'[Endpoint Requestor]| error while sending POST request to {self.url}: {ex}')
            raise ex


class GoogleSheets:

    credentials = Credentials.from_service_account_file(
        'src/utils/dashboard_manager_google_creds.json',
        scopes=['https://www.googleapis.com/auth/spreadsheets']
    )
    gc = gspread.authorize(credentials)
    pyg = pygsheets.authorize(service_file='src/utils/dashboard_manager_google_creds.json')
    
    """def import_sheets(self, url, worksheet_name):
        logging.info(f'[GoogleSheets:import_sheets]| extraction process for table {worksheet_name} has started')
        sheet = self.gc.open_by_url(url)
        worksheet = sheet.worksheet(worksheet_name)
        rows = worksheet.get_all_values()
        df = pd.DataFrame.from_records(rows)
        df.columns = df.iloc[0]
        df.drop([0], axis=0, inplace=True)
        logging.info(f'[GoogleSheets:import_sheets]| extraction process for table {worksheet_name} has finished')
        return df"""

    def parallel_import_sheets(self, url_worksheet_dataframe_names: list) -> dict:
        """
        Import data from multiple Google Sheets in parallel.
        Using a ThreadPoolExecutor, this method will import data from multiple Google Sheets in parallel.
        Then it will return a dictionary with the imported DataFrames under their specified names.
        If any of the imports fail, the error will be logged and the DataFrame will not be included in the result.

        Parameters:
            url_worksheet_dataframe_names (list): A list of tuples containing (url, worksheet_name, dataframe_name).

        Returns:
            dict: A dictionary with the imported DataFrames under their specified names.
        """
        with concurrent.futures.ThreadPoolExecutor() as executor:
            import_futures = {
                dataframe_name: executor.submit(self.import_single_sheet, url, worksheet_name)
                for url, worksheet_name, dataframe_name in url_worksheet_dataframe_names
            }

        imported_dfs = {
            dataframe_name: future.result()
            for dataframe_name, future in import_futures.items()
        }

        for url, worksheet_name, dataframe_name in url_worksheet_dataframe_names:
            if dataframe_name not in imported_dfs:
                logging.error(f'Error importing sheet "{worksheet_name}" from URL "{url}": Future result not available')

        return imported_dfs

    def import_single_sheet(self, url, worksheet_name):
        """
        Import data from a single Google Sheet

        Parameters:
            url (str): The URL of the Google Sheet.
            worksheet_name (str): The name of the worksheet within the Google Sheet.

        Returns:
            a DataFrame containing the data from the Google Sheet.
        """
        try:
            df = self.import_sheets(url, worksheet_name)
            return df
        except Exception as e:
            logging.error(f'Error importing sheet "{worksheet_name}" from URL "{url}": {e}')
            return None

    def export_sheets(self, dataframe, url, worksheet_name, row=1, col=1, clear_sheet=False):
        """
        :param dataframe: dataframe to be exported
        :param url: url of the Google sheet
        :param worksheet_name: name of the worksheet
        :param row: row to start exporting
        :param col: column to start exporting
        :param clear_sheet: clear the sheet before exporting
        Updates the Google sheet with the dataframe using the gspread library
        """
        logging.info(f'[GoogleSheets:export_sheets]| export process for table {worksheet_name} has started')
        logging.info(f'[GoogleSheets:export_sheets]| Using URL: {url}')
        sheet = self.gc.open_by_url(url)
        worksheet = sheet.worksheet(worksheet_name)
        if clear_sheet:
            worksheet.clear()
        set_with_dataframe(worksheet, dataframe, row=row, col=col)

    def parallel_export_sheets(self, export_info_list: list):
        """
        Parallelize the export of dataframes to Google Sheets.

        Parameters:
            export_info_list (list): A list of tuples containing (dataframe, url, worksheet_name, row, col, clear_sheet).

        Returns:
            None
        """
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for export_info in export_info_list:
                executor.submit(self.export_sheets, *export_info)

    def append_sheets(self, dataframe, url, worksheet_name):
        dataframe = dataframe.astype(str)
        sh = self.pyg.open_by_url(url)
        wk = sh.worksheet_by_title(worksheet_name)
        wk.append_table(values=dataframe.values.tolist())

    def update_or_append_sheets(self,
                                dataframe: pd.DataFrame,
                                url: str,
                                worksheet_name: str,
                                unique_columns: List[str]):
        """
        Update existing values in a Google Sheet based on unique columns or append new rows.

        Parameters:
        - dataframe (pd.DataFrame): DataFrame to be exported.
        - url (str): URL of the Google Sheet.
        - worksheet_name (str): Name of the worksheet.
        - unique_columns (List[str]): List of columns that uniquely identify a row.

        Note:
        1. The function updates the sheet in-place and does not return any value.
        2. probably is a TODO, but, if you want only to update columns that have changed, you need to modify the code
        at line: if (matching_row[updatable_cols] != row[updatable_cols]).any(): in order to get the columns that need
        an update, modifying updatable_cols variable
        """
        dataframe = dataframe.astype(str)
        sh = self.pyg.open_by_url(url)
        wk = sh.worksheet_by_title(worksheet_name)

        existing_data = wk.get_all_records()
        existing_df = pd.DataFrame(existing_data)

        mask_exist = dataframe[unique_columns].isin(existing_df[unique_columns].to_dict(orient='list')).all(axis=1)
        existing_rows = dataframe[mask_exist]
        new_rows = dataframe[~mask_exist]

        updatable_cols = [item for item in existing_df.columns if item not in unique_columns]
        for _, row in existing_rows.iterrows():
            mask = (existing_df[unique_columns] == row[unique_columns]).all(axis=1)
            matching_row = existing_df[mask]
            matching_row_num = matching_row.index[0]

            if (matching_row[updatable_cols] != row[updatable_cols]).any():
                for col_num, value in enumerate(row[updatable_cols], start=1):
                    add_col = col_num + len(updatable_cols) + 1
                    wk.update_value((matching_row_num + 2, add_col), value)

        if not new_rows.empty:
            wk.append_table(values=new_rows.values.tolist())

    def get_last_value(self, url: str, worksheet_name: str, column: int):
        sh = self.pyg.open_by_url(url)
        wk = sh.worksheet_by_title(worksheet_name)
        values = wk.get_col(column, include_tailing_empty=False)
        last_non_blank = None
        for value in reversed(values[1:]):
            if value != '':
                last_non_blank = value
                break
        return last_non_blank
