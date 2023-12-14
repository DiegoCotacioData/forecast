
from utils.gsheets_connectors import GoogleSheets
import pandas as pd
import service_settings.infra_settings as set


class ExtractInputDataStep:

   #TODO: abasto and expo vols pipelines pending.
   def extract_raw_data(self):
        """
        Extracts raw data from Google Sheets.

        Returns:
            pd.DataFrame: Raw SIPSA prices data.
        """
        extract = GoogleSheets()

        raw_sipsa_prices = extract.import_single_sheet(set.PRICES_URL_TABLE, set.PRICES_WORKSHEETNAME)
        
        #raw_abasto_volumes = self.extract.import_single_sheet(infra_settings.ABASTO_VOLS_URL_TABLE, infra_settings.ABASTO_VOLS_WORKSHEETNAME)
        
        #raw_expo_volumes = self.extract.import_single_sheet(infra_settings.EXPO_VOLS_URL_TABLE, infra_settings.EXPO_VOLS_WORKSHEETNAME)
    
        return raw_sipsa_prices  #,raw_abasto_vols, raw_expo_vols
      