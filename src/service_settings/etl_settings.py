import service_settings.infra_settings as infra
from etl_pipelines.prices_pipeline_methods import SipsaPricesMethods
from etl_pipelines.abasto_vols_pipeline_methods import AbastoVolumesETL
from etl_pipelines.export_vols_pipeline_methods import ExportVolumesETL


class BaseETLConfig:

    def __init__(self):

        self.datasource_name: str = None
        self.input_data_source_url: str = None
        self.output_data_base_url: str = None
        self.output_worksheet_name: str = None
        self.waiting_time: int = None
        self.etl_methods = None


class SipsaPricesPipeline(BaseETLConfig):

    def __init__(self):
        super().__init__()
        self.datasource_name = "sipsa_prices"
        self.input_data_source_url = infra.SIPSA_PRICES_API_URL
        self.output_data_base_url = infra.PRICES_URL_TABLE
        self.output_worksheet_name = infra.PRICES_WORKSHEETNAME
        self.waiting_time = 5
        self.etl_methods = SipsaPricesMethods(self.datasource_name,
                                          self.input_data_source_url,
                                          self.output_data_base_url,
                                          self.output_worksheet_name,
                                          self.waiting_time)
        
class AbastoVolumesPipeline(BaseETLConfig):

    def __init__(self):
        super().__init__()
        self.datasource_name = "volumes_prices"
        self.input_data_source_url = "www.volumes_url.com"
        self.output_data_base_url = "www.volumes_output_url.com"
        self.output_worksheet_name = "volumes_output"
        self.waiting_time = 5
        self.etl_methods = AbastoVolumesETL(self.datasource_name,
                                          self.input_data_source_url,
                                          self.output_data_base_url,
                                          self.output_worksheet_name,
                                          self.waiting_time)
        

class ExportVolumesPipeline(BaseETLConfig):

    def __init__(self):
        super().__init__()
        self.datasource_name = "expo_volumes"
        self.input_data_source_url = "www.expovolumes_url.com"
        self.output_data_base_url = "www.expovolumes_output_url.com"
        self.output_worksheet_name = "expo_volumes_output"
        self.waiting_time = 5
        self.etl_methods = ExportVolumesETL(self.datasource_name,
                                          self.input_data_source_url,
                                          self.output_data_base_url,
                                          self.output_worksheet_name,
                                          self.waiting_time)
        



SIPSA_REQUEST_HEADERS = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'es-419,es;q=0.9',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json',
            'Cookie': 'ec1d9c9ecb5c2f4ef7998e3e52b19fdf=pbves1lfevgso9iovbp41e7slm; _gid=GA1.3.312679753.1690809092; SERVERID=s1; cookiesession1=678B76A19A61DAD67B01DCF9AA04C524; PRETSESSID=jWBPjosQS5%2CryoIf9-OXcE3QZq7i2ucy8c-1; _ga=GA1.1.1541742356.1690214816; _ga_EVNW3DW2NE=GS1.1.1691092597.28.1.1691095862.50.0.0; _ga_MV4DN0WN4F=GS1.1.1691092597.28.1.1691095862.0.0.0',
            'Origin': 'https://sen.dane.gov.co',
            'Referer': 'https://sen.dane.gov.co/variacionPrecioMayoristaSipsa_Client/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"'
            }

