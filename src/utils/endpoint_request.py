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
