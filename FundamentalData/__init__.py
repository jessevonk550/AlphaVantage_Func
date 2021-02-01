import logging
import os
import json
import requests as r

import azure.functions as func
from azure.storage.blob import BlobClient 

DL_KEY = os.environ['GE_DATALAKE_KEY']
DL_ACCOUNT_URL = os.environ['GE_DATALAKE_BLOB_URL']
API_KEY = os.environ['ALPHA_VANTAGE_KEY']
BASE_URL = r'https://www.alphavantage.co/query'


def main(event: func.EventGridEvent):

    event_data = event.get_json()
    symbol = event_data.get('symbol')
    statement = event_data.get('statement')

    # Define URL parameters for request to Alpha Vantage API
    params = {
        'apikey': API_KEY,
        'function': statement,
        'symbol': symbol
    }

    response = r.get(url=BASE_URL, params=params)

    try:
        data = response.json()

    except json.JSONDecodeError as e:
        logging.error(f'An error occurred while parsing response data.\n{e}')
        raise e

    try:
        target_blob = BlobClient(
            account_url=DL_ACCOUNT_URL,
            container_name='alpha-vantage',
            blob_name=f'financial_statements/{statement}/{symbol}.json',
            credential=DL_KEY
        )

        target_blob.upload_blob(
            json.dumps(data)
        )
    
    except Exception as e:
        logging.error(f'An error occurred while uploading data to Data Lake.\n{e}')
        raise e


    
    
    

   
    

    



    
