import logging
import os
import json
import requests as r

import azure.functions as func
from azure.storage.blob import BlobClient 


def main(req: func.HttpRequest) -> func.HttpResponse:

    # Event grid vaidation handshake
    try:
        body = req.get_json()[0] # Only extract first event body
        event_data = body.get('data')
        event_type = body.get('eventType')

        if event_type == "Microsoft.EventGrid.SubscriptionValidationEvent":
            # Extracting nessescary data from handshake body
            logging.info('Received event handshake request.')
            val_url = event_data.get('validationUrl')
            val_code = event_data.get('validationCode')

            # Responding to handshake
            logging.info('Responding to event grid handshake.')
            validation_response = {'validationResponse': val_code}

            return func.HttpResponse(body=json.dumps(validation_response), status_code=200)

        elif event_type == "Microsoft.Storage.BlobCreated":
            logging.info('Received request to handle Ealyze update data.')
        
        else:
            logging.error('Received unknown event type.')
            return func.HttpResponse(status_code=400)
        
    except (json.JSONDecodeError, KeyError) as e:
        logging.error('Received invalid event schema.')
        return func.HttpResponse(status_code=400)
    
    
    DL_KEY = os.environ['GE_DATALAKE_KEY']
    API_KEY = os.environ['ALPHA_VANTAGE_KEY']
    BASE_URL = r'https://www.alphavantage.co/query'
    SOURCES = ['INCOME_STATEMENT', 'CASH_FLOW', 'BALANCE_SHEET']


    # Getting data from Alpha Vantage
    for source in SOURCES:
        # Implement GET requests here
        pass
    

    # Saving data to data lake
    for source in SOURCES:
        # Implement upload procedure here
        # Should define the file structure in the data lake
        pass

    
    
    



    
