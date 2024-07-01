from azure.storage.blob import BlobServiceClient
import yfinance as yf
import json
import logging
import datetime
import numpy as np
import os
import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    storage_account_key="**** Enter storage account key****"
    storage_account_name="**** Enter storage account name ****"
    connection_string="*** Enter connection string ****"
    container_name="**** Enter container name *****"

    blob_service_client=BlobServiceClient.from_connection_string(connection_string)

    SYMBOLS = ['APPL','MSFT','AMZN']

    #creating and updating data into a file 
    for symbol in SYMBOLS:
        stock = yf.Ticker(symbol)
        data = stock.info
        json_data=json.dumps(data)
        
        '''def uploadToBlobStorage(file_path, file_name):
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        blob_client.upload_blob()'''

        blob_name=f"{symbol}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=symbol)
        
        # Upload the created file
        blob_client.upload_blob(json_data, overwrite=True)

        logging.info(f"Blob {blob_name} uploaded successfully.")
    
    logging.info('Python timer trigger function ran at %s', utc_timestamp)