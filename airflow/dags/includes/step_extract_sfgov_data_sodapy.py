import os
import json
import logging
import requests
from includes.constants import DATASET_ID, BASE_PATH_BRONZE, FILE_NAME, FILE_EXTENSION, LATEST_STATUS
from includes.utils import LatestStatus
from datetime import datetime, timedelta
from sodapy import Socrata
from airflow.exceptions import AirflowFailException

log = logging.getLogger(__name__)

date = datetime.now().strftime("%Y-%m-%d")
time = datetime.now().strftime("%H%M")

ls = LatestStatus()

DIR_BRONZE = f"{BASE_PATH_BRONZE}{date}"
FILE_PATH = f"{DIR_BRONZE}/{FILE_NAME}{time}{FILE_EXTENSION}"
LIMIT = 50000 # Maximum allowed

def extract_data_from_api():
    """
    Extracts data from the Socrata API in batches of 50,000 records and appends them to a JSON file.
    """
    os.makedirs(DIR_BRONZE, exist_ok=True)
    
    client = Socrata("data.sfgov.org", None)
    offset = 0
    first_batch = True    

    while True:
        try:
            log.info(f"Start offset:{offset}")
            # Fetch data with limit and offset
            data = client.get(DATASET_ID, limit=LIMIT, offset=offset)
            
            if not data:  # If there's no more data, exit the loop
                log.info(f"End offset:{offset} - No data to process")
                break
            
            with open(FILE_PATH, "a", encoding="utf-8") as json_file:
                for item in data:                
                    json.dump(item, json_file, ensure_ascii=False)
                    json_file.write("\n")  # Add a new line after each record
                    
            offset += LIMIT
            log.info(f"End offset:{offset}")
        
        except Exception as e:            
            log.error(f"Error API: {str(e)}")
            raise AirflowFailException(f"Execution failed: {str(e)}")
            break
        
    ls.update_json_last_file(FILE_PATH)    