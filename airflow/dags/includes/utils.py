import os
import json
import logging
from datetime import datetime
from includes.constants import LATEST_STATUS, LATEST_STATUS_STRUCT, BASE_PATH_BRONZE

log = logging.getLogger(__name__)

class LatestStatus:
    def __init__(self):        
        self.lastest_status_path = f"{BASE_PATH_BRONZE}{LATEST_STATUS}"
        self.check_or_create_status_json()
    
    def check_or_create_status_json(self):
        log.info(f"Checking if {self.lastest_status_path} exists...")
        if not os.path.exists(self.lastest_status_path):
            with open(self.lastest_status_path, "w") as f:
                log.info(f"Creating file {self.lastest_status_path}...")
                json.dump(LATEST_STATUS_STRUCT, f, indent=4)
        
    def update_json_last_file(self,file):
        log.info("Updating status: last_file...")
        with open(self.lastest_status_path, "r") as f:
            status_data = json.load(f)
        
        status_data["last_file"] = file

        with open(self.lastest_status_path, "w") as f:
            json.dump(status_data, f, indent=4)
    
    def update_json_last_update(self):
        
        log.info("Updating status: last_update...")
        date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000")
        
        with open(self.lastest_status_path, "r") as f:
            status_data = json.load(f)
        
        status_data["last_update"] = date

        with open(self.lastest_status_path, "w") as f:
            json.dump(status_data, f, indent=4)
    
    def get_last_file(self):        
        log.info("Geting status: last_file...")
        with open(self.lastest_status_path, "r") as f:
                status_data = json.load(f)
        return status_data.get("last_file", None) 
    
    def get_last_update(self):        
        log.info("Geting status: last_update...")
        with open(self.lastest_status_path, "r") as f:
                status_data = json.load(f)
        return status_data.get("last_update", None)   
