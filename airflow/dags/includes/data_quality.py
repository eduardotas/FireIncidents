from airflow.exceptions import AirflowFailException
import logging

log = logging.getLogger(__name__)

class DataQuality:
    def __init__(self, process_name: str):
        self.process_name = process_name        

    def check_empty_dataframe(self, df):
        log.info(f"DQ check_empty_dataframe ...")
        if df.rdd.isEmpty():
            raise AirflowFailException(f"The DataFrame loaded is empty at {self.process_name}.")
        
    def check_expected_schema(self, df, expected_schema):
        ### TODO NOT WORKING
        log.info(f"DQ check_expected_schema ...")
        df_schema = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
        expected_sc= [(f.name, f.dataType.simpleString()) for f in expected_schema.fields]

        if df_schema != expected_sc:
            raise AirflowFailException(f"Erro Schema")
        
    def check_duplicates(self, df, keys):              
        df = df.dropDuplicates(keys)                
        return df