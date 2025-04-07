from airflow.exceptions import AirflowFailException
from includes.database import DBPostgres
from sqlalchemy import text
import logging

log = logging.getLogger(__name__)

class DataQuality:
    def __init__(self, process_name: str):
        self.process_name = process_name        

    def check_empty_dataframe(self, df):
        log.info(f"DQ check_empty_dataframe ...")
        # if df.rdd.isEmpty(): # Problem with airflow
        if df.count() == 0:    
            raise AirflowFailException(f"The DataFrame loaded is empty at {self.process_name}.")
        
    def check_expected_schema(self, df, expected_schema):        
        log.info(f"DQ check_expected_schema ...")
        df_schema = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
        expected_sc= [(f.name, f.dataType.simpleString()) for f in expected_schema.fields]

        if df_schema != expected_sc:
            raise AirflowFailException(f"Error processing the schema.")
        
    def check_duplicates(self, df, keys):              
        df = df.dropDuplicates(keys)                
        return df
    
    def check_duplicates_at_table(self,schema, table, coluna_chave = "id"):        
        log.info(f"DQ check_duplicates_at_table ...")
        db = DBPostgres()
        
        query = f"""
            SELECT {coluna_chave}, COUNT(*) 
            FROM {schema}.{table}
            GROUP BY {coluna_chave}
            HAVING COUNT(*) > 1;
        """

        duplicated_values = db.execute_select_query(query)

        if duplicated_values:
            raise AirflowFailException("Failure in duplication check.")