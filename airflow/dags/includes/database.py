from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from airflow.exceptions import AirflowFailException
from includes.constants import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_NAME
import logging

log = logging.getLogger(__name__)

class DBPostgres:
    def __init__(self):        
        self.db_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_NAME}"
        self.engine = None
        self.connection = None
        self._connect()
    
    def _connect(self):
        try:
            self.engine = create_engine(self.db_url)
            self.connection = self.engine.connect()            
            log.info("Connection to the database established successfully.")
        except SQLAlchemyError as e:
            log.error(f"Error connecting to the database: {str(e)}")
            raise AirflowFailException(f"Error while connecting to PostgreSQL: {str(e)}")
    
    def execute_query(self, query: str):        
        try:
            with self.engine.begin() as connection:
                connection.execute(text(query))           
            log.info("Query executed successfully.")
        except SQLAlchemyError as e:
            log.error(f"Error executing query: {str(e)}")
            raise AirflowFailException(f"Failed to execute query: {str(e)}")
    
    def execute_select_query(self, query):
        try:
            if not self.connection:
                self._connect()  # Conecta se não estiver conectado
            result = self.connection.execute(text(query))            

            logging.info("SELECT query executed successfully.")
            
            return result.fetchall()            
        except SQLAlchemyError as e:
            logging.error(f"Error executing SELECT query: {str(e)}")
            raise AirflowFailException(f"Failed to execute SELECT query: {str(e)}")