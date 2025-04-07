from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from airflow.exceptions import AirflowFailException
from includes.constants import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_NAME, SCHEMA_GOLD
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
            result = self.connection.execute(text(query))            

            logging.info("SELECT query executed successfully.")
            
            return result.fetchall()            
        except SQLAlchemyError as e:
            logging.error(f"Error executing SELECT query: {str(e)}")
            raise AirflowFailException(f"Failed to execute SELECT query: {str(e)}")
        
    def create_or_refresh_mv(self, query, mv_name):
        try:            
            check_view_query = f"""
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_matviews
                    WHERE matviewname = '{mv_name}'
                );
            """
            result = self.execute_select_query(check_view_query)
            view_exists = result[0][0]

            if not view_exists:
                self.execute_query(query)
                logging.info(f"Materialized view {mv_name} created successfully.")
            else:
                refresh_view_query = f"REFRESH MATERIALIZED VIEW {SCHEMA_GOLD}.{mv_name};"
                self.execute_query(refresh_view_query)
                logging.info(f"Materialized view {mv_name} refreshed successfully.")

        except Exception as e:
            logging.error(f"Error creating or refreshing materialized view: {str(e)}")
            raise AirflowFailException(f"Failed to create or refresh materialized view: {str(e)}")
