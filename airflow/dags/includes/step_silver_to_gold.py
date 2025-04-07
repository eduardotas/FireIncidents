from airflow.exceptions import AirflowFailException
from sqlalchemy import create_engine, text
import logging
from includes.constants import  SCHEMA_SILVER, TEMP_TABLE, MAIN_TABLE, SCHEMA_GOLD, MV_DAY_NAME, MV_MONTH_NAME
from includes.data_quality import DataQuality
from includes.database import DBPostgres
from datetime import datetime, timedelta

log = logging.getLogger(__name__)

def silver_to_gold():    
    db = DBPostgres()    

    try:
        log.info(f"Starting gold update {MV_DAY_NAME}...")        
        query = f"""     
            CREATE MATERIALIZED VIEW {SCHEMA_GOLD}.{MV_DAY_NAME} AS       
                SELECT
                    TO_CHAR(DATE_TRUNC('day', incident_date), 'YYYY-MM-DD') AS period,
                    supervisor_district,
                    battalion,
                    count(DISTINCT incident_number) AS incident_count,    
                    sum(number_of_alarms) AS total_alarms    ,
                    sum(suppression_units ) as total_suppression_units,
                    sum(suppression_personnel) as total_suppression_personnel,
                    sum(ems_units) as total_ems_units,
                    sum(ems_personnel) as total_ems_personnel,
                    sum(other_units) as total_other_units,
                    sum(other_personnel) as total_other_personnel,
                    sum(fire_fatalities) AS total_fire_fatalities,
                    sum(fire_injuries) AS total_fire_injuries,
                    sum(civilian_fatalities) AS total_civilian_fatalities,
                    sum(civilian_injuries) AS total_civilian_injuries,
                    sum(number_of_alarms) AS total_number_of_alarms        
                FROM {SCHEMA_SILVER}.{MAIN_TABLE}
                GROUP BY period, supervisor_district, battalion
                ORDER BY period, supervisor_district, battalion;
        """
        db.create_or_refresh_mv(query,MV_DAY_NAME)
        log.info("Update finished!")
    except Exception as e:
        raise AirflowFailException(f"Failed to update MV {MV_DAY_NAME}: {str(e)}")   
    
    try:
        log.info(F"Starting gold update {MV_MONTH_NAME}...")        
        query = f"""     
            CREATE MATERIALIZED VIEW {SCHEMA_GOLD}.{MV_MONTH_NAME} AS       
                SELECT
                    TO_CHAR(DATE_TRUNC('month', incident_date), 'YYYY-MM-DD') AS period,
                    supervisor_district,
                    battalion,
                    count(DISTINCT incident_number) AS incident_count,    
                    sum(number_of_alarms) AS total_alarms    ,
                    sum(suppression_units ) as total_suppression_units,
                    sum(suppression_personnel) as total_suppression_personnel,
                    sum(ems_units) as total_ems_units,
                    sum(ems_personnel) as total_ems_personnel,
                    sum(other_units) as total_other_units,
                    sum(other_personnel) as total_other_personnel,
                    sum(fire_fatalities) AS total_fire_fatalities,
                    sum(fire_injuries) AS total_fire_injuries,
                    sum(civilian_fatalities) AS total_civilian_fatalities,
                    sum(civilian_injuries) AS total_civilian_injuries,
                    sum(number_of_alarms) AS total_number_of_alarms        
                FROM {SCHEMA_SILVER}.{MAIN_TABLE}
                GROUP BY period, supervisor_district, battalion
                ORDER BY period, supervisor_district, battalion;
        """
        db.create_or_refresh_mv(query,MV_MONTH_NAME)
        log.info("Update finished!")
    except Exception as e:
        raise AirflowFailException(f"Failed to update MV {MV_MONTH_NAME}: {str(e)}")