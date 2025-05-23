from airflow.exceptions import AirflowFailException
from includes.constants import  SCHEMA_SILVER, TEMP_TABLE, MAIN_TABLE
from includes.data_quality import DataQuality
from includes.database import DBPostgres
import logging

log = logging.getLogger(__name__)
dq = DataQuality(process_name=__name__)

def temp_silver_to_main_silver():    
    db = DBPostgres()

    dq.check_duplicates_at_table(SCHEMA_SILVER, TEMP_TABLE)

    try:
        log.info("Starting data update...")        
        query = f"""            
            INSERT INTO {SCHEMA_SILVER}.{MAIN_TABLE} AS t
            SELECT * ,
            CURRENT_TIMESTAMP AS updated_at
            FROM {SCHEMA_SILVER}.{TEMP_TABLE} AS tmp
            ON CONFLICT (id)
            DO UPDATE SET
                incident_number = EXCLUDED.incident_number,
                exposure_number = EXCLUDED.exposure_number,
                address = EXCLUDED.address,
                incident_date = EXCLUDED.incident_date,
                call_number = EXCLUDED.call_number,
                alarm_dttm = EXCLUDED.alarm_dttm,
                arrival_dttm = EXCLUDED.arrival_dttm,
                close_dttm = EXCLUDED.close_dttm,
                city = EXCLUDED.city,
                zipcode = EXCLUDED.zipcode,
                battalion = EXCLUDED.battalion,
                station_area = EXCLUDED.station_area,
                suppression_units = EXCLUDED.suppression_units,
                suppression_personnel = EXCLUDED.suppression_personnel,
                ems_units = EXCLUDED.ems_units,
                ems_personnel = EXCLUDED.ems_personnel,
                other_units = EXCLUDED.other_units,
                other_personnel = EXCLUDED.other_personnel,
                first_unit_on_scene = EXCLUDED.first_unit_on_scene,
                fire_fatalities = EXCLUDED.fire_fatalities,
                fire_injuries = EXCLUDED.fire_injuries,
                civilian_fatalities = EXCLUDED.civilian_fatalities,
                civilian_injuries = EXCLUDED.civilian_injuries,
                number_of_alarms = EXCLUDED.number_of_alarms,
                primary_situation = EXCLUDED.primary_situation,
                mutual_aid = EXCLUDED.mutual_aid,
                action_taken_primary = EXCLUDED.action_taken_primary,
                property_use = EXCLUDED.property_use,
                supervisor_district = EXCLUDED.supervisor_district,
                neighborhood_district = EXCLUDED.neighborhood_district,
                point = EXCLUDED.point,
                data_as_of = EXCLUDED.data_as_of,
                data_loaded_at = EXCLUDED.data_loaded_at,
                updated_at = CURRENT_TIMESTAMP;
        """
        db.execute_query(query)
        log.info("Update finished!")
    except Exception as e:
        raise AirflowFailException(f"Failed to update data in table {MAIN_TABLE}: {str(e)}")   