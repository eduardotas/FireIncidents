### About the Project

This is a data engineering project developed with the goal of demonstrating technical knowledge in the field.  
The pipeline performs an ETL process on the **Fire Incidents** dataset from the city of **San Francisco**, which is publicly available [here](https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric/about_data).

The project uses the following technologies:

- **Docker** – Used to create and manage the project environment.  
- **Python** – Responsible for data extraction from the API and update processes.  
- **PySpark** – Used for data processing and transformation.  
- **Airflow** – Orchestrates the ETL pipeline.  
- **PostgreSQL** – Serves as the data warehouse.

**Access credentials**:

   The access credentials can be found in the **.env** file located in the root directory of the project.

# Data Architecture: Bronze, Silver, and Gold Layers

The **Bronze, Silver, and Gold** layers are used in data architecture to organize and transform information:

- **Br
onze**  🟤 → Raw data, ingested directly from source systems, without processing (ideally stored in S3 or Blob Storage, but structured this way to avoid costs).
- **Silver** ⚪ → Cleaned and transformed data, prepared for deeper analysis.
- **Gold** 🟡 → Highly refined and aggregated data, optimized for BI and reporting.

### Summary
**Bronze (raw) → Silver (processed) → Gold (ready for use).**
## Why This Method?
I chose this method because the **Bronze, Silver, and Gold** layers ensure an organized data flow, making auditing and traceability easier.

# How I did it and how I thought
### Step 1: Setting Up the Environment

In Step 1, I configured the environment using **Docker** and **Airflow**. This involved:

- **Bronze Layer Setup**: Created data directories to serve as the **Bronze** layer. Files are stored in separate daily folders, ensuring historical data is preserved and easily validated if needed.
- **Airflow DAGs Setup**: Under the **airflow/dags/includes** directory, I organized the process into separate scripts by step, making it easy to maintain and follow the data pipeline flow.

### Step 2: API Extraction Methods

In Step 2, I studied the API documentation and found that there are two extraction methods available: **HTTPS** and **Sodapy** (a Python library).

- **Process Creation**: I created two extraction processes to test and decide which method to use—either **HTTPS** or **Sodapy**.
- **Selection of Sodapy**: After testing, I chose **Sodapy** due to its more stable performance.

Additionally, I opted for a **batch processing method**, extracting **50,000 records at a time**, as the documentation [(About the data)](https://data.sfgov.org/Public-Safety/Fire-Incidents/wr8u-xric/about_data) recommended avoiding excessive API consumption.

#### https
![extract_sfgov_data_https - Time](imgs/extract_sfgov_data_https%20-%20Time.png)

#### sodapy
![extract_sfgov_data_https - Time](imgs/extract_sfgov_data_sodapy%20-%20Time.png)

- **The extraction**: The extraction process loads the data into the **bronze layer**, which is organized into folders—**one folder per day**. Inside each folder, JSON files are named as `"data0000.json"`, where `0000` represents the **hour of extraction**.
- **Update Control and Last File Tracking**: To identify when the last update occurred and which was the last file ingested, I created a JSON file named **`latest_status.json`**.  
This file stores both pieces of information, which are read and updated dynamically throughout the process.

### Step 3: Data Transformation and Cleaning

In Step 3, I transformed and cleaned the JSON data using **PySpark**. The main actions performed were:

- **Quality Validation**: Ensured the file wasn't empty and the schema was correct.
- **Handling Duplicates**: Removed duplicate records based on the **"ID"** identifier.
- **Filtering**: The process checks the last update date and filters the data using the `incident_date` column. If no previous update exists, it defaults to retrieving records from the last 5 years and from **San Francisco**.
- **Data Cleaning**: Removed records with a null or empty **`supervisor_district`**.
- **Adjustments and Reordering**: Adjusted the **`point`** column, casted columns, and reordered them according to the standard.
- **Storage**: The transformed data was inserted into a temporary table in the **Silver** layer, **silver.temp_incidents**.

### Step 4: Updating Data in the Silver Layer

In Step 4, I built the process to update the data in the **Silver** layer. The main steps included:

- **Duplicate Check**: Revalidated the data for duplicates in the temporary table, **silver.temp_incidents**.
- **Conflict Handling**: Utilized a temporary table,**silver.temp_incidents**, and a main table,**silver.incidents**, applying the **`on_conflict`** method to handle conflicts and ensure the correct updates.
- **Tracking Changes**: Added the **`updated_at`** column to track when the data was last modified.

### Step 5: Updating Data in the Gold Layer

In Step 5, I developed the process to update the **Gold** layer. This process includes the following steps:

- **Materialized Views Creation**: Created two materialized views, **gold.daily_fire_incident_mv** and **gold.monthly_fire_incident_mv**, grouped by period, district, and battalion.
- **View Management**: The process will either create the materialized views if they do not exist, or update them if they already exist.

## Step 6: Data Update Process

The data update process is set to run **daily** using the DAG **'dag_SFGOV_FULL_PROCESS'**. This DAG encompasses the entire end-to-end process.

In addition, separate DAGs are created for each step of the process. This allows for flexibility, as any part of the process can be run independently if needed, without having to rerun the entire workflow.

