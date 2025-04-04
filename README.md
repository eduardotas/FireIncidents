# Data Architecture: Bronze, Silver, and Gold Layers

## Explanation
The **Bronze, Silver, and Gold** layers are used in data architecture to organize and transform information:

- **Bronze** 🟤 → Raw data, ingested directly from source systems, without processing.
- **Silver** ⚪ → Cleaned and transformed data, prepared for deeper analysis.
- **Gold** 🟡 → Highly refined and aggregated data, optimized for BI and reporting.

### Summary
**Bronze (raw) → Silver (processed) → Gold (ready for use).**
## Why This Method?
I chose this method because the **Bronze, Silver, and Gold** layers ensure an organized data flow, making auditing and traceability easier.

# How I did it and how I thought
1. I set up the environment with Docker and Airflow.  
2. I studied the API documentation and found that there are two extraction methods: HTTPS and Soday (Python library).  
3. I created two data extraction processes, one with each method, to determine which was better.  

3.1. I chose extract_sfgov_data_sodapy because it has more stable execution times.

#### 3.2.extract_sfgov_data_https
![extract_sfgov_data_https - Time](imgs/extract_sfgov_data_https%20-%20Time.png)

#### 3.3. extract_sfgov_data_sodapy
![extract_sfgov_data_https - Time](imgs/extract_sfgov_data_sodapy%20-%20Time.png)