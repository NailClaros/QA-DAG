## QA-DAG

## QA-DAG is a lightweight orchestration framework for quality-assurance workflows using DAG-style pipelines. It includes both Airflow and Streamlit interfaces to visualize and run your processes.

Repository Structure

├── airflow/               # Airflow DAG definitions and logic
├── streamlit/             # Streamlit app for interactive viewing
├── db/                    # Database connection and utility scripts
├── DAG.py                 # Core entrypoint script (Airflow operator or CLI)
├── db.py                  # Database helper and models
├── requirements.txt       # Python dependencies
├── .env.env               # Environment variable template or placeholder
└── .gitignore             # Untracked files exclusion

## About Me!

Hi! I’m Nail Claros, a Computer Science graduate from UNC Charlotte with a passion for backend and data engineering. I specialize in building scalable data pipelines, backend APIs, backend engineering, and serverless solutions. This repository is a hands-on project where I explore:

- Workflow orchestration using DAGs
- Database cleanup and automation
- Integrating Python tools like Airflow, Streamlit, and AWS Lambda

I love creating practical projects that combine data engineering with automation and visualization.

This repo implements an Airflow DAG that orchestrates daily extraction, transformation, and loading (ETL) of air quality (AQ) data from the OpenAQ API into a Postgres database. The design is modular, relying on base scripts to provide reusable building blocks for the DAG tasks.

# QA-DAG – Technical Overview

This repository implements an **Airflow DAG** that orchestrates daily **Extraction, Transformation, and Loading (ETL)** of air quality (AQ) data from the **OpenAQ API** into a Postgres database. The design is **modular**, relying on base scripts to provide reusable building blocks for the DAG tasks.

---

## 1. DAG Overview (`AQ_DAG`)

The DAG is defined using **Airflow SDK decorators**:

- **Schedule:** `@daily` – runs once per day  
- **Start Date:** August 22, 2025  
- **Retries:** 2 attempts with a 5-minute delay  

It has **three main tasks**, representing a classical ETL pipeline:

### **Extract Task**
- Calls the OpenAQ API for each location in `LOC_DICT`.  
- Fetches both **sensor metadata** and **latest sensor values**.  
- Filters only relevant sensors from `UNIT_LIST`.  
- Logs detailed messages for each location and sensor.  
- Returns a list of rows ready for transformation.

### **Transform Task**
- Filters out rows with missing measurements (`None` values).  
- Ensures only valid data is passed to the database.  
- Logs the number of rows retained.

### **Load Task**
- Connects to Postgres via `psycopg2`.  
- Inserts transformed data into `aq_data.daily_measurements`.  
- Uses `ON CONFLICT ... DO NOTHING` to prevent duplicate inserts.  
- Commits the transaction and logs the number of rows inserted.

**Execution Flow:**  
extract() → transform() → load()

---

## 2. Base Logic (`base.py` / `get_daily_data`)

The DAG builds directly on the **base Python scripts**, which encapsulate the **raw ETL logic**.

### **Responsibilities of the Base Script**
- API interaction (`requests` calls to OpenAQ)  
- Filtering data by sensor IDs (`UNIT_LIST`)  
- Mapping sensor data into structured rows: `(location, parameter, value, date)`  
- Inserting into Postgres via `mass_insert_data()`  

### **Why Base Scripts Exist**
- Provides **reusable Python functions** for local testing or standalone execution.  
- Separates **business logic** from Airflow orchestration.  
- Makes it easier to port into **Lambda**, **CLI**, or DAG tasks without duplicating code.

---

## 3. Relationship Between DAG and Base Scripts

| Component           | Role in DAG                      | Base Logic Origin                     |
|--------------------|---------------------------------|--------------------------------------|
| `extract()`         | Task: Fetch and prepare data    | `get_daily_data()` in `base.py`       |
| `transform()`       | Task: Clean/validate data       | Inline filtering from base logic      |
| `load()`            | Task: Insert into database      | `mass_insert_data()` in `db.py`       |
| Logging & errors    | Airflow task logging            | Print statements + exception handling|

**Key Idea:**  
The DAG **wraps the base scripts in task boundaries**, adding:

- Logging via Airflow’s logger  
- Structured retries & error handling  
- Task-level orchestration  

---

## 4. DAG Benefits Over Base Script Alone

- **Scheduling:** Airflow automatically triggers daily.  
- **Retries & Alerts:** Built-in mechanisms for failure recovery.  
- **Monitoring:** Logs for each task, visible in Airflow UI.  
- **Task Isolation:** Extract, Transform, and Load steps are modular.  
- **Extensibility:** New tasks or additional validation steps can be added without changing the base script.

---

## 5. Example Execution Flow
```
Start DAG
|
v
Extract Task: Fetch OpenAQ data → row_data
|
v
Transform Task: Filter invalid rows → transformed_data
|
v
Load Task: Insert into Postgres
|
v
DAG Completed
```

## ⭐ Technical Highlights

- **Automated ETL Pipeline**  
  Designed a fully automated **daily data ingestion pipeline** that extracts, transforms, and loads air quality data from the OpenAQ API into PostgreSQL, ensuring timely and accurate data availability.

- **Modular DAG Architecture**  
  Built a **reusable Airflow DAG (`AQ_DAG`)** with clearly separated tasks for **Extract**, **Transform**, and **Load**, providing a maintainable and extendable workflow that can easily incorporate new locations or sensors.

- **Robust Error Handling & Logging**  
  Implemented **task-level logging, retries, and exception handling**, enabling reliable execution and efficient debugging in case of API failures or data issues.

- **Reusable Base Python Scripts**  
  Developed **modular Python functions** for API requests, sensor filtering, and database insertion, allowing for standalone testing and seamless integration into the DAG while keeping business logic separate from orchestration.

- **Serverless Deployment on AWS Lambda**  
  Successfully deployed the DAG on **AWS Lambda**, achieving **cost-effective, serverless execution** with full ETL functionality, eliminating the need for dedicated Airflow infrastructure.

- **Local Development with Docker**  
  Leveraged **Docker to run Airflow locally**, replicating the production environment for dependency management, testing, and validation prior to cloud deployment.

- **Interactive Dashboards with Streamlit**  
  Created **real-time, interactive dashboards** to visualize daily and historical air quality trends, making insights accessible for monitoring and analysis.

- **Data Validation & Transformation**  
  Ensured **high-quality data** by filtering invalid measurements, handling missing values, and standardizing formats for database insertion, maintaining the integrity of the dataset.

- **PostgreSQL Integration**  
  Leveraged **PostgreSQL features** such as `ON CONFLICT DO NOTHING` to perform safe upserts, ensuring reliable and idempotent database loads.

- **End-to-End Full-Stack Data Engineering**  
  Demonstrates expertise across the full stack: **cloud deployment, ETL orchestration, API integration, data storage, and interactive visualization**, all within a single cohesive project that is production-ready and scalable.

---

## Deployment & Hosting
- **Local Development:** Airflow running in Docker ensures environment parity, dependency management, and workflow testing.  
- **Cloud Execution:** Serverless deployment on **AWS Lambda** for cost-effective scheduled execution without maintaining an Airflow server.  
- **Dashboards:** Streamlit dashboards provide visual access to daily and historical air quality data.

---

This README provides a **comprehensive and personal overview** of QA-DAG, showcasing **technical depth, backend engineering, and data engineering skills**.
