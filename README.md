# PacBikes Data Pipeline Project

## Overview
This project is designed to orchestrate and manage an end-to-end data pipeline for PacBikes. The project involves extracting, transforming, and loading data using Apache Airflow, DBT (Data Build Tool), and supporting components such as S3, Docker, and a data warehouse.

The pipeline is structured to move data from raw data sources to a clean and structured format within a data warehouse, while ensuring data integrity and providing monitoring and alerting capabilities.

---

## Project Structure

```
.
├── pipeline
│   └── dags
├── setup
│    ├── sources
│    ├── airflow
│    ├── airflow-monitoring
│    ├── data-lake
│    ├── warehouse
```

---

## Key Components

### 1. **Airflow DAGs**
Located under `pipeline/dags/`, the Airflow DAGs orchestrate the entire data pipeline.

- **pacbikes_staging:** 
  - Handles initial or incrementally data ingestion and staging transformations
  - Converting raw data into a usable form for downstream processing.
  - Trigger **pacbikes_warehouse** dags.
- **pacbikes_warehouse:** 
  - Performs complex transformations and data enrichment using DBT models.
  - Eventually loading data into warehouse tables.

### 2. **Tasks (Staging)**

Found under `pipeline/dags/pacbikes_staging/tasks/`:

- **extract.py:** Extracts data from external sources, such as APIs, databases, or flat files. The extraction process includes connection handling and data retrieval.
- **load.py:** Loads extracted and transformed data into the staging area within the data warehouse.
- **main.py:** Orchestrates the task flow by defining dependencies and logic for running individual tasks within the staging process.
- **run.py:** Defines and triggers the DAG for the stagings stage.

### 3. **Tasks (Warehouse)**

Located under `pipeline/dags/pacbikes_warehouse/`:

- **run.py:** Defines and triggers the DAG for the warehouse stage. This DAG integrates with DBT to run models and materialize tables and views in the data warehouse.

### 4. **DBT Project**

Located under `pipeline/dags/pacbikes_warehouse/pacbikes_warehouse_dbt/`:

- **models:** Contains SQL models defining tables and views within the data warehouse.
  - **final:** Holds final models like dimension and fact tables for reporting and analytics.
- **snapshots:** Captures historical changes to source tables and tracks changes over time.
- **seeds:** Contains static reference datasets, such as date dimensions (`dim_date.csv`).
- **packages.yml:** Manages external DBT dependencies.
- **dbt_project.yml:** Defines DBT project settings and configurations.

### 5. **Helper Functions**

Located under `pipeline/dags/helper/`:

- **s3.py:** Handles reading and writing files to Amazon S3 storage.
- **slack_notifier.py:** Sends task success or failure notifications to a designated Slack channel.
- **callbacks:** Directory containing utility functions for DAG task callbacks.

### 6. **Setup**

The `setup/` directory contains configuration files and Docker setups for deploying the infrastructure.

- **airflow:** Configures Airflow services, including webserver, scheduler, and workers using Docker Compose.
  - **Dockerfile:** Customizes the Airflow Docker image.
  - **requirements.txt:** Python dependencies for Airflow tasks.
- **sources:** Initializes data sources, including sample data scripts (`init.sql`).
- **data-lake:** Sets up a scalable data lake for raw and semi-processed data storage.
- **warehouse:** Defines a Postgres or equivalent data warehouse.
- **airflow-monitoring:** Configures Prometheus and Grafana to monitor Airflow performance metrics.

---

## Deployment Instructions

### 1. **Clone the Repository**
Clone the project and navigate to the project directory:
```bash
git clone <repository-url>
cd <project-folder>
```

### 2. **Configure Environments and Start all Services**
Execute the setup script to prepare environment configurations and dependencies:
```bash
./setup.sh
```

---

## Monitoring and Alerts

The project integrates monitoring to ensure data pipeline performance:

- **Prometheus:** Monitors Airflow system metrics, including task execution times and resource usage.
- **Grafana:** Visualizes metrics with pre-configured dashboards (`Airflow-Dashboards.json`).
- **Slack Notifications:** Sends alerts on task failures, retries, and overall pipeline health.


## Configuration Files

### Airflow Variables and Connections
Located in `setup/airflow/variables_and_connections/`:

- **airflow_connections_init.yaml:** Predefined connections to databases, cloud services, and APIs.
- **airflow_variables_init.json:** Defines environment-specific variables used within DAGs.

---

## Key Features

- **Modular Pipeline:** Clean separation of staging, transformations, and final load processes.
- **Integrated Monitoring:** Provides real-time insights into pipeline performance and failures.
- **Scalable Architecture:** Built using containerized services for distributed processing.

---

## Project Flow and Explanation 

