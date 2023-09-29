# Airflow DAGs for Copying MBELT3 Data from PostgreSQL to BigQuery

This repository contains the necessary DAGs and configuration files to set up an Apache Airflow workflow for copying MBELT3 data from PostgreSQL to Google BigQuery. This README will guide you through the setup and configuration process.

## Prerequisites

Before getting started, ensure you have the following prerequisites installed:

- Docker: To run Airflow and PostgreSQL as containers.
- Docker Compose: To define and run multi-container Docker applications.

## Installation

1. Clone this repository to your local machine:

   ```bash
   git clone https://github.com/yourusername/airflow-postgresql-to-bigquery.git
   cd airflow-postgresql-to-bigquery
   ```

## Docker Compose

To start Airflow and PostgreSQL as containers, you need to configure the ```docker-compose.yml``` file. Update the environment variables in the ```webserver``` section as needed.

## Usage

1. Start Airflow and PostgreSQL containers using Docker Compose:

   ```bash
   docker-compose up -d
   ```

2. Access the Airflow web UI by navigating to `http://localhost:8080` in your web browser. Username: `airflow`. Password: `airflow`

3. In the Airflow UI, trigger and enable the DAGs located in the ```dags``` directory.

4. Monitor and manage your data copying workflows in the Airflow UI.

## DAGs

This repository includes the following DAGs:

- `mbelt3_public_bigloader.py`: DAG for copying MBELT3 data from PostgreSQL to BigQuery.

You can customize these DAGs to meet your specific data copying requirements by modifying the Python code in the ```dags``` directory.


## Configuration

### Airflow Variables and Connections

To successfully run the DAGs, you need to configure Airflow Variables and Connections. These settings are stored in Airflow's metadata database and can be managed using the Airflow UI or by defining them in code.

#### Airflow Variables

Create the following Airflow Variables in the Airflow UI

- `airflow_env_params`: JSON with main variables (see below)
- `bigloader_bucket`: Google Cloud Storage (GCS) bucket name
- `sa_bigquery_token`: Service account token for Google BigQuery (path to JSON key file)

Example `airflow_env_params`
```json
{
  "env_mode": "prod",
  "airflow_home_dir": "/opt/airflow",
  "airflow_output": "/opt/airflow/output",
  "gcp_conn_id": "de",
  "dbt_bin": /opt/airflow/.local/bin/dbt,
  "dbt_project_dir": /opt/airflow/p2p,
  "dbt_profiles_dir": /opt/airflow/.dbt,
  "slack_alerting_channel":de_alerting_stage,
  "bq_dataset": "raw_data",
  "bq_project_id": "mbelt3-dev"
}

#### Airflow Connections

Create the following Airflow Connections in the Airflow UI

- `mbelt3_kusama`: PostgreSQL connection for the Kusama MBELT3 database
- `mbelt3_moonbeam`: PostgreSQL connection for the Moonbeam MBELT3 database
- `mbelt3_moonriver`: PostgreSQL connection for the Moonriver MBELT3 database
- `mbelt3_polkadot`: PostgreSQL connection for the Polkadot MBELT3 database


### Additional Notes

- Ensure that your PostgreSQL database and Google BigQuery project are properly configured and accessible from the environment where Airflow is running.

- Make sure you have the necessary permissions to read from PostgreSQL and write to BigQuery.

- For security and best practices, consider using Airflow's built-in support for secrets management to handle sensitive credentials.

- Refer to the Airflow documentation for more information on configuring Airflow and creating DAGs: [Airflow Documentation](https://airflow.apache.org/docs/stable/index.html).

Now you have a fully functional Airflow setup for copying data from PostgreSQL to BigQuery. Customize it to fit your specific data integration needs.




