import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import bigquery

from datetime import timedelta

# Set your Google Cloud project ID, dataset details, and GCS bucket
PROJECT_ID = 'practical-gcp-sandbox-1'
DATASET_ID = 'sample_us'
TEMP_TABLE_ID = 'temp_wikipedia_daily_contributor_edits'
FINAL_TABLE_ID = 'wikipedia_daily_contributor_edits'
GCS_BUCKET = 'practical-gcp-sandbox-us-temp'
GCS_OBJECT = 'wikipedia_aggregated_data_{}.csv'

# Source table details
SOURCE_PROJECT = 'bigquery-public-data'
SOURCE_DATASET = 'samples'
SOURCE_TABLE = 'wikipedia'


def extract_to_temp_table(**context):
    execution_date = context['execution_date']
    client = bigquery.Client(project=PROJECT_ID)

    # Construct the query
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{TEMP_TABLE_ID}` AS
    SELECT
      DATE(TIMESTAMP_SECONDS(timestamp)) AS date,
      contributor_id,
      COUNT(*) AS edit_count
    FROM
      `{SOURCE_PROJECT}.{SOURCE_DATASET}.{SOURCE_TABLE}`
    WHERE
      contributor_id IS NOT NULL
    GROUP BY
      date,
      contributor_id
    """

    # Run the query
    query_job = client.query(query)
    query_job.result()  # Wait for the job to complete

    print(f"Data extracted to temporary table: {PROJECT_ID}.{DATASET_ID}.{TEMP_TABLE_ID}")


def export_to_gcs(**context):
    execution_date = context['execution_date']
    client = bigquery.Client(project=PROJECT_ID)

    destination_uri = f"gs://{GCS_BUCKET}/{GCS_OBJECT.format(execution_date.strftime('%Y%m%d'))}"
    dataset_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID)
    table_ref = dataset_ref.table(TEMP_TABLE_ID)

    job_config = bigquery.ExtractJobConfig()
    job_config.print_header = True
    job_config.destination_format = bigquery.DestinationFormat.CSV

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config
    )
    extract_job.result()  # Wait for the job to complete

    print(f"Data exported to: {destination_uri}")


# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'wikipedia_daily_aggregation',
        default_args=default_args,
        description='Aggregate Wikipedia pageviews data daily by contributor',
        schedule_interval='@daily',
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False,
) as dag:
    create_final_table_if_not_exists = BigQueryCreateEmptyTableOperator(
        task_id='create_final_table_if_not_exists',
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=FINAL_TABLE_ID,
        schema_fields=[
            {'name': 'date', 'type': 'DATE', 'mode': 'REQUIRED'},
            {'name': 'contributor_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'edit_count', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        ],
        time_partitioning={'type': 'DAY', 'field': 'date'},
        cluster_fields=['contributor_id'],
    )

    extract_to_temp_table = PythonOperator(
        task_id='extract_to_temp_table',
        python_callable=extract_to_temp_table,
        provide_context=True,
    )

    export_to_gcs = PythonOperator(
        task_id='export_to_gcs',
        python_callable=export_to_gcs,
        provide_context=True,
    )

    load_to_final_table = GCSToBigQueryOperator(
        task_id='load_to_final_table',
        bucket=GCS_BUCKET,
        source_objects=[GCS_OBJECT.format('*')],
        destination_project_dataset_table=f'{PROJECT_ID}.{DATASET_ID}.{FINAL_TABLE_ID}',
        schema_fields=[
            {'name': 'date', 'type': 'DATE', 'mode': 'REQUIRED'},
            {'name': 'contributor_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'edit_count', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        ],
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
        source_format='CSV',
        skip_leading_rows=1,
    )

    create_final_table_if_not_exists >> extract_to_temp_table >> export_to_gcs >> load_to_final_table
