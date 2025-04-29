# dag_full_monthly_etl.py
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# ----------------- 0. Configuración del DAG -----------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="full_monthly_etl",
    description="Orquestación completa: Raw to Bronze to Silver",
    default_args=default_args,
    schedule_interval="@monthly",  # Corre una vez al mes
    start_date=days_ago(1),
    catchup=False,
    tags=["tapi", "etl", "silver"],
)

# ----------------- 1. Variables -----------------
AWS_CONN_ID = "aws_default"
REGION_NAME = "us-east-1"  # Asegurate que sea la tuya
DATABASE_ATHENA = "dbtapi"
OUTPUT_S3_ATHENA = "s3://tapi-challenge/athena-results/"

# ----------------- 2. Tareas Glue (Jobs existentes) -----------------

# Bronze: payments
bronze_pp_job = GlueJobOperator(
    task_id="bronze_pp_job",
    job_name="bronze_pp_job",  # nombre exacto del Glue Job
    aws_conn_id=AWS_CONN_ID,
    region_name=REGION_NAME,
    dag=dag,
)

# Bronze: providers_commission y clients_revenue_share
bronze_dims_job = GlueJobOperator(
    task_id="bronze_dims_job",
    job_name="bronze_dims_job",  # nombre exacto del Glue Job
    aws_conn_id=AWS_CONN_ID,
    region_name=REGION_NAME,
    dag=dag,
)

# Silver: transform
silver_transform_job = GlueJobOperator(
    task_id="silver_transform_job",
    job_name="silver_transform_job",  # nombre exacto del Glue Job
    aws_conn_id=AWS_CONN_ID,
    region_name=REGION_NAME,
    dag=dag,
)

# ----------------- 3. Reparar particiones en Athena -----------------

repair_fact_payments = AthenaOperator(
    task_id="repair_fact_payments",
    query="MSCK REPAIR TABLE silver.fact_payments",
    database=DATABASE_ATHENA,
    output_location=OUTPUT_S3_ATHENA,
    aws_conn_id=AWS_CONN_ID,
    dag=dag,
)

repair_dim_client = AthenaOperator(
    task_id="repair_dim_client",
    query="MSCK REPAIR TABLE silver.dim_client",
    database=DATABASE_ATHENA,
    output_location=OUTPUT_S3_ATHENA,
    aws_conn_id=AWS_CONN_ID,
    dag=dag,
)

repair_dim_provider = AthenaOperator(
    task_id="repair_dim_provider",
    query="MSCK REPAIR TABLE silver.dim_provider",
    database=DATABASE_ATHENA,
    output_location=OUTPUT_S3_ATHENA,
    aws_conn_id=AWS_CONN_ID,
    dag=dag,
)

# ----------------- 4. Definición de dependencias -----------------

# Primero los jobs de bronze
[bronze_pp_job, bronze_dims_job] >> silver_transform_job

# Después de transformar a silver
silver_transform_job >> [repair_fact_payments, repair_dim_client, repair_dim_provider]
