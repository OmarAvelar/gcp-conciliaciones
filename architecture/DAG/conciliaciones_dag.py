import datetime
from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'composer',
    'start_date': datetime.datetime(2023, 5, 30),
    'email': ['your-email@example.com'],
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

dag = DAG(
    'create_and_destroy_cluster',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
)

project_id = 'conciliaciones-388322'
region = 'us-central1'
cluster_name = 'conciliaciones-cluster'
bash_script_location = 'bash-script-location'

create_cluster = DataprocClusterCreateOperator(
    task_id='create_cluster',
    project_id=project_id,
    cluster_name=cluster_name,
    region=region,
    num_workers=2,
    master_machine_type='n1-standard-2',
    worker_machine_type='n1-standard-2',
    dag=dag,
)

run_script = BashOperator(
    task_id='run_script',
    bash_command=f'gcloud compute ssh {cluster_name}-m --region={region} --command="bash {bash_script_location}"',
    dag=dag,
)

delete_cluster = DataprocClusterDeleteOperator(
    task_id='delete_cluster',
    project_id=project_id,
    cluster_name=cluster_name,
    region=region,
    trigger_rule='all_done',
    dag=dag,
)

create_cluster >> run_script >> delete_cluster

