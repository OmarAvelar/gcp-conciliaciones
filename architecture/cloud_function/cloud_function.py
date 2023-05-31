from google.cloud import composer_v1beta1
from google.protobuf.timestamp_pb2 import Timestamp

def trigger_composer_dag(project_id, location, composer_environment, dag_name):
    client = composer_v1beta1.EnvironmentsClient()

    # Get the Composer environment endpoint
    environment_path = client.environment_path(project_id, location, composer_environment)

    # Create a DAG trigger request
    trigger_request = {
        'dag_run_id': 'trigger-{}'.format(int(Timestamp().GetCurrentTime())),
        'execution_date': {
            'seconds': Timestamp().GetCurrentTime(),
        }
    }

    # Trigger the DAG
    response = client.create_dag_run(parent=environment_path, dag_run=trigger_request, dag_id=dag_name)

    print('DAG {} triggered successfully with run_id: {}'.format(dag_name, response.name))

# Example usage
project_id = 'conciliaciones-388322'
location = 'us-central1'
composer_environment = 'composer-environment'
dag_name = 'dag-conciliaciones'

trigger_composer_dag(project_id, location, composer_environment, dag_name)

