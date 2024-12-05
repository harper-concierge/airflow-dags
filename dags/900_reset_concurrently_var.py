from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from plugins.utils.send_harper_slack_notification import send_harper_failure_notification

rebuild = Variable.get("REBUILD_MONGO_DATA", "False").lower() in ["true", "1", "yes"]


def reset_concurrently_var():
    Variable.set("CONCURRENTLY", "True")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 7, 14),
    "schedule_interval": "@daily",
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=5),
    "retries": 0,
    "on_failure_callback": [send_harper_failure_notification()],
}


dag = DAG(
    "900_reset_concurrently_var",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,  # This ensures sequential execution
    template_searchpath="/usr/local/airflow/dags",
)

wait_for_slack = ExternalTaskSensor(
    task_id="wait_for_send_slack_messages",
    external_dag_id="70_send_slack_messages",  # The ID of the DAG you're waiting for
    external_task_id=None,  # Set to None to wait for the entire DAG to complete
    allowed_states=["success"],  # You might need to customize this part
    dag=dag,
)


reset_concurrently_var_task = PythonOperator(
    task_id="reset_concurrently_var_task",
    depends_on_past=False,
    python_callable=reset_concurrently_var,
    dag=dag,
)

wait_for_slack >> reset_concurrently_var_task
