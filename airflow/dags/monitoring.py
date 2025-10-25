# IMPORT
import pandas as pd
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from evidently.ui.workspace import CloudWorkspace
from evidently import Report
from evidently.metrics import *
from evidently.presets import *
from datetime import datetime
from mail_utility import SimpleMailSender

def _detect_data_drift(**context):
    # Connect to PostgreSQL and get data
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = postgres_hook.get_sqlalchemy_engine()
    reference_data = pd.read_sql("SELECT * FROM {} WHERE id <= 500".format(Variable.get("TABLE_HOUSING_PRICES")), engine, index_col="id")
    current_data = pd.read_sql("SELECT * FROM {}".format(Variable.get("TABLE_HOUSING_PRICES")), engine, index_col="id")

    # Connect to Evidently Cloud
    ws = CloudWorkspace(
    token=Variable.get("EVIDENTLY_TOKEN"),
    url="https://app.evidently.cloud")

    # Retrieve project
    project = ws.get_project(Variable.get("EVIDENTLY_PROJECT_ID"))

    # Define report
    data_drift_report = Report(
        metrics=[
            DataDriftPreset()
        ],
        include_tests=True
    )

    # Create report
    report = data_drift_report.run(current_data=current_data, reference_data=reference_data)
    dashboard = ws.add_run(project.id, report)

    report_dict = report.dict()
    dashboard_url = dashboard.dict()["url"]

    # Check if there's data drift
    if report_dict["tests"][0]["status"]=="FAIL":
        context["task_instance"].xcom_push(key="dashboard_url", value=dashboard_url)

        return "data_drift_detected"
    else:
        return "no_data_drift_detected"
    

def _send_notification(**context):
    # Filename from the context
    dashboard_url = context["task_instance"].xcom_pull(key="dashboard_url")

    body = f"Un drift de données a été détecté, un ré-entrainement du modèle a été déclenché.\n\nPour plus de détails, consulter le dashboard : {dashboard_url}"  

    SimpleMailSender.send_email(Variable.get("SENDER_EMAIL"), Variable.get("RECEIVER_EMAIL"), "Détection d'un drift de données !", body)


with DAG(dag_id="monitoring", start_date=datetime(2025, 8, 28), schedule_interval=None, catchup=False) as dag:
    start = DummyOperator(task_id="start")

    detect_data_drift = BranchPythonOperator(
        task_id='detect_data_drift',
        python_callable=_detect_data_drift,
        provide_context=True
    )

    data_drift_detected = DummyOperator(task_id="data_drift_detected")

    no_data_drift_detected = DummyOperator(task_id="no_data_drift_detected")

    send_notification = PythonOperator(
        task_id="send_notification",
        python_callable=_send_notification
    )

    trigger_training_ec2 = TriggerDagRunOperator(
        task_id='trigger_training_ec2',
        trigger_dag_id='training_ec2'
    )

    end = DummyOperator(task_id="end", trigger_rule="none_failed")

    start >> detect_data_drift >> [data_drift_detected, no_data_drift_detected]
    data_drift_detected >> [send_notification, trigger_training_ec2] >> end
    no_data_drift_detected  >> end