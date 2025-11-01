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
import logging
from typing import Any, Dict, Optional

def _detect_data_drift(**context: Dict[str, Any]) -> str:
    """
    Detect data drift between reference and current housing prices stored in Postgres,
    create an Evidently report and dashboard run, and branch depending on the test result.

    Returns:
        "data_drift_detected" if a drift test failed, otherwise "no_data_drift_detected".

    Raises:
        Exception: re-raises unexpected exceptions after logging and pushing error info to XCom.
    """
    logger = logging.getLogger("airflow.task")
    ti = context.get("task_instance") or context.get("ti")

    try:
        # Connect to PostgreSQL and get data
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        engine = postgres_hook.get_sqlalchemy_engine()
        table_name = Variable.get("TABLE_HOUSING_PRICES")
        reference_query = f"SELECT * FROM {table_name} WHERE id <= 500"
        current_query = f"SELECT * FROM {table_name}"

        reference_data: pd.DataFrame = pd.read_sql(reference_query, engine, index_col="id")
        current_data: pd.DataFrame = pd.read_sql(current_query, engine, index_col="id")

        # Connect to Evidently Cloud
        ws = CloudWorkspace(token=Variable.get("EVIDENTLY_TOKEN"), url="https://app.evidently.cloud")

        # Retrieve project
        project = ws.get_project(Variable.get("EVIDENTLY_PROJECT_ID"))

        # Define report
        data_drift_report = Report(metrics=[DataDriftPreset()], include_tests=True)

        # Create report
        report = data_drift_report.run(current_data=current_data, reference_data=reference_data)
        dashboard = ws.add_run(project.id, report)

        report_dict = report.dict()
        dashboard_url: Optional[str] = dashboard.dict().get("url")

        # Defensive check for test results
        tests = report_dict.get("tests", [])
        if not tests:
            logger.warning("Evidently report contains no tests; treating as no drift.")
            return "no_data_drift_detected"

        first_test_status = tests[0].get("status")
        if first_test_status == "FAIL":
            if ti:
                ti.xcom_push(key="dashboard_url", value=dashboard_url)
            logger.info("Data drift detected; dashboard created at: %s", dashboard_url)
            return "data_drift_detected"
        else:
            logger.info("No data drift detected.")
            return "no_data_drift_detected"

    except Exception as exc:
        # Push error info for downstream debugging and re-raise so task fails
        logger.exception("Error while detecting data drift: %s", exc)
        if ti:
            ti.xcom_push(key="monitoring_error", value=str(exc))
        raise


def _send_notification(**context: Dict[str, Any]) -> None:
    """
    Send an email notification when data drift is detected. Expects the dashboard URL
    to be available in XCom under key 'dashboard_url'. Logs and raises on errors.
    """
    logger = logging.getLogger("airflow.task")
    ti = context.get("task_instance") or context.get("ti")

    try:
        dashboard_url: Optional[str] = None
        if ti:
            dashboard_url = ti.xcom_pull(key="dashboard_url")

        if not dashboard_url:
            logger.warning("No dashboard URL found in XCom; sending notification without link.")
            body = "Un drift de données a été détecté, un ré-entrainement du modèle a été déclenché.\n\nAucun dashboard n'est disponible."
        else:
            body = (
                "Un drift de données a été détecté, un ré-entrainement du modèle a été déclenché.\n\n"
                f"Pour plus de détails, consulter le dashboard : {dashboard_url}"
            )

        SimpleMailSender.send_email(
            Variable.get("SENDER_EMAIL"),
            Variable.get("RECEIVER_EMAIL"),
            "Détection d'un drift de données !",
            body,
        )
        logger.info("Notification email sent to %s", Variable.get("RECEIVER_EMAIL"))

    except Exception as exc:
        logger.exception("Failed to send notification email: %s", exc)
        if ti:
            ti.xcom_push(key="notification_error", value=str(exc))
        raise


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
        python_callable=_send_notification,
        provide_context=True
    )

    trigger_training_ec2 = TriggerDagRunOperator(
        task_id='trigger_training_ec2',
        trigger_dag_id='training_ec2'
    )

    end = DummyOperator(task_id="end", trigger_rule="none_failed")

    start >> detect_data_drift >> [data_drift_detected, no_data_drift_detected]
    data_drift_detected >> [send_notification, trigger_training_ec2] >> end
    no_data_drift_detected  >> end