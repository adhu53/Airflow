from airflow.sdk import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.standard.operators.python import PythonOperator
import base64


def export_parse(**context):
    ti=context['ti']
    raw=ti.xcom_pull(task_ids="export_list")
    #decode the value using base64  since it is stored in serialized format
    decoded = base64.b64decode(raw).decode("utf-8")
    print(decoded)
    # decoded is a single string with newline seperated. use splitlines to covert to list ['export1.export', 'export2.export', 'export3.export']
    files = decoded.splitlines()
    print(files)
    return files

with DAG(
    dag_id="Export_jobs",
    start_date=datetime(2026,6,25),
    schedule=CronDataIntervalTimetable(cron="30 0 * * *", timezone="utc"),
    catchup=False
) as dag:
    #ssh operator. get the export files name and peform xcom_push
    export_list=SSHOperator(task_id="export_list",
                            ssh_conn_id="remote_server",
                            command="cd /home/adarshmd28/exports/config/daily/ && ls *.export",
                            do_xcom_push=True)

    #Parse_export file
    Parse_export=PythonOperator(task_id="Parse_export",
                                python_callable=export_parse)
    #
    process_exports=SSHOperator.partial(task_id="process_exports",
                                        ssh_conn_id="remote_server",
                                        pool="export_pool",
                                        ).expand(command=Parse_export.output.map(
                                            lambda file: f"python3 /home/adarshmd28/exports/bin/export.py {file}"))
    export_list >> Parse_export>> process_exports
