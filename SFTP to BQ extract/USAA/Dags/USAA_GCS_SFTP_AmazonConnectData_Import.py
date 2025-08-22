
# Import libraries -
import sys, os, json, time
from datetime import datetime
from airflow.utils.state import State
from airflow.models import DAG              #library to use DAG code , main class for geenerating DAG graph and execution flow.
from airflow.utils.dates import days_ago    #Library used in dag schedule definiton
from airflow.models import Variable         #Library to import Airflow variables defined at Airflow UI
from airflow.operators.dummy_operator import DummyOperator    #Library to import dummy operator to create dummy tasks for execution -> Start and End Dag Tasks uses this library
from airflow.operators.python import PythonOperator #Library to import Python Operatot to call Python functions
from airflow.operators.bash_operator import BashOperator 
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator  #Library used to load data from GCS to BigQuery
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.contrib.operators.gcs_to_gcs import GoogleCloudStorageToGoogleCloudStorageOperator
from bigquery_schema_generator.generate_schema import SchemaGenerator ## Library used to generate_schema 
from google.cloud.dataform_v1beta1 import WorkflowInvocation
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCancelWorkflowInvocationOperator,
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformGetCompilationResultOperator,
    DataformGetWorkflowInvocationOperator
    )
#logger library
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.exceptions import AirflowSkipException
#google cloud secretmanager
from google.cloud import secretmanager
# Imports the Google Cloud client library
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import re
from functools import partial

file_path=os.path.abspath(__file__)
path_elements=file_path.split(os.sep)
Environment =path_elements[5]
Dag_Name =path_elements[-1].replace('.py','')

if Environment in ["Dev", "Qa"]:
	dag_id = Environment + "_" + Dag_Name
else:
	dag_id = Dag_Name  

# Variable to store current date utc
Current_date_utc = datetime.utcnow().strftime('%Y%m%d')
# ******* Set up Environments here like Dev, QA and Production and Data Connections
if Environment == 'Dev':
    GCP_CONN_ID = "Dev_CltUSAA_BQ_Connection"
    SFTP_CONN_ID = "SFTP_CONN_USAA"
    
elif Environment == 'Qa':
    GCP_CONN_ID = "Qa_CltUSAA_BQ_Connection"
    SFTP_CONN_ID = "SFTP_CONN_USAA"
    
else:
    GCP_CONN_ID = "CltUSAA_BQ_Connection"
    SFTP_CONN_ID = "SFTP_CONN_USAA"

# ********* Initialize DAG Variables *********
dag_var_path = "/home/airflow/gcs/dags/{}/USAA/Configurations/{}/Variables/".format(Environment,Dag_Name)
dag_var = dag_var_path + "dag_variables.json"

# ******** Initialize DataForm Configurations *********
common_var_path = "/home/airflow/gcs/dags/{}/USAA/Configurations/Common/".format(Environment)
common_var = common_var_path + "common_variables.json"
        

# Import the read_json_file module from utility folder 
utility_folder_path="/home/airflow/gcs/dags/{}/CommonPattern".format(Environment)
sys.path.append(utility_folder_path)
import Read_config_file 
import Run_Audit_module
import sftp_to_gcs_module

# returns configs from json files in map variable format based on the  environment settings.
common_variables = Read_config_file.read_json(common_var)
dag_variables = Read_config_file.read_json(dag_var)

# Define your custom callback functions

from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from datetime import datetime

# Define your custom callback functions
def on_failure_callback(context,parent_system_id, mask_task_id):
    try:
        LoggingMixin().log.info("DAG Failed! Updating RunAudit Table...")
        ti = context['task_instance']
        failed_dag_id= ti.dag_id
        failed_tasks = [] 
        for task_instance in context['dag_run'].get_task_instances():
            if task_instance.state == State.FAILED:
                failed_tasks.append(str(task_instance.task_id)) 
        failed_tasks_list = ', '.join(failed_tasks)
        if failed_tasks:  
            inactive_reason = f"Dag Failure - Tasks: {failed_tasks_list} ended with errors in the dag {failed_dag_id}"
        else:
            inactive_reason = f"Dag {failed_dag_id} Generic Failure"
        runauditid = ti.xcom_pull(task_ids=f'GetRunaudit_{parent_system_id}_{mask_task_id}')
        if runauditid is not None:
            Run_Audit_module.execute_sp_UpdateRunAuditInfo(arg1={'RunAuditId': runauditid, 'InactiveReason': inactive_reason})
    except Exception as e:
        LoggingMixin().log.error(f"An error occurred in on_failure_callback: {str(e)}")

def on_success_callback(context, parent_system_id, mask_task_id):
    try:
        LoggingMixin().log.info("DAG Succeeded! Updating RunAudit Table...")
        ti = context['task_instance']
        inactive_reason = f"DAG {ti.dag_id} completed successfully..."
        runauditid = ti.xcom_pull(task_ids=f'GetRunaudit_{parent_system_id}_{mask_task_id}')
        if runauditid is not None:
            Run_Audit_module.execute_sp_UpdateRunAuditInfo(arg1={'RunAuditId': runauditid, 'LastCompletedDate': datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'InactiveReason': inactive_reason})
    except Exception as e:
        LoggingMixin().log.error(f"An error occurred in on_success_callback: {str(e)}")


#  ******* DAG Code Starts here *********
with DAG(
    dag_id=dag_id,
    template_searchpath=['/home/airflow/gcs/dags/'],
    start_date=days_ago(1),
    default_args={
        'owner': 'airflow',
        'retries': 0,
    }, 
    schedule_interval=f"{dag_variables['ScheduleInterval']}",max_active_runs=1, catchup=False, 
    tags=[path_elements[6], path_elements[7], path_elements[-3], Environment], 
    on_failure_callback=partial(on_failure_callback),
    on_success_callback=on_success_callback,


) as dag:

    # Start task - Dummy task
    Start = DummyOperator(task_id='Start')

    # End task - Dummy task
    End = DummyOperator(
        task_id='End',
)

for System in dag_variables['Systems']:
    for file_mask in System['FileMasks']:
        mask_task_id = file_mask.replace('*', '').replace('.', '').replace(' ', '').replace('&','').replace('xlsx','')
        parent_system_id = System['ParentSystemId']
        sftp_remote_path = f"{dag_variables['FtpSourcePath']}/{file_mask}"
        input_path = f"{System['ParentSystemId']}/{System['SystemId']}/Archive/{System[mask_task_id]['Input_Folder']}"

    Task_SFTP_to_GCS = PythonOperator(
        task_id=f'SFTP_To_GCS_{parent_system_id}_{mask_task_id}',
        python_callable=sftp_to_gcs_module.sftp_to_gcs,
        on_failure_callback=partial(on_failure_callback,
                    parent_system_id=parent_system_id,
                    mask_task_id=mask_task_id),
        op_kwargs={
            'arg1': {
                'sftp_conn_id': SFTP_CONN_ID,
                'source_path': sftp_remote_path,
                'gcp_conn_id': GCP_CONN_ID,
                'destination_bucket': dag_variables['GCSArchiveBucket'],
                'destination_path': input_path,
                'move_object': True,
                'mime_type': "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            }
        },
        provide_context=True,
        dag=dag,

    )

Start >> Task_SFTP_to_GCS >> End
