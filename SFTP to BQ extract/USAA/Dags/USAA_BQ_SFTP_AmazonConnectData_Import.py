#*************************************************************************
#Desc: AmazonConnectDataLoad for 3462
#Dependent Objects: Amazon Connect Validate Stored Procedures, Snapshot and Merge EdwOds Dataforms for Amazonconnect 3462
#Created by: Sarah Alhamadah
#Created date: 2024-12-04
#Change History *********************************************************************
#Date of Change – Initials – Ticket # - Description of change
#**************************************************************************


# Import libraries -
import sys, os, json, time
from datetime import datetime, timedelta
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
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
#google cloud secretmanager
from google.cloud import secretmanager
# Imports the Google Cloud client library
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import re
from functools import partial
from airflow.operators.python import PythonOperator


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
# import convert_excel_to_csv
import excel_csv_file_load_generic_module

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

def get_runid_timeframe(parent_system_id, mask_task_id, **context): 
    try:
        ti = context['ti']
        result = ti.xcom_pull(task_ids=f'InsertRunaudit_{parent_system_id}_{mask_task_id}')
        # Assuming result is a list of records
        if result:
            # Assuming the result is a list of records
            runauditid = result[0][0]
            LoggingMixin().log.info("runauditid:{}".format(runauditid))
        return runauditid
    except Exception as e:
        LoggingMixin().log.error(f"An error occurred in get_runid_timeframe: {str(e)}")
        return None



#  ******* DAG Code Starts here *********
with DAG(
    dag_id=dag_id,
    template_searchpath=['/home/airflow/gcs/dags/'],
    start_date=days_ago(1),
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=2)
    }, 
    schedule_interval=f"{dag_variables['ScheduleInterval']}",max_active_runs=1, catchup=False, 
    tags=[path_elements[6], path_elements[7], path_elements[-3], Environment], 
    on_failure_callback=partial(on_failure_callback),
    on_success_callback=on_success_callback,

) as dag:

    # Start task - Dummy task
    Start = DummyOperator(task_id='Start')

    End_Of_File_Load = DummyOperator(
        task_id='End_Of_File_Load',
        trigger_rule='all_done',
    )

    def final_status(**kwargs):
        for task_instance in kwargs['dag_run'].get_task_instances():
            if task_instance.current_state() != State.SUCCESS and task_instance.task_id != kwargs['task_instance'].task_id:
                if task_instance.current_state() != State.SKIPPED:
                    raise Exception("Task {} failed. Failing this DAG run".format(task_instance.task_id))

    #Final task to fail the entire dag incase any one task fails
    End = PythonOperator(
        task_id = 'End',
        provide_context=True,
        python_callable=final_status,
        trigger_rule=TriggerRule.ALL_DONE, # Ensures this task runs even if upstream fails
        retries=0,
        dag=dag,
    )

    schedule_start = datetime.now()   
    formatted_start = schedule_start.strftime("%Y-%m-%d %H:%M:%S")
    
    input_file_format = dag_variables.get('InputFileFormat', 'xlsx')

    Create_Compilation_Result = DataformCreateCompilationResultOperator(
    task_id=f"Create_Compilation_Result",
    project_id=common_variables["DF_Project"],
    region=common_variables["DF_Region"],
    repository_id=common_variables["DF_Repository_Id_Ods"],
    compilation_result={
        "git_commitish": common_variables["DF_GitBranch"],
    },
    gcp_conn_id=GCP_CONN_ID,
    on_failure_callback=partial(on_failure_callback),
    )

    for System in dag_variables['Systems']:
        for file_mask in System['FileMasks']:
            mask_var = file_mask.replace('*', '').replace(f".{input_file_format}", "").replace(' ', '').replace('.txt','TXT').replace('.csv','CSV')
            mask_task_id = file_mask.replace('*', '').replace('.', '').replace(' ', '').replace('&','').replace('xlsx','')
            parent_system_id = System['ParentSystemId']
            iAudit_dict = {
                'EdwTableName': System[mask_var]['EdwTableName'],
                'SystemId': int(System['SystemId']),
                'ParentSystemId': int(System['ParentSystemId']),
                'RunTypeId': int(System['RunTypeId']),
                'SystemTypeId': int(System['SystemTypeId']),
                'username': GCP_CONN_ID,
                'job_name': Dag_Name,
                'ScheduledStartDate': formatted_start
            } 


            Task_InsertRunAudit = PythonOperator(
                task_id=f'InsertRunaudit_{parent_system_id}_{mask_task_id}', 
                python_callable=Run_Audit_module.execute_sp_GetRunAuditInfo,
                op_kwargs={'arg1': iAudit_dict},
                provide_context=True,
                dag=dag,
            ) 

            # Task to get the result of execute_sp_GetRunAuditInfo
            Task_GetRunAudit = PythonOperator(
                task_id=f"GetRunaudit_{parent_system_id}_{mask_task_id}", 
                python_callable=get_runid_timeframe,
                provide_context=True,
                op_kwargs={
                    'parent_system_id': parent_system_id,
                    'mask_task_id': mask_task_id
                },
                dag=dag,
            )

            sftp_remote_path = f"{dag_variables['FtpSourcePath']}/{file_mask}"
            input_path = f"{System['ParentSystemId']}/{System['SystemId']}/{System[mask_var]['Input_Folder']}"
            rejected_path = f"{System['ParentSystemId']}/{System['SystemId']}/Rejected/{System[mask_var]['Input_Folder']}"
            archive_path = f"{System['ParentSystemId']}/{System['SystemId']}/Archive/{System[mask_var]['Input_Folder']}"
            csv_output_path = f"{System['ParentSystemId']}/{System['SystemId']}/CSV/{System[mask_var]['Input_Folder']}"

            Task_SFTP_to_GCS = PythonOperator(
                task_id=f'SFTP_To_GCS_{parent_system_id}_{mask_task_id}',
                python_callable=sftp_to_gcs_module.sftp_to_gcs,
                pool="sftp_amazonconnect_pool",
                on_failure_callback=partial(on_failure_callback,
                            parent_system_id=parent_system_id,
                            mask_task_id=mask_task_id),
                op_kwargs={
                    'arg1': {
                        'sftp_conn_id': SFTP_CONN_ID,
                        'source_path': sftp_remote_path,
                        'gcp_conn_id': GCP_CONN_ID,
                        'destination_bucket': dag_variables['GCSStageBucket'],
                        'destination_path': input_path,
                        'move_object': True,
                        'mime_type': "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    }
                },
                provide_context=True,
                dag=dag,
    
            )

            tables = {}
            stored_procs = {}
            dataform_tag={}
            sheet = System[mask_var]['Sheets']

            if sheet:
                for sheet_name in sheet:
                    tables[System[mask_var][sheet_name]['Sheet']] = System[mask_var][sheet_name]['TableName']
                    stored_procs[System[mask_var][sheet_name]['Sheet']] = System[mask_var][sheet_name]['StoredProcedureCall']
                    dataform_tag[System[mask_var][sheet_name]['Sheet']] = System[mask_var][sheet_name]['DataformTag']
            else:
                tables['Sheet'] = System[mask_var]['TableName']
                stored_procs['StoredProcedure'] = System[mask_var]['StoredProcedureCall']
                dataform_tag['Dataform'] = System[mask_var]['DataformTag']
                

            Task_Execute_SFTP_File_Load_Generic_Module = PythonOperator(
                task_id=f'Execute_SFTP_File_Load_Generic_Module_{parent_system_id}_{mask_task_id}',
                python_callable=excel_csv_file_load_generic_module.file_load_generic_tasks,
                on_failure_callback=partial(on_failure_callback,
                parent_system_id=parent_system_id,
                mask_task_id=mask_task_id),
                provide_context=True,
                op_kwargs={
                    'arg1': {
                        'file_mask': file_mask,
                        'sheets': System[mask_var]['Sheets'],
                        'bucket_name': dag_variables['GCSStageBucket'],
                        'schema_bucket': dag_variables['GCSSchemaBucket'],
                        'archive_bucket': dag_variables['GCSArchiveBucket'],
                        'input_folder': input_path,
                        'output_folder': csv_output_path,
                        'schema_path': csv_output_path,
                        'archive_path': archive_path,
                        'rejected_path': rejected_path,
                        'input_file_format': input_file_format,
                        'dag_id': dag_id,
                        'gcp_conn_id': GCP_CONN_ID,
                        'parent_system_id': int(System['ParentSystemId']),
                        'system_id': int(System['SystemId']),
                        'system_type_id': int(System['SystemTypeId']),
                        'stage_dataset': dag_variables['StageDatasetId'],
                        'project': dag_variables['ProjectId'],
                        'stage_tables_with_sheets': tables,
                        'validation_required': 'Y',
                        'stored_procedures_with_sheets': stored_procs,
                        'source_file_archival': 'Y',
                        'send_email_notification': 'Y',
                        'email_sender': System['EmailSender'],
                        'email_recipient': System['EmailRecipient'],
                        'email_msg': System['EmailMsg'],
                        'email_subject': System['EmailSubject'],
                        'dataform_required': 'Y',
                        'region': common_variables['DF_Region'],
                        'repository_id': common_variables['DF_Repository_Id'],
                        'git_branch': common_variables['DF_GitBranch'],
                        'dataform_tags_with_sheets': dataform_tag,
                    }
                },               

            )

            # Task flow
            Start >> Create_Compilation_Result >> Task_InsertRunAudit >> Task_GetRunAudit >> Task_SFTP_to_GCS >> Task_Execute_SFTP_File_Load_Generic_Module >> End_Of_File_Load
   
    # Task to create dataform workflow to ODS -- execute
    Create_workflow_Invocation_Ods = DataformCreateWorkflowInvocationOperator(
        task_id="Create_workflow_Invocation_Ods",
        project_id=common_variables["DF_Project"],
        region=common_variables["DF_Region"],
        repository_id=common_variables["DF_Repository_Id_Ods"],
        workflow_invocation={
                "compilation_result": (
                    f"{{{{ task_instance.xcom_pull("f"task_ids='Create_Compilation_Result')"f"['name'] }}}}"
                ),            "invocation_config": {
                "included_tags": {common_variables[f"DF_Tags_Ods"]},
                "transitive_dependencies_included": True,
            },
        },
        gcp_conn_id=GCP_CONN_ID,
        trigger_rule='all_done',
        on_failure_callback=partial(on_failure_callback,
                            parent_system_id=parent_system_id,
                            mask_task_id=mask_task_id),
        on_success_callback=partial(on_success_callback,
                                    parent_system_id=parent_system_id,
                                    mask_task_id=mask_task_id)
    )

End_Of_File_Load >> Create_workflow_Invocation_Ods >> End