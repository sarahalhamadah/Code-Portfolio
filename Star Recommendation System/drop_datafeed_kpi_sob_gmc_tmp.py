drop_datafeed_kpi_sob_gmc_tmp
drop_datafeed_kpi_sob_gmc_raw
insert_into_raw
drop_sob_gmc_datafeed_datekey
load_sob_gmc_datafeed_datekey
drop_sob_gmc_fact_datarefresh_in
insert_sob_gmc_fact_datarefresh_in





import os
import re
import csv
import time
import logging
import configparser
from airflow import DAG
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator

logging.getLogger("google.cloud.bigquery").setLevel(logging.ERROR)
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '..', 'configuration', 'code.config'))
datalake_project_id = config.get('parameters', 'datalake_project_id')
project_id = config.get('parameters', 'project_id')
database_in = config.get('parameters', 'database_in')
#database_sea_sales_in = config.get('parameters', 'database_sea_sales_in')
project_id_process = config.get('parameters', 'project_id_process')
topic = config.get('parameters', 'topic')

def on_failure(context):

    dag_id = context['dag_run'].dag_id

    task_id = context['task_instance'].task_id
    exception = context.get('exception')    
    message= " <h1 style=color:blue;> <b> Composer Notification - {} </b> </h1> </p><p style=color:blue;><b> Sea_sale_clone </b></p><p style=color:red;> DAG_ID: `{}` </p> <p style=color:black;> Task_ID: `{}` </p> <p style=color:black;> Status: Failed </p> </p> <p style=color:red;> Exception: `{}` </p>".format(project_id_process, dag_id, task_id, exception)
         
    publish_task = PubSubPublishMessageOperator(
        task_id="publish_task",
        project_id=project_id_process,
        topic=topic,
        messages=[{'data': bytes(message, encoding= 'utf-8')}],
    )
    publish_task.execute(context)
   
def on_success(context):

    dag_id = context['dag_run'].dag_id

    task_id = context['task_instance'].task_id
        
    message= " <h1 style=color:blue;> <b> Composer Notification - {} </b> </h1> </p> <p style=color:blue;><b> Sea_sale_clone </b></p><p style=color:green;> DAG_ID: `{}` </p> <p style=color:black;> Task_ID: `{}` </p> <p style=color:green;> Status: Success </p>".format(project_id_process, dag_id, task_id)
         
    publish_task = PubSubPublishMessageOperator(
        task_id="publish_task",
        project_id=project_id_process,
        topic=topic,
        messages=[{'data': bytes(message, encoding= 'utf-8')}],
    )
    publish_task.execute(context)

with DAG(
    'sob_data_refresh_datafeed_kpi_gmc',
    schedule_interval=None,
    start_date=datetime(2023, 9, 14),
    catchup=False,
    default_args={
        'on_failure_callback': on_failure,
        'on_success_callback': on_success,
        'retries': 0
    }
) as dag:

    def sleep():
        time.sleep(30)
    
    
    sleep_task_1= PythonOperator(
    task_id='sleep_task_1',
    python_callable=sleep,
    trigger_rule='all_success',
    dag=dag
)
    sleep_task_2 = PythonOperator(
    task_id='sleep_task_2',
    python_callable=sleep,
    trigger_rule='all_success',
    dag=dag
)  
    sleep_task_3= PythonOperator(
    task_id='sleep_task_3',
    python_callable=sleep,
    trigger_rule='all_success',
    dag=dag
)
    sleep_task_4 = PythonOperator(
    task_id='sleep_task_4',
    python_callable=sleep,
    trigger_rule='all_success',
    dag=dag
)  
    sleep_task_5= PythonOperator(
    task_id='sleep_task_5',
    python_callable=sleep,
    trigger_rule='all_success',
    dag=dag
)
    sleep_task_6 = PythonOperator(
    task_id='sleep_task_6',
    python_callable=sleep,
    trigger_rule='all_success',
    dag=dag
)  
    sleep_task_7= PythonOperator(
    task_id='sleep_task_7',
    python_callable=sleep,
    trigger_rule='all_success',
    dag=dag
)

                
    def drop_datafeed_kpi_sob_gmc_tmp():
        from google.cloud import bigquery
        client = bigquery.Client()
        query = f"""DROP TABLE IF EXISTS `{datalake_project_id}.{database_temp}.196865_aen_datafeed_kpi_sob_gmc_tmp`;"""
        query_job = client.query(query)
        try:
            query_job.result()
        except Exception as e:
            for error in query_job.errors:
                logging.error(f'Error: {error["message"]}')
        
    def drop_datafeed_kpi_sob_gmc_raw():
        from google.cloud import bigquery
        client = bigquery.Client()
        query=f"""DROP TABLE IF EXISTS `{datalake_project_id}.{database_raw}.196865_aen_datafeed_kpi_sob_gmc_raw`
        """
        query_job = client.query(query)
        try:
            query_job.result()
        except Exception as e:
            for error in query_job.errors:
                logging.error(f'Error: {error["message"]}')
    drop_datafeed_kpi_sob_gmc_tmp = PythonOperator(task_id='drop_datafeed_kpi_sob_gmc_tmp', python_callable=drop_datafeed_kpi_sob_gmc_raw, dag=dag)
    drop_datafeed_kpi_sob_gmc_raw = PythonOperator(task_id='drop_datafeed_kpi_sob_gmc_raw', python_callable=drop_datafeed_kpi_sob_gmc_raw, dag=dag)
    
    def insert_into_raw():
        from google.cloud import bigquery
        client = bigquery.Client()
        query=f"""
         CREATE TABLE `{datalake_project_id}.{database_raw}.196865_aen_datafeed_kpi_sob_gmc_raw` AS
SELECT
  *
FROM `{datalake_project_id}.{database_temp}.196865_aen_datafeed_kpi_sob_gmc_tmp`
       """

        query_job = client.query(query)
        try:
            query_job.result()
        except Exception as e:
            for error in query_job.errors:
                logging.error(f'Error: {error["message"]}')
    insert_into_raw = PythonOperator(task_id='insert_into_raw', python_callable=insert_into_raw, dag=dag)
    
   def drop_sob_gmc_datafeed_datekey():
        from google.cloud import bigquery
        client = bigquery.Client()
        query=f""" DROP TABLE IF EXISTS `{datalake_project_id}.{database_in}.196865_aen_sob_gmc_datafeed_datekey`
       """

        query_job = client.query(query)
        try:
            query_job.result()
        except Exception as e:
            for error in query_job.errors:
                logging.error(f'Error: {error["message"]}')
    drop_sob_gmc_datafeed_datekey = PythonOperator(task_id='drop_sob_gmc_datafeed_datekey', python_callable=drop_sob_gmc_datafeed_datekey, dag=dag)
    

          def load_sob_gmc_datafeed_datekey():
        from google.cloud import bigquery
        client = bigquery.Client()
        query=f"""CREATE TABLE  `{datalake_project_id}.{database_in}.196865_aen_sob_gmc_datafeed_datekey` AS
SELECT
  View_name,
  datekey,
  CASE
    WHEN SUBSTR(CAST(MAX(DateKey) AS string),5,2) ='01' THEN CONCAT('End Of Jan',' ',SUBSTR(CAST(MAX(DateKey) AS string),1,4))
    WHEN SUBSTR(CAST(MAX(DateKey) AS string),5,2) ='02' THEN CONCAT('End Of Feb',' ',SUBSTR(CAST(MAX(DateKey) AS string),1,4))
    WHEN SUBSTR(CAST(MAX(DateKey) AS string),5,2) ='03' THEN CONCAT('End Of Mar',' ',SUBSTR(CAST(MAX(DateKey) AS string),1,4))
    WHEN SUBSTR(CAST(MAX(DateKey) AS string),5,2) ='04' THEN CONCAT('End Of Apr',' ',SUBSTR(CAST(MAX(DateKey) AS string),1,4))
    WHEN SUBSTR(CAST(MAX(DateKey) AS string),5,2) ='05' THEN CONCAT('End Of May',' ',SUBSTR(CAST(MAX(DateKey) AS string),1,4))
    WHEN SUBSTR(CAST(MAX(DateKey) AS string),5,2) ='06' THEN CONCAT('End Of Jun',' ',SUBSTR(CAST(MAX(DateKey) AS string),1,4))
    WHEN SUBSTR(CAST(MAX(DateKey) AS string),5,2) ='07' THEN CONCAT('End Of Jul',' ',SUBSTR(CAST(MAX(DateKey) AS string),1,4))
    WHEN SUBSTR(CAST(MAX(DateKey) AS string),5,2) ='08' THEN CONCAT('End Of Aug',' ',SUBSTR(CAST(MAX(DateKey) AS string),1,4))
    WHEN SUBSTR(CAST(MAX(DateKey) AS string),5,2) ='09' THEN CONCAT('End Of Sep',' ',SUBSTR(CAST(MAX(DateKey) AS string),1,4))
    WHEN SUBSTR(CAST(MAX(DateKey) AS string),5,2) ='10' THEN CONCAT('End Of Oct',' ',SUBSTR(CAST(MAX(DateKey) AS string),1,4))
    WHEN SUBSTR(CAST(MAX(DateKey) AS string),5,2) ='11' THEN CONCAT('End Of Nov',' ',SUBSTR(CAST(MAX(DateKey) AS string),1,4))
    WHEN SUBSTR(CAST(MAX(DateKey) AS string),5,2) ='12' THEN CONCAT('End Of Dec',' ',SUBSTR(CAST(MAX(DateKey) AS string),1,4))
  ELSE
  NULL
END
  Data_as_of
FROM (
  SELECT
    '196865_aen_vametrics_fact_vw' View_Name,
    CAST(MAX(datekey) AS string) datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_vametrics_fact_vw`
  UNION ALL
  SELECT
    '196865_aen_gilc_planned_metrics_vw_in' View_Name,
    CAST(MAX(datekey) AS string)datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_gilc_planned_metrics_vw_in`
  UNION ALL
  SELECT
    '196865_aen_forecast_gilc_rev_cci_vw_in_dlk'View_Name,
    CAST(MAX(datekey) AS string) datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_forecast_gilc_rev_cci_vw_in_dlk`
  UNION ALL
  SELECT
    '196865_aen_negative_eac_fact_vw' View_Name,
    CAST(MAX(datekey) AS string) datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_negative_eac_fact_vw`
  UNION ALL
  SELECT
    '196865_aen_solution_contingency_FF_vw_in' View_Name,
    CAST(MAX(datekey) AS string) datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_solution_contingency_FF_vw_in`
  UNION ALL
  SELECT
    '196865_aen_fact_precls_dp_finance_vw_in'View_Name,
    CAST(MAX(DateKey) AS string) datekey,
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_fact_precls_dp_finance_vw_in`
  UNION ALL
  SELECT
    '196865_aen_chargeability_vw_in'View_Name,
    CAST(MAX(datekey) AS string) datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_chargeability_vw_in`
  UNION ALL
  SELECT
    '196865_aen_chargeability_trend_vw'View_Name,
    CAST(MAX(datekey) AS string) datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_chargeability_trend_vw`
  UNION ALL
  SELECT
    '196865_aen_fact_ssg_attribute_vw_in'View_Name,
    CAST(MAX(datekey) AS string) datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_fact_ssg_attribute_vw_in`
  UNION ALL
  SELECT
    '196865_aen_fact_sea_sales_opportunity_vw_in'View_Name,
    CAST(MAX(datekey) AS string) datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_fact_sea_sales_opportunity_vw_in`
  UNION ALL
  SELECT
    '196865_aen_mega_deal_pipeline_vw_in'View_Name,
    CAST(MAX(datekey) AS string) datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_mega_deal_pipeline_vw_in`
  UNION ALL
  SELECT
    '196865_aen_sob_nwd_actual_plan_vw_in'View_Name,
    CAST(MAX(datekey) AS string) datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_sob_nwd_actual_plan_vw_in`
  UNION ALL
  SELECT
    '196865_aen_fact_finance_vw_in',
    CAST(MAX(datekey) AS string) datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_fact_finance_vw_in`
  UNION ALL
  SELECT
    '196865_aen_sob_ODE_include_DA_vw_in'View_Name,
    CAST(MAX(datekey) AS string) datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_sob_ODE_include_DA_vw_in`
  UNION ALL
  SELECT
    '196865_aen_sob_ODE_exclude_DA_vw_in'View_Name,
    CAST(MAX(datekey) AS string) datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_sob_ODE_exclude_DA_vw_in`
  UNION ALL
  SELECT
    '196865_aen_fact_finance_sgp_vw_in'View_Name,
    CAST(MAX(datekey) AS string) datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_fact_finance_sgp_vw_in`
  UNION ALL
  SELECT
    '196865_aen_sob_partner_growth_vw_in'View_Name,
    CAST(MAX(datekey) AS string) datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_sob_partner_growth_vw_in`
  UNION ALL
  SELECT
    '196865_aen_fact_Levers_vw_in',
    REPLACE(CAST(MAX(Date) AS string),'-','') datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_fact_Levers_vw_in`
  UNION ALL
  SELECT
    '196865_aen_fact_won_adr_target_vw_in',
    CAST(MAX(Last_StartMon) AS string)
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_fact_won_adr_target_vw_in`
  UNION ALL
  SELECT
    '196865_aen_fact_sob_rev_post_vw_in',
    REPLACE(CAST(MAX(Date) AS string),'-','')
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_fact_sob_rev_post_vw_in`
  UNION ALL
  SELECT
    '196865_aen_fact_Chg_Data_vw_in',
    REPLACE(CAST(MAX(Date_1) AS string),'-','')
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_fact_Chg_Data_vw_in` )
GROUP BY
  1,
  2
UNION ALL
SELECT
  View_name,
  FYQ,
  CONCAT(SUBSTR(fyq,5,2),' Of ',CONCAT('20',SUBSTR(fyq,3,2))) Data_as_of
FROM (
  SELECT
    '196865_aen_sob_ODE_songix_vw_in' View_name,
    MAX(FYQ)FYQ
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_sob_ODE_songix_vw_in`
  UNION ALL
   SELECT
    '196865_aen_fact_industry_details_vw_in'View_Name,
    CAST(MAX(FYQ) AS string) datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_fact_industry_details_vw_in`
union all
 SELECT
    '196865_aen_fact_industry_forecast_vw_in'View_Name,
    CAST(MAX(fyq) AS string) datekey
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_fact_industry_forecast_vw_in`
  UNION ALL
  SELECT
    '196865_aen_fact_won_cci_in_vw_in',
    MAX(FYQ)FYQ
  FROM
    `{datalake_project_id}.{database_in}.196865_aen_fact_won_cci_in_vw_in` )
GROUP BY
  1,
  2
       """

        query_job = client.query(query)
        try:
            query_job.result()
        except Exception as e:
            for error in query_job.errors:
                logging.error(f'Error: {error["message"]}')
    load_sob_gmc_datafeed_datekey = PythonOperator(task_id='load_sob_gmc_datafeed_datekey', python_callable=load_sob_gmc_datafeed_datekey, dag=dag)
    

    def drop_sob_gmc_fact_datarefresh_in():
        from google.cloud import bigquery
        client = bigquery.Client()
        query=f"""DROP TABLE IF EXISTS `{datalake_project_id}.{database_in}.196865_aen_sob_gmc_fact_datarefresh_in`
       """

        query_job = client.query(query)
        try:
            query_job.result()
        except Exception as e:
            for error in query_job.errors:
                logging.error(f'Error: {error["message"]}')
    drop_sob_gmc_fact_datarefresh_in = PythonOperator(task_id='drop_sob_gmc_fact_datarefresh_in', python_callable=drop_sob_gmc_fact_datarefresh_in, dag=dag)
    --
     def insert_sob_gmc_fact_datarefresh_in():
        from google.cloud import bigquery
        client = bigquery.Client()
        query=f"""CREATE TABLE  `{datalake_project_id}.{database_in}.196865_aen_sob_gmc_fact_datarefresh_in` AS
SELECT
  a.data_source,
  A.data_refresh_feed_source_name,
  A.data_refresh_cycle,
  A.kpi,
  a.view,
  c.Data_as_of,
  B.DataLoadCompletionDTM,
  B.PBICompletionDTM,
FROM
  `prd-65343-datalake-bd-88394358.prd_test_196865_raw.196865_aen_datafeed_kpi_sob_gmc_raw` A
LEFT JOIN
  `prd-65343-datalake-bd-88394358.entprep_196865_in.196865_aen_perm_dashbaorddatafeedrefreshtimings_vw` B
ON
  A.data_refresh_feed_source_name=B.DataFeedName
LEFT JOIN
  `prd-65343-datalake-bd-88394358.prd_test_196865_in.196865_aen_sob_gmc_datafeed_datekey` c
ON
  A.view=c.view_name
       """

        query_job = client.query(query)
        try:
            query_job.result()
        except Exception as e:
            for error in query_job.errors:
                logging.error(f'Error: {error["message"]}')
    insert_sob_gmc_fact_datarefresh_in = PythonOperator(task_id='insert_sob_gmc_fact_datarefresh_in', python_callable=insert_sob_gmc_fact_datarefresh_in, dag=dag)
    

drop_datafeed_kpi_sob_gmc_tmp >> drop_datafeed_kpi_sob_gmc_raw >> insert_into_raw >> drop_sob_gmc_datafeed_datekey >> load_sob_gmc_datafeed_datekey >> drop_sob_gmc_fact_datarefresh_in >> insert_sob_gmc_fact_datarefresh_in