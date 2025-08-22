## builds (and rebuilds) a backup/derived table in BigQuery from your SEA Sales “opportunity” data, then triggers a downstream fact-build DAG. It also pushes Pub/Sub notifications on every task success/failure.

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
sea_sales_opp_kpi_in= config.get('parameters', 'sea_sales_opp_kpi_in')
sea_sales_ssg_ISS_AllocationPct= config.get('parameters', 'sea_sales_ssg_ISS_AllocationPct')
project_id = config.get('parameters', 'project_id')
database_in = config.get('parameters', 'database_in')
database_raw= config.get('parameters', 'database_raw')
project_id_process = config.get('parameters', 'project_id_process')
topic = config.get('parameters', 'topic')

def on_failure(context):

    dag_id = context['dag_run'].dag_id

    task_id = context['task_instance'].task_id
    exception = context.get('exception')    
    message= " <h1 style=color:blue;> <b> Composer Notification - {} </b> </h1> </p><p style=color:blue;><b> AccEntScrcd_Composer_Notification </b></p><p style=color:red;> DAG_ID: `{}` </p> <p style=color:black;> Task_ID: `{}` </p> <p style=color:black;> Status: Failed </p> </p> <p style=color:red;> Exception: `{}` </p>".format(project_id_process, dag_id, task_id, exception)
         
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
        
    message= " <h1 style=color:blue;> <b> Composer Notification - {} </b> </h1> </p> <p style=color:blue;><b> AccEntScrcd_Composer_Notification </b></p><p style=color:green;> DAG_ID: `{}` </p> <p style=color:black;> Task_ID: `{}` </p> <p style=color:green;> Status: Success </p>".format(project_id_process, dag_id, task_id)
         
    publish_task = PubSubPublishMessageOperator(
        task_id="publish_task",
        project_id=project_id_process,
        topic=topic,
        messages=[{'data': bytes(message, encoding= 'utf-8')}],
    )
    publish_task.execute(context)

with DAG(
    'sea_sales_opp_bkp',
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
    sleep_task_3 = PythonOperator(
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

    sleep_task_5 = PythonOperator(
    task_id='sleep_task_5',
    python_callable=sleep,
    trigger_rule='all_success',
    dag=dag
)

    def drop_sea_sales_opp_bkp_in():
        from google.cloud import bigquery
        client = bigquery.Client()
        query = f"""DROP TABLE IF EXISTS `{project_id}.{database_in}.196865_aen_sea_sales_opportunity_in_bkp`;"""
        query_job = client.query(query)
        try:
            query_job.result()
        except Exception as e:
            for error in query_job.errors:
                logging.error(f'Error: {error["message"]}')
        
    def create_sea_sales_opp_bkp_in():
        from google.cloud import bigquery
        client = bigquery.Client()
        query=f'''
        CREATE TABLE `{datalake_project_id}.{database_in}.196865_aen_sea_sales_opportunity_in_bkp`  AS
with mnlable as (select distinct   
  case when Extract(MONTH FROM max(reportingdate)) in (9,12,3,6) then 1 
  when Extract(MONTH FROM max(reportingdate)) in (10,1,4,7) then 2
  when Extract(MONTH FROM max(reportingdate)) in (11,2,5,8) then 3
  
  end  as mnlable
  from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` 
  where  datekey in (select max(Datekey) from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where StatusDesc  in ('Pipeline') and PeriodCd='ME'  ) 

  )

SELECT
  MasterCustomerNm,
  MasterCustomerNbr,
  HistoricalMasterClientClassDesc,
  CurrentMasterClientClassDesc as CurrentMasterClientClass,
  sea_s.Datekey,
  CASE WHEN (MarketNm='' OR MarketNm IS NULL) THEN 'blank' ELSE MarketNm END as Market,
  CASE WHEN (MarketUnitNm='' OR MarketUnitNm IS NULL) THEN 'blank' ELSE MarketUnitNm END as MarketUnit,
  CASE WHEN UPPER(ClientGroupNm) like ('%CMT%') THEN 'Communications, Media & Technology'
			 WHEN UPPER(ClientGroupNm) like ('%PRD%') THEN 'Products'
			 WHEN UPPER(ClientGroupNm) like ('%FS%') THEN 'Financial Services'
			 WHEN UPPER(ClientGroupNm) like ('%RES%') THEN 'Resources'
			 WHEN UPPER(ClientGroupNm) like ('%H&PS%') THEN 'Health & Public Service'
			 WHEN (ClientGroupNm='' OR ClientGroupNm IS NULL) THEN 'blank'
			 ELSE 'blank' END as IndustryGroup,
  CASE WHEN (MasterCustomerIndustryNm='' OR MasterCustomerIndustryNm IS NULL) THEN 'blank' ELSE MasterCustomerIndustryNm END as IndustrySegment,
   cast(null as timestamp) as ExpectedContractSignDt,
  ExpectedContractSignDtFiscalQuarter,
  QuarterClosed,
  MonthClosed,
  StatusDesc,
  StageDesc,
  cast(null as timestamp) as PipelineEntryDt,
  cast(null as timestamp) as StatusSinceDt,
  cast(null as timestamp) as ConsultingStartDt,
  cast(null as timestamp) as ConsultingEndDt,
  CAST(SUM(TotalCurrentRevenueGlobalAmt*IFNULL(IndustrySubSegment_Percentage,1)) as NUMERIC) as TCR,
  CASE WHEN (ServiceGroupDesc='' OR ServiceGroupDesc is null ) THEN 'blank' ELSE   ServiceGroupDesc END as ServiceGroup,
  CAST(SUM(ContractControllableIncomeGlobalAmt*IFNULL(IndustrySubSegment_Percentage,1)) as NUMERIC) as CCI,
  CAST(SUM(WorkHours *IFNULL(IndustrySubSegment_Percentage,1))as NUMERIC) as WorkHours,
  CAST(SUM(WeightedPipelineGlobalAmt *IFNULL(IndustrySubSegment_Percentage,1))as NUMERIC) as WtdPipeline,
  CAST(SUM(ServicesRevenueGlobalExcludingBEAmt *IFNULL(IndustrySubSegment_Percentage,1))as NUMERIC) as ServicesRevenueGlobalExcludingBEAmt,
  DealSizeStandardDesc as DealSizeStandardDesc,
  CAST(SUM(TotalCurrentRevenueGlobalExcludingBEAmt *IFNULL(IndustrySubSegment_Percentage,1))as NUMERIC) as TCRGlobalExcludingBEAmt,
  CAST(SUM(WeightedTargetMarginNumeratorGlobalExcludingBEAmt *IFNULL(IndustrySubSegment_Percentage,1))as NUMERIC) as WeightedTargetMarginNumGlobalExcludingBEAmt,
  CAST(SUM(WeightedTargetMarginDenominatorGlobalExcludingBEAmt*IFNULL(IndustrySubSegment_Percentage,1))as NUMERIC) as WeightedTargetMarginDenGlobalExcludingBEAmt,
  ServicesDesc,
  DealFamilyDesc,
  case when TypeOfWorkCd= 'C' then 'Consulting'
   when TypeOfWorkCd= 'O' then 'Outsourcing'  else TypeOfWorkCd End as  TypeOfWorkCd ,
   WinRateFlag,
   SubServiceGroupAHRWithinRangeInd,
SubServiceGroupHasEffortInd,

CAST(SUM(ServicesCostConstantAmt *IFNULL(IndustrySubSegment_Percentage,1)) as NUMERIC) as ServicesCostConstantAmt,
CAST(SUM(ServicesRevenueConstantExcludingBEAmt *IFNULL(IndustrySubSegment_Percentage,1)) as NUMERIC) as ServicesRevenueConstantExcludingBEAmt,
CAST(SUM(TotalCurrentRevenueConstantAmt *IFNULL(IndustrySubSegment_Percentage,1))  as NUMERIC) as TotalCurrentRevenueConstantAmt,
CAST(SUM(OtherBillableCostConstantAmt *IFNULL(IndustrySubSegment_Percentage,1)) as NUMERIC) as OtherBillableCostConstantAmt,
CAST(SUM(OtherBillableCostGlobalAmt  *IFNULL(IndustrySubSegment_Percentage,1)) as NUMERIC) as OtherBillableCostGlobalAmt ,
CAST(SUM(ServicesCostGlobalAmt  *IFNULL(IndustrySubSegment_Percentage,1)) as NUMERIC) as ServicesCostGlobalAmt ,
CAST(SUM(WeightedTargetMarginNumeratorConstantExcludingBEAmt *IFNULL(IndustrySubSegment_Percentage,1)) as NUMERIC) as WeightedTargetMarginNumeratorConstantExcludingBEAmt,
CAST(SUM(WeightedTargetMarginDenominatorConstantExcludingBEAmt *IFNULL(IndustrySubSegment_Percentage,1)) as NUMERIC) as WeightedTargetMarginDenominatorConstantExcludingBEAmt,
CAST(SUM(TotalCurrentRevenueConstantExcludingBEAmt *IFNULL(IndustrySubSegment_Percentage,1)) as NUMERIC) as TotalCurrentRevenueConstantExcludingBEAmt,
CAST(SUM(ContractControllableIncomeConstantAmt *IFNULL(IndustrySubSegment_Percentage,1)) as NUMERIC) as ContractControllableIncomeConstantAmt,
case when  (Extract(MONTH FROM ReportingDate) = 9 and (select * from mnlable)=1) then CONCAT('FY',Substr(cast(Extract(Year FROM ReportingDate)+1 as String),3),'Q1-M2') 
when  (Extract(MONTH FROM ReportingDate) = 6 and (select * from mnlable)=1) then CONCAT('FY',Substr(cast(Extract(Year FROM ReportingDate) as String),3),'Q4-M2') 
when  (Extract(MONTH FROM ReportingDate) = 3 and (select * from mnlable)=1) then CONCAT('FY',Substr(cast(Extract(Year FROM ReportingDate) as String),3),'Q3-M2') 
when  (Extract(MONTH FROM ReportingDate) = 12 and (select * from mnlable)=1) then CONCAT('FY',Substr(cast(Extract(Year FROM ReportingDate)+1 as String),3),'Q2-M2') 
 when  (Extract(MONTH FROM ReportingDate) = 10 and (select * from mnlable)=2) then CONCAT('FY',Substr(cast(Extract(Year FROM ReportingDate)+1 as String),3),'Q1-M3') 
when  (Extract(MONTH FROM ReportingDate) = 1 and (select * from mnlable)=2) then CONCAT('FY',Substr(cast(Extract(Year FROM ReportingDate) as String),3),'Q2-M3') 
when  (Extract(MONTH FROM ReportingDate) = 4 and (select * from mnlable)=2) then CONCAT('FY',Substr(cast(Extract(Year FROM ReportingDate) as String),3),'Q3-M3') 
when  (Extract(MONTH FROM ReportingDate) = 7 and (select * from mnlable)=2) then CONCAT('FY',Substr(cast(Extract(Year FROM ReportingDate) as String),3),'Q4-M3')
when  (Extract(MONTH FROM ReportingDate) = 11 and (select * from mnlable)=3) then CONCAT('FY',Substr(cast(Extract(Year FROM ReportingDate)+1 as String),3),'Q2-M1') 
when  (Extract(MONTH FROM ReportingDate) = 2 and (select * from mnlable)=3) then CONCAT('FY',Substr(cast(Extract(Year FROM ReportingDate) as String),3),'Q3-M1') 
when  (Extract(MONTH FROM ReportingDate) = 5 and (select * from mnlable)=3) then CONCAT('FY',Substr(cast(Extract(Year FROM ReportingDate) as String),3),'Q4-M1') 
when  (Extract(MONTH FROM ReportingDate) = 8 and (select * from mnlable)=3) then CONCAT('FY',Substr(cast(Extract(Year FROM ReportingDate)+1 as String),3),'Q1-M1')
     else null END as Quarter_Beginning_Period,

	CASE WHEN StatusDesc='Unqualified' OR StatusDesc='Pipeline' THEN 
DATE(CONCAT(FORMAT_DATE('%Y', PARSE_DATE('%B %Y', ExpectedContractSignDtMonth)),'-',FORMAT_DATE('%m', PARSE_DATE('%B %Y', ExpectedContractSignDtMonth)),'-','01'))
ELSE 
	DATE(CONCAT(FORMAT_DATE('%Y', PARSE_DATE('%B %Y', MonthClosed)),'-',FORMAT_DATE('%m', PARSE_DATE('%B %Y', MonthClosed)),'-','01')) END as ReportingStatusDate,
    CAST(ReportingDate as DATE) as ReportingDate,
  SUM(CAST(PursuitsGlobalAmt as FLOAT64)*IFNULL(IndustrySubSegment_Percentage,1)) as PursuitsGlobalAmt,
  OpportunityExtensionInd,
  CompetitionCategory,
  MegaDealDesc,
  ClientGroupNm,
ISS_IndustryGroup,
ISS_IndustrySegment,
QuarterSpecificInd,
CountryNm as Country,
ResponsibleBusinessEntityWithOverrideNm as service_dim,
StandardDealGroupDesc,
IF
(ClientGroupCd = ''
 OR ClientGroupCd IS NULL, 'blank',ClientGroupCd) AS ClientGroupCd 
FROM
(select sea_subquery.*,IndustrySubSegment,IndustrySubSegment_Percentage,  CASE WHEN ssg_industrysubseg.ISS_OpportunityId IS NULL THEN 'Other' ELSE ssg_industrysubseg.ISS_IndustryGroup END as ISS_IndustryGroup,
  CASE WHEN ssg_industrysubseg.ISS_OpportunityId IS NULL THEN 'Other' ELSE ssg_industrysubseg.ISS_IndustrySegment END as ISS_IndustrySegment
  from
  `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` sea_subquery
    LEFT JOIN
(select Datekey as ISS_Datekey,
  OpportunityId as ISS_OpportunityId,
IndustrySubSegment,
IndustrySubSegment_Percentage,
IndustrySegment as ISS_IndustrySegment,
IndustryGroup as ISS_IndustryGroup from
  `{datalake_project_id}.{database_in}.{sea_sales_ssg_ISS_AllocationPct}`) ssg_industrysubseg
on sea_subquery.datekey = ssg_industrysubseg.ISS_Datekey
AND sea_subquery.OpportunityId = ssg_industrysubseg.ISS_OpportunityId
WHERE sea_subquery.ExpectedContractSignDtMonth <> ''
) sea_s
  
LEFT JOIN
  `{datalake_project_id}.{database_raw}.196865_aen_market_mu_mapping_raw` mu_map
ON
  sea_s.MarketNm = mu_map.market
  AND sea_s.MarketUnitNm = mu_map.market_unit
  AND mu_map.Flag in ('Y','NC')      ## Added by Arunima on 04.10.2023 for MU Org changes
where sea_s.StatusDesc<>'Closed-Duplicated' AND sea_s.PeriodCd='ME' 

AND sea_s.DateKey in ( 
  --FY20 year end datekey
  20200904,
  cast(Format_date ("%Y%m%d",PARSE_DATE("%Y%m%d", cast((SELECT max(Datekey) FROM `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME' ) as STRING)) ) as int64) ,
 
(SELECT   MAX(datekey) FROM  `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(datekey as string) LIKE (
SELECT concat(SUBSTR(Format_date ("%Y%m%d",DATE_SUB(PARSE_DATE("%Y%m%d",CAST((SELECT MAX(Datekey) from
`{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME') AS STRING)),INTERVAL 1 QUARTER) ),0,6),'%'))), 

(SELECT   MAX(datekey) FROM  `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(datekey as string) LIKE (
SELECT concat(SUBSTR(Format_date ("%Y%m%d",DATE_SUB(PARSE_DATE("%Y%m%d",CAST((SELECT MAX(Datekey) from
`{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME') AS STRING)),INTERVAL 2 QUARTER) ),0,6),'%'))),

(SELECT   MAX(datekey) FROM  `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(datekey as string) LIKE (
SELECT concat(SUBSTR(Format_date ("%Y%m%d",DATE_SUB(PARSE_DATE("%Y%m%d",CAST((SELECT MAX(Datekey) from
`{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME') AS STRING)),INTERVAL 3 QUARTER) ),0,6),'%'))),

(SELECT   MAX(datekey) FROM  `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(datekey as string) LIKE (
SELECT concat(SUBSTR(Format_date ("%Y%m%d",DATE_SUB(PARSE_DATE("%Y%m%d",CAST((SELECT MAX(Datekey) from
`{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME') AS STRING)),INTERVAL 1 YEAR) ),0,6),'%'))),

(SELECT   MAX(datekey) FROM  `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(datekey as string) LIKE (
SELECT concat(SUBSTR(Format_date ("%Y%m%d",DATE_SUB(PARSE_DATE("%Y%m%d",CAST((SELECT MAX(Datekey) from
`{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME') AS STRING)),INTERVAL 2 YEAR) ),0,6),'%'))),





(select max(datekey) FROM `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(Datekey as string) like (select CONCAT('20',cast(SUBSTR(max(QuarterClosed),(-4),2) as int64)-3,'09%') from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where datekey in (select max(datekey) from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}`))),

(select max(datekey) FROM `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(Datekey as string) like (select CONCAT('20',cast(SUBSTR(max(QuarterClosed),(-4),2) as int64)-2,'09%') from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where datekey in (select max(datekey) from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}`))),

(select max(datekey) FROM `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(Datekey as string) like (select CONCAT('20',cast(SUBSTR(max(QuarterClosed),(-4),2) as int64)-1,'09%') from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where datekey in (select max(datekey) from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}`))),

(SELECT MAX(datekey) FROM `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` WHERE CAST(Datekey AS string) LIKE ( SELECT DISTINCT CONCAT(CAST(SUBSTR(TRIM(CAST(datekey AS string)),0,4) AS int64)-1,'08%') FROM `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` WHERE datekey = (SELECT MAX(datekey) FROM `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME'))),

(select max(datekey) FROM `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(Datekey as string) like (select CONCAT('20',cast(SUBSTR(max(QuarterClosed),(-4),2) as int64)-1,'12%') from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where datekey in (select max(datekey) from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME'))),

(select max(datekey) FROM `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(Datekey as string) like (select CONCAT('20',cast(SUBSTR(max(QuarterClosed),(-4),2) as int64)-1,'03%') from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where datekey in (select max(datekey) from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME'))),

(select max(datekey) FROM `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(Datekey as string) like (select CONCAT('20',cast(SUBSTR(max(QuarterClosed),(-4),2) as int64)-1,'06%') from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where datekey in (select max(datekey) from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME'))),

(select max(datekey) FROM `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(Datekey as string) like (select CONCAT('20',cast(SUBSTR(max(QuarterClosed),(-4),2) as int64),'12%') from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where datekey in (select max(datekey) from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME'))),

(select max(datekey) FROM `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(Datekey as string) like (select CONCAT('20',cast(SUBSTR(max(QuarterClosed),(-4),2) as int64),'03%') from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where datekey in (select max(datekey) from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME'))),

(select max(datekey) FROM `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(Datekey as string) like (select CONCAT('20',cast(SUBSTR(max(QuarterClosed),(-4),2) as int64),'06%') from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where datekey in (select max(datekey) from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME'))),

(select max(datekey) FROM `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(Datekey as string) like (select CONCAT('20',cast(SUBSTR(max(QuarterClosed),(-4),2) as int64)-2,'12%') from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where datekey in (select max(datekey) from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME'))),

(select max(datekey) FROM `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(Datekey as string) like (select CONCAT('20',cast(SUBSTR(max(QuarterClosed),(-4),2) as int64)-2,'03%') from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where datekey in (select max(datekey) from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME'))),

(select max(datekey) FROM `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(Datekey as string) like (select CONCAT('20',cast(SUBSTR(max(QuarterClosed),(-4),2) as int64)-2,'06%') from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where datekey in (select max(datekey) from `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME'))),
(SELECT   MAX(datekey) FROM  `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(datekey as string) LIKE (

SELECT concat(SUBSTR(Format_date ("%Y%m%d",DATE_SUB(PARSE_DATE("%Y%m%d",CAST((SELECT MAX(Datekey) from

`{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME') AS STRING)),INTERVAL 5 QUARTER) ),0,6),'%'))), 

(SELECT   MAX(datekey) FROM  `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(datekey as string) LIKE (

SELECT concat(SUBSTR(Format_date ("%Y%m%d",DATE_SUB(PARSE_DATE("%Y%m%d",CAST((SELECT MAX(Datekey) from

`{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME') AS STRING)),INTERVAL 7 QUARTER) ),0,6),'%'))),

(SELECT   MAX(datekey) FROM  `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(datekey as string) LIKE (

SELECT concat(SUBSTR(Format_date ("%Y%m%d",DATE_SUB(PARSE_DATE("%Y%m%d",CAST((SELECT MAX(Datekey) from

`{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME') AS STRING)),INTERVAL 6 QUARTER) ),0,6),'%'))),

(SELECT   MAX(datekey) FROM  `{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where cast(datekey as string) LIKE (

SELECT concat(SUBSTR(Format_date ("%Y%m%d",DATE_SUB(PARSE_DATE("%Y%m%d",CAST((SELECT MAX(Datekey) from

`{datalake_project_id}.sea_sales_bkp_196865_ts.{sea_sales_opp_kpi_in}` where PeriodCd='ME') AS STRING)),INTERVAL 4 QUARTER) ),0,6),'%')))

)GROUP BY
MasterCustomerNm,
MasterCustomerNbr,
sea_s.Datekey,
MarketNm,
MarketUnitNm,
ClientGroupNm,
MasterCustomerIndustryNm,
--ExpectedContractSignDt,
ExpectedContractSignDtFiscalQuarter,
ExpectedContractSignDtMonth,
QuarterClosed,
MonthClosed,
StatusDesc,
StageDesc,
--PipelineEntryDt,
--StatusSinceDt,
--ConsultingStartDt,
--ConsultingEndDt,
ServiceGroupDesc,
DealSizeStandardDesc,
ServicesDesc,
DealFamilyDesc,
TypeOfWorkCd,
WinRateFlag,
SubServiceGroupAHRWithinRangeInd,
SubServiceGroupHasEffortInd,
sea_s.ReportingDate,
HistoricalMasterClientClassDesc,
CurrentMasterClientClassDesc,
  OpportunityExtensionInd,
  CompetitionCategory,
  MegaDealDesc,
ISS_IndustryGroup,
ISS_IndustrySegment,
QuarterSpecificInd,
Country,
ResponsibleBusinessEntityWithOverrideNm,
StandardDealGroupDesc,
ClientGroupCd
  ;
        '''
        query_job = client.query(query)
        try:
            query_job.result()
        except Exception as e:
            for error in query_job.errors:
                logging.error(f'Error: {error["message"]}')
    drop_sea_sales_opp_bkp_in = PythonOperator(task_id='drop_sea_sales_opp_bkp_in', python_callable=drop_sea_sales_opp_bkp_in, dag=dag)
    create_sea_sales_opp_bkp_in = PythonOperator(task_id='create_sea_sales_opp_bkp_in', python_callable=create_sea_sales_opp_bkp_in, dag=dag)
    trigger_sea_sales_opp_bkp_fact = TriggerDagRunOperator(
        task_id='trigger_sea_sales_opp_bkp_fact',
        trigger_dag_id='sea_sales_opp_fact_bkp',
        dag=dag,
        trigger_rule='all_success')

    drop_sea_sales_opp_bkp_in>>sleep_task_1>>create_sea_sales_opp_bkp_in>>sleep_task_2>>trigger_sea_sales_opp_bkp_fact>>sleep_task_3



   