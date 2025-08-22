# pylint: disable=all

operator = "bigquery"

view1 = """  CREATE OR REPLACE VIEW `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_Chg_Data_ext_vw_in` AS 
SELECT
Date_1		,
Entity_Level_1		,
Entity_Level_2		,
entity_level_3		,
MTD_FTE	As FTE	,
SAFE_CAST(mtd_curr_chg as NUMERIC)	AS Curr_Chg	,
SAFE_CAST(mtd_official_target as NUMERIC)	As Official_Target	,
SAFE_CAST(mtd_var_target_chg as NUMERIC)	AS Var_Target_Chg	,
SAFE_CAST(mtd_standard_available_hrs as NUMERIC)	AS Standard_Available_Hrs	,
SAFE_CAST(mtd_prev_chg as NUMERIC)	AS Prev_Chg	,
SAFE_CAST(mtd_var_prev_chg as NUMERIC)	AS Var_Prev_Chg	,
SAFE_CAST(mtd_delta as NUMERIC)	AS Delta	,
SAFE_CAST(mtd_chg_hrs as NUMERIC)	As Chg_Hrs	,
SAFE_CAST(mtd_curr_external_chg as NUMERIC)	As Curr_External_Chg	,
SAFE_CAST(mtd_prev_external_chg as NUMERIC)	As Prev_External_Chg	,
SAFE_CAST(mtd_var_prev_external_chg as NUMERIC)	As Var_Prev_External_Chg	,
SAFE_CAST(mtd_external_chg_hrs as NUMERIC)	As External_Chg_Hrs	,
SAFE_CAST(mtd_var_monetized_chg as NUMERIC)	As MTD_Var_Monetized_Chg	,
SAFE_CAST(mtd_monetized_chg_gap_fte as NUMERIC)	As MTD_Monetized_Chg_Gap_FTE	,
Market		,
Entity_key		
FROM
  `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_fact_Chg_Data_ext_in`; 
"""

view2 = '''
CREATE OR REPLACE VIEW `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_DCS_vw_in` AS 
with maxQuarter as (
select max(Quarter) as Qtr 
from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_DCS_in`
)

select 
a.*,
case 
when Attribute = "weaker" and Quarter = maxQuarter.Qtr and Question_num = "Q1" then 1
when Attribute = "weaker" and Quarter = Concat(Concat("FY",cast(substr(maxQuarter.Qtr,3,2) as int)-1), right(maxQuarter.Qtr,2)) and Question_num = "Q1" then 2 
else null 
end as Flag
from(
select 
Quarter,
replace(replace(replace(Attribute,"Stronger_pcnt","Stronger"),"same_pcnt","The Same"),"weaker_pcnt","weaker") as Attribute,
Value,
Question_num,
DataAsOf
from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_DCS_in`
unpivot ( value for  Attribute in (Stronger_pcnt,same_pcnt,weaker_pcnt) )
) a , maxQuarter
'''

view3 = '''
CREATE OR REPLACE VIEW `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_FY23Plan_data_in_Fact_vw_in` AS 
SELECT
Market,	
MU,
Year,
New_Bookings,	
Revenue,
CCI,
BD,	
CI,
MU_Key,
MUCG_Key
FROM
  `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_FY23Plan_data_in_Fact`
'''
view4 = '''
CREATE OR REPLACE VIEW `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_GDP_Inflation_vw_in` AS 
with 
        markettable as(
        select distinct Market_Description, market_dim_key 
        from `prd-65343-datalake-bd-88394358.entprep_196865_in.196865_aen_dim_market_mu_in`
        ),
        maxCY as (
        select max(Year) as maxyear 
        FROM `prd-65343-datalake-bd-88394358.entprep_196865_in.196865_aen_GDP_Inflation_in`
        )
        SELECT
        Type,
        Current_Prev,
        Market,
        replace(Attribute,"Major Banks","Banks") as Attribute,
        Average,
        DataAsOf,
        Concat("CY" ,a.year) as Year, 
        case when year = maxcy.maxyear and type = "GDP" and current_prev = "Current" then 1
        when year = maxcy.maxyear and type = "GDP" and current_prev = "Prior" then 2
        when year = maxcy.maxyear and type = "Inflation" and current_prev = "Current" then 3
        when year = maxcy.maxyear and type = "Inflation" and current_prev = "Prior" then 4
        else null end as Flag,
        b.market_dim_key
        FROM `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_GDP_Inflation_in` a,maxCY
        left join
        markettable b
        on a.market = b.Market_Description
'''
view5 = '''
CREATE OR REPLACE VIEW `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_Guidance_Revisions_vw_in` AS 
with temptable as (
select max(label) as lbl, max(Quarter) as Qtr 
from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_Guidance_Revisions_in` where value is not null
)

select 
label,
Quarter, 
Value,
case 
when label = temptable.lbl and quarter = temptable.Qtr then 1
when label = temptable.lbl and quarter = concat(concat("CY",cast(substr(temptable.Qtr,3,2) as int) -1 ) , right(temptable.Qtr,2)) then 2
else null 
end as YoYflag,
DataAsOf
from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_Guidance_Revisions_in`,temptable
where value is not null
'''

view6 = '''
CREATE OR REPLACE VIEW `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_IT_Business_Services_Outlook_vw_in` AS 
with maxCY as (
        select max(CY) as maxyear 
        from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_IT_Business_Services_Outlook_in`
        )
        select 
        CY,
        Label,
        Prior_Forecast,
        Current_Forecast,
        QoQ, 
        DataAsOf,
        case 
        when CY = maxcy.maxyear then 1 
        else null 
        end as Flag
        from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_IT_Business_Services_Outlook_in` a,maxCY
'''

view7 = '''
CREATE OR REPLACE VIEW `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_Layoff_Announcements_vw_in` AS 
with datetable as (
select 
Extract(month from(max(parse_datetime("%d-%B-%Y %H:%M%p",Date)))) as maxmonth, max(year) as maxyear 
from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_Layoff_Announcements_in`
),

markettable as (
Select distinct 
market_description, 
market_dim_key 
from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_dim_market_mu_in`
)

select 
a.Primary_Market,
b.market_dim_key,
parse_datetime("%d-%B-%Y %H:%M%p",Date) as date,
a.count,
case 
when year = datetable.maxyear and Extract(month from(parse_datetime("%d-%B-%Y %H:%M%p",Date))) = datetable.maxmonth then 1
when year = datetable.maxyear and Extract(month from(parse_datetime("%d-%B-%Y %H:%M%p",Date))) = datetable.maxmonth-1 then 2 
else null 
end as MoMFlag,
DataAsOf
from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_Layoff_Announcements_in` a, datetable
left join markettable b
on trim(a.Primary_market) = b.market_description
'''
view8 = '''
CREATE OR REPLACE VIEW `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_ProjectedWinsSoQ_li_vw` AS 
SELECT
        ServiceGroup,
        Market,
        Wins,
        Small_Wins,
        Large_Wins,
        Mega_Wins
        FROM `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_ProjectedWinsSoQ_li_in`
'''
view9 = '''
CREATE OR REPLACE VIEW `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_SP500_Price_vw_in` AS 
with 
		Var1 as
		(select max(date) as maxdate
		from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_SP500_Price_in`),
		Var2 as
		(select  min(date) as mindate7
		from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_SP500_Price_in`
		where date <= Date_SUB((select max(date)
		from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_SP500_Price_in`),INTERVAL 3 Day) and date >= Date_SUB((select max(date)
		from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_SP500_Price_in`),INTERVAL 1 Week)
		),
		Var3 as
		(select  min(date) as mindate30
		from `prd-65343-datalake-bd-88394358.entprep_196865_in.196865_aen_SP500_Price_in`
		where date <= Date_SUB((select max(date)
		from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_SP500_Price_in`),INTERVAL 23 Day) and date >= Date_SUB((select max(date)
		from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_SP500_Price_in`),INTERVAL 1 Month)
		),
		Var4 as
		(select  min(date) as mindate90
		from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_SP500_Price_in`
		where date <= Date_SUB((select max(date)
		from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_SP500_Price_in`),INTERVAL 80 Day) and date >= Date_SUB((select max(date)
		from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_SP500_Price_in`),INTERVAL 3 Month)
		)
		SELECT
		a.date,
		a.SP500_Price,
		b.datekey,
		case when date = var1.maxdate then "1"
		when a.date = var2.mindate7 then "2"
		when a.date = var3.mindate30 then "3"
		when a.date = var4.mindate90 then "4"
		else null end as Flag
		FROM
		`prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_SP500_Price_in` a,Var1,var2,var3,var4
		left join `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_dim_date_key_in` b
		on a.date = CAST(PARSE_DATE('%m/%d/%Y',b.fulldate) as DATE)
		WHERE cast(a.date as STRING) IS NOT NULL
'''
view10 = '''
CREATE OR REPLACE VIEW `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_Suspension_unassigned_trend_vw_in` AS 
with datetable as
        (select
        max(Date_1) as maxdate,
        date_sub(max(Date_1), Interval 1 year) as mindate
        from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_Suspension_unassigned_trend_in`
        where Suspension_unassigned_pcnt is not NULL
        ),
        markettable as (
        Select distinct
        market_description,
        market_dim_key
        from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_dim_market_mu_in`
        )
        select
        a.Date_1,
        a.Suspension_unassigned_pcnt,
        a.Market,
        case
            when Date_1 = datetable.maxdate then 1
            when Date_1 = datetable.mindate then 2
        else null
        end as YOYFlag,
        b.market_dim_key,
        a.DataAsOf,
        from `prd-65343-datalake-bd-88394358.entsit_196865_in.196865_aen_Suspension_unassigned_trend_in` a, datetable
        left join markettable b
        on a.market = b.market_description
'''



sql=[view1,view2,view3,view4,view5,view6,view7,view8,view9,view10]