-- query to create relation between UR and AOD , used for predicting UR
create   table scratch.riders.historical_Utilsation_AOD_daily as
with base as
(
select 
distinct 
lower(city_name) as city_name
,case when zone_code in
('ABAR','AKW','AMR','AMZ','ANH','AQ','AQPP','AQS','ATW','AWH','DBB','DCH','DD11','DDO','DEIR','DFC','DIC','DMEY','DSO',
'DSW','DT','DWS','FCOW','HCC','HPDT','JMH','KAR','MRDF','NAS','RAK','UMS','WAA','ZBL') then  'Deira and Downtown Cluster'
when zone_code in (
'AR','DBRS','DDH','DEGC','DHS','DHS2','DICP','DIP','DJP','DM','DSB','DTS','HPJLT','HPMC','IBN','JBLI','JGE','JI',
'JLT','JVC','JVT','MDW8','PC','PR','RMRM','SC','SPR','SS','TM','TWM') then 'Sportcity and Marina Cluster'
else city_name end as Cluster_Assigned
,Date(START_OF_PERIOD_LOCAL) as dt
-- ,CAST(EXTRACT(HOUR FROM TO_TIMESTAMP(START_OF_PERIOD_LOCAL )) AS INT)as order_created_hour
,case 
     when CAST(EXTRACT(HOUR FROM START_OF_PERIOD_LOCAL) AS INT)  in (12, 13) and DAYNAME(START_OF_PERIOD_LOCAL) in ('Mon', 'Tue', 'Wed','Thu','Fri') then 'Peak_lunch'
     when CAST(EXTRACT(HOUR FROM START_OF_PERIOD_LOCAL) AS INT)  in (19, 20) and DAYNAME(START_OF_PERIOD_LOCAL) in ('Mon', 'Tue', 'Wed','Thu') then 'Peak Dinner'
     when CAST(EXTRACT(HOUR FROM START_OF_PERIOD_LOCAL) AS INT)  in (19, 20) and DAYNAME(START_OF_PERIOD_LOCAL) in ('Fri','Sat','Sun') then 'Super Peak Dinner'
     else 'Other' end as shift

,coalesce(sum(ORDERS_DELIVERED),0)as orders_delievered
,COALESCE(SUM(RET_MINS_SUM ), 0) / NULLIF(COALESCE(SUM(ret_mins_cnt ), 0), 0) AS RET_AVG
,COALESCE(SUM(aod_mins_sum ), 0) / NULLIF(COALESCE(SUM(aod_mins_cnt ), 0), 0) AS AOD_AVG
,NULLIFZERO(sum(ORDERS_ASAP))/NULLIFZERO(sum(RIDERS_AVAILABLE_IN_HOUR)) as TP_1
,NULLIFZERO(sum(RIDER_HOURS_ON_ORDERS_DHW_SUM))/ NULLIFZERO(sum(RIDER_HOURS_WORKED)) as Utilisation_rate_1
,coalesce(sum(TOTAL_ORDER_TIME_PHYSICAL_ZONE_HOURS),0)/NULLIF(COALESCE(sum(TOTAL_TIME_PHYSICAL_ZONE_HOURS ),0),0) as UR_zone_corrected
    
from
PRODUCTION.AGGREGATE.AGG_ZONE_DELIVERY_METRICS_HOURLY
where  date (start_of_period_local) between '2023-09-01' and '2024-05-30'
-- date (start_of_period_local) between 
--     dateadd('month', -6, date_trunc('month',  current_date()))
--     and  LAST_DAY(dateadd('month', -1, date_trunc('month',  current_date())), MONTH)
and CNT_ERAT >0 -- use this filter to get only active zone
and case when UPPER(country_name) ='KUWAIT' and date (start_of_period_local) between '2024-03-10' and '2024-04-10' then true else false end  =false -- remove the id data from kuwait
group by 1,2,3,4
)
,TP_calc as
(
select 
*
,coalesce(UR_zone_corrected*(60/ RET_avg),TP_1)  as TP_zone_corrected
from 
base 
)
select *exclude(TP_1) from TP_calc;

select count(*), count(distinct CLUSTER_ASSIGNED, dt,shift)  from  scratch.riders.historical_Utilsation_AOD_daily-- 6556 
GRANT SELECT ON TABLE scratch.riders.historical_Utilsation_AOD_daily TO ROLE BI_DEVELOPMENT;



