create  or replace table scratch.riders.TP_master_table as 
 
with base as
(
select 
 distinct 
 zone_code
 ,city_name
 ,CAST(EXTRACT(HOUR FROM START_OF_PERIOD_LOCAL) AS INT) as hour_of_day
from 
PRODUCTION.AGGREGATE.AGG_ZONE_DELIVERY_METRICS_HOURLY
where
date (start_of_period_local) between   current_date-30 and current_date
and is_within_zone_hours =true
and CNT_ERAT >0  
)
, base_2 as
(
 select 
 distinct 
 DATE_TRUNC(month, Date (START_OF_PERIOD_LOCAL)) AS order_month
 ,DAYNAME ( START_OF_PERIOD_LOCAL) as week_of_Day
 from 
 PRODUCTION.AGGREGATE.AGG_ZONE_DELIVERY_METRICS_HOURLY
where
date (start_of_period_local) between  dateadd('month', -6, date_trunc('month',  current_date())) and  LAST_DAY(dateadd('month', -1, date_trunc('month',  current_date())), MONTH)
and CNT_ERAT >0 
)
select * from base 
left join 
base_2 
on 1=1

;
