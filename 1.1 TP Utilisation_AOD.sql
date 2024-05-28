create  table scratch.riders.historical_Utilsation_AOD as
with base as
(
select 
upper(zone_code) as zone_code
,lower(city_name) as city_name
,DATE_TRUNC(month, Date (START_OF_PERIOD_LOCAL)) AS order_month 
,DAYNAME ( START_OF_PERIOD_LOCAL) as week_of_Day
,CAST(EXTRACT(HOUR FROM START_OF_PERIOD_LOCAL) AS INT) as hour_of_day
,coalesce(sum(ORDERS_ASAP),0)as orders_ASAP
,coalesce(sum(ORDERS_DELIVERED),0)as orders_delievered
,sum(coalesce(RET_MINS_SUM,0))as RET_total
,COALESCE(SUM(RET_MINS_SUM ), 0) / NULLIF(COALESCE(SUM(ret_mins_cnt ), 0), 0) AS RET_AVG
,COALESCE(SUM(aod_mins_sum ), 0) / NULLIF(COALESCE(SUM(aod_mins_cnt ), 0), 0) AS AOD_AVG
,NULLIFZERO(sum(ORDERS_ASAP))/NULLIFZERO(sum(RIDERS_AVAILABLE_IN_HOUR)) as TP_1
,sum(TOTAL_ORDER_TIME_PHYSICAL_ZONE_HOURS)as TOTAL_ORDER_TIME_PHYSICAL_ZONE_HOURS
,sum(TOTAL_TIME_PHYSICAL_ZONE_HOURS)as TOTAL_TIME_PHYSICAL_ZONE_HOURS
,NULLIFZERO(sum(RIDER_HOURS_ON_ORDERS_DHW_SUM))/ NULLIFZERO(sum(RIDER_HOURS_WORKED)) as Utilisation_rate_1
,coalesce(sum(TOTAL_ORDER_TIME_PHYSICAL_ZONE_HOURS),0)/NULLIF(COALESCE(sum(TOTAL_TIME_PHYSICAL_ZONE_HOURS ),0),0) as UR_zone_corrected
from
PRODUCTION.AGGREGATE.AGG_ZONE_DELIVERY_METRICS_HOURLY
where
date (start_of_period_local) between dateadd('month', -6, date_trunc('month',  current_date())) and '2024-04-30' -- get last 6 months data 
and CNT_ERAT >0 -- use this filter to get only active zone
group by 1,2,3,4,5
)
,TP_calc as
(
select 
*
,coalesce(UR_zone_corrected*(60/ RET_avg),TP_1)  as TP_zone_corrected
from 
base 
)

select 
a.*,b. UR_zone_corrected, b.AOD_AVG, b.RET_AVG
from 
    scratch.riders.TP_master_table as a
left join 
TP_calc as b
    on  lower(a.zone_code)=lower(b.zone_code)
    and lower(a.city_name)=lower(b.city_name)
    and a.order_month=b.order_month
    and a.hour_of_day=b.hour_of_day
    and a.week_of_day=b.week_of_day;
