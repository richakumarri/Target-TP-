
create table scratch.riders.avg_route_distance_last_30_dys as

with route_distance as
(
 select 
     ORDER_ID as ID
     ,picked_up_to_completed_at_raw_meters
     ,row_number () over (partition by ORDER_ID order by PICKUP_CREATED_AT desc )as rnk
 from 
    production.riders.rider_diversions 
 where date (PICKUP_CREATED_AT) between dateadd('month', -7, date_trunc('month',  current_date())) and   LAST_DAY(dateadd('month', -1, date_trunc('month',  current_date())), MONTH)
 qualify rnk=1 
)
,base as
(
   select
     ID
    ,date(LOCAL_TIME_CREATED_AT)as order_dt   
    ,CAST(EXTRACT(HOUR FROM TO_TIMESTAMP(LOCAL_TIME_CREATED_AT )) AS INT)as order_created_hour
    ,DATE_TRUNC(month, Date (LOCAL_TIME_CREATED_AT)) AS order_month 
    ,DATE_TRUNC(week, Date (LOCAL_TIME_CREATED_AT)) AS order_week  
    ,DAYNAME ( LOCAL_TIME_CREATED_AT) as week_of_Day
    ,CAST(EXTRACT(HOUR FROM TO_TIMESTAMP_NTZ(coalesce(LOCAL_TIME_TARGET_READY_AT, LOCAL_TIME_DELIVERED_AT,LOCAL_TIME_CREATED_AT) )) AS INT) as Delivered_hour_of_day --use this for TP
   ,TO_CHAR(DATE_TRUNC('second', LOCAL_TIME_CREATED_AT ), 'YYYY-MM-DD HH24:MI:SS') as local_time
   ,ZONE_CODE
   ,City_name
   ,picked_up_to_completed_at_raw_meters as total_route_distance
 
   from 
    PRODUCTION.denormalised.ORDERS as ordrs
     left join
    route_distance
    using (ID)
where  
    status ='DELIVERED'  
    and order_type != 'REDELIVERY'   
    and fulfillment_type='Deliveroo' 
    AND order_fulfillment = 'Deliveroo Rider' 
    and order_date between  dateadd('month', -7, date_trunc('month',  current_date())) and   LAST_DAY(dateadd('month', -1, date_trunc('month',  current_date())), MONTH)
   and  date (LOCAL_TIME_PREP_FOR)between   dateadd('month', -7, date_trunc('month',  current_date())) and   LAST_DAY(dateadd('month', -1, date_trunc('month',  current_date())), MONTH)
)

 , distinct_hour as
( 
select 
distinct Delivered_hour_of_day,
week_of_Day
from base
)

 ,master_table as
   (
   select 
   distinct 
   city_name
   ,zone_code
   ,order_month
   ,b.Delivered_hour_of_day 
   ,b.week_of_Day
   
   from base as a
   left join distinct_hour  as b
    on 1=1
   )
   
    , rolling_30_days as
    (
    select 
    a.zone_code as zone_code
    ,a.city_name as city_name
    ,a.order_month as order_month
    ,a.Delivered_hour_of_day  as delivered_hour
    ,a.week_of_Day as WOD
    ,b.zone_code as zone_code_b
    ,b.city_name as city_name_b
    ,b.order_dt as ordr_dt_b
    ,b.Delivered_hour_of_day as Delivered_hour_b
    ,b.week_of_Day as week_of_Day_b
    ,b.ID
    ,b.total_route_distance
   
     from master_table as a
     left join
     base as b
     on b.order_dt > DATEADD(day,-31, a.order_month)
     and b.order_dt <=DATEADD(day,-1, a.order_month)
     
     and a.Delivered_hour_of_day=b.Delivered_hour_of_day

     and a.zone_code=b.zone_code
     and a.city_name =b.city_name
     and a.week_of_Day=b.week_of_Day

    )


     select
      a.zone_code
     ,a.city_name
     ,a.order_month
     ,a.delivered_hour
     ,a.WOD   
     ,avg (total_route_distance)as avg_total_dist_last_30_dys
     ,PERCENTILE_CONT( 0.1) WITHIN GROUP (ORDER BY total_route_distance)as p10_total_dist_last_30_dys
     ,PERCENTILE_CONT( 0.2) WITHIN GROUP (ORDER BY total_route_distance)as p20_total_dist_last_30_dys
     ,PERCENTILE_CONT( 0.5) WITHIN GROUP (ORDER BY total_route_distance)as p50_total_dist_last_30_dys
     ,PERCENTILE_CONT( 0.7) WITHIN GROUP (ORDER BY total_route_distance)as p70_total_dist_last_30_dys
     ,PERCENTILE_CONT( 0.9) WITHIN GROUP (ORDER BY total_route_distance)as p90_total_dist_last_30_dys
     from rolling_30_days  as a
      where
      a.order_month between  dateadd('month', -6, date_trunc('month',  current_date())) and   LAST_DAY(dateadd('month', -1, date_trunc('month',  current_date())), MONTH) -- remove the data prior to this
       group by 1,2,3,4,5
     order by 1 desc;

