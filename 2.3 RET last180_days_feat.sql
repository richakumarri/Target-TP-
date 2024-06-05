
create  or replace table scratch.riders.last_180_dys_metrics as
with cuisine_flag as
(
select 
distinct 
restaurant_id
,lower(cuisine.cuisine_1)  AS cuisine
from 
PRODUCTION.reference.denormalised_restaurant_with_company AS resto_base
LEFT JOIN production.competitor.cuisine_classification_deliveroo_rx_id  AS cuisine
USING (restaurant_id)
)
, base as
(
   select
   ID
   ,case when ORDER_SCHEDULE='ASAP' then ID end as ASAP_ID
   ,date(LOCAL_TIME_CREATED_AT)as order_dt   
   ,CAST(EXTRACT(HOUR FROM TO_TIMESTAMP(LOCAL_TIME_CREATED_AT )) AS INT)as order_created_hour
   ,DATE_TRUNC(month, Date (LOCAL_TIME_CREATED_AT)) AS order_month 
   ,DATE_TRUNC(week, Date (LOCAL_TIME_CREATED_AT)) AS order_week  
   ,CAST(EXTRACT(HOUR FROM TO_TIMESTAMP_NTZ(coalesce(LOCAL_TIME_TARGET_READY_AT, LOCAL_TIME_DELIVERED_AT,LOCAL_TIME_CREATED_AT) )) AS INT) as Delivered_hour_of_day --use this for TP
   ,TO_CHAR(DATE_TRUNC('second', LOCAL_TIME_CREATED_AT ), 'YYYY-MM-DD HH24:MI:SS') as local_time
   ,case when Delivered_hour_of_day between 6 and 10 then 'Breakfast'
    when Delivered_hour_of_day between 10 and 14 then 'Lunch'
    when Delivered_hour_of_day between 14 and 17 then 'Interpeak'
    when Delivered_hour_of_day between 17 and 22 then'Dinner'
    when Delivered_hour_of_day in(23) or Delivered_hour_of_day between 0 and  6 then 'Late_night' end as shift  
   ,ZONE_CODE
   ,City_name
   ,LOCAL_TIME_CREATED_AT
   ,LOCAL_TIME_DELIVERED_AT
   ,ASAP_ORDER_DURATION as AOD 
   ,ASAP_ESTIMATED_ORDER_DURATION  as EOD
   ,LOCAL_TIME_RA_ACKNOWLEDGED_AT -- rider acknowldged order
   ,LOCAL_TIME_RA_CONFIRMED_AT -- rider reached_resto
   ,DATEDIFF(minute, LOCAL_TIME_RA_ACKNOWLEDGED_AT,LOCAL_TIME_RA_CONFIRMED_AT ) as rider_to_restaurant_mins
   ,LOCAL_TIME_TARGET_READY_AT -- food marked ready
   ,DATEDIFF(minute, LOCAL_TIME_SUBMITTED_AT,LOCAL_TIME_TARGET_READY_AT ) as FPT
   ,LOCAL_TIME_OA_RECEIVED_AT -- rider received order
   ,local_time_target_delivered_at
   ,LOCAL_TIME_DELIVERED_AT
   ,WAIT_AT_CUSTOMER
   ,WAIT_AT_RESTAURANT 
   ,DATEDIFF(minute, LOCAL_TIME_OA_RECEIVED_AT,LOCAL_TIME_DELIVERED_AT ) resto_customer_mins
   ,LATENESS -- (difference between EOD and AOD,ASAP_ESTIMATED_ORDER_DURATION-ASAP_ORDER_DURATION)
   ,case when LATENESS>=5 then 1 else 0 end as late_by_5_mins
   ,case when LATENESS>=10 then 1 else 0 end as late_by_10_mins
   ,case when LATENESS>=15 then 1 else 0 end as late_by_15_mins
   ,TEMPERATURE
   ,WIND_SPEED
   ,case when CROSS_ZONE_PICKUP = true then 1 else 0 end as cross_zone_pickup_flag
   ,case when cross_zone_delivery = true then 1 else 0 end as cross_zone_dlvry_flag
   ,ERAT
   ,case when lower(SF_GLOBAL_BRAND) like '%starbucks%' or SF_GLOBAL_BRAND like '%McDonald%' then SF_GLOBAL_BRAND
    when SF_GLOBAL_BRAND in ('Allo Beirut') then 'lebanese'
    when  SF_GLOBAL_BRAND in  ('UAE - Joe & The Juice') then 'breakfast & coffee' 
    else cuisine_flag.cuisine end as cuisine_updated
    
   from 
    PRODUCTION.denormalised.ORDERS as ordrs
     left join
    cuisine_flag
    using (restaurant_id)
where  
    status ='DELIVERED'  
    and order_type != 'REDELIVERY'   
    and fulfillment_type='Deliveroo' 
    AND order_fulfillment = 'Deliveroo Rider' 
     and order_date between  '2023-09-01' and   LAST_DAY(dateadd('month', -1, date_trunc('month',  current_date())), MONTH)
   and  date (LOCAL_TIME_PREP_FOR)between  '2023-09-01' and   LAST_DAY(dateadd('month', -1, date_trunc('month',  current_date())), MONTH)

    )
  


 ,master_table as
   (
   select 
   distinct 
   city_name
   ,zone_code
   ,order_month
   from base
   )
   
    , rolling_180_days as
    (
    select 
    a.zone_code as zone_code
    , a.city_name as city_name
    , a.order_month as order_month
    , b.zone_code as zone_code_b
    ,b.city_name as city_name_b
    , b.order_dt as ordr_dt_b
    ,b.ID
    ,b.AOD
    ,b.EOD
    ,b.ERAT
    ,b.Rider_to_restaurant_mins
    ,b.FPT
    ,b.wait_at_customer
    ,b.resto_customer_mins
    ,b.lateness
    ,b.late_by_5_mins
    ,b.late_by_10_mins
    ,b.late_by_15_mins
    ,b.temperature
    ,b.wind_speed
    ,b.cross_zone_pickup_flag
    ,b.cross_zone_dlvry_flag
    ,b.cuisine_updated
     from master_table as a
     left join
     base as b
     on b.order_dt > DATEADD(day,-181, a.order_month)
     and b.order_dt <=DATEADD(day,-1, a.order_month)

      and a.zone_code=b.zone_code
      and a.city_name =b.city_name

    )
    
  

    ,top_cuisine_180_dys_1 as
    (
    select
    zone_code
    ,city_name
    , order_month
    , CUISINE_UPDATED
    , count(distinct ID)as order_cnt
     from rolling_180_days
     group by 1,2,3,4
    )
    ,top_cuisine_180_dys_2 as
    (
    select
    *, row_number () over (partition by zone_code, city_name, order_month order by order_cnt desc)as rnk
     from 
     top_cuisine_180_dys_1
     qualify rnk <=3
    )
    ,top_cuisine_180_dys_3 as
    (
    select 
    distinct zone_code
    , city_name
    , order_month 
    , max(case when rnk =1 then cuisine_updated end) as top_1_cuisine
    , max(case when rnk =2 then cuisine_updated end) as top_2_cuisine
    , max(case when rnk =3 then cuisine_updated end) as top_3_cuisine
     
    from top_cuisine_180_dys_2 
    group by 1,2,3
    )
   
     select

     a.zone_code
     ,a.city_name
     ,a.order_month
     ,max(b.top_1_cuisine) as top_1_cuisine_last_180_dys
     ,max(b.top_2_cuisine) as top_2_cuisine_last_180_dys
     ,max(b.top_3_cuisine) as top_3_cuisine_last_180_dys
     
     ,count(distinct ID)as order_cnt_last_180_dys
     ,avg (AOD)as avg_AOD_last_180_dys
     ,PERCENTILE_CONT( 0.1) WITHIN GROUP (ORDER BY AOD)as p10_AOD_last_180_dys
     ,PERCENTILE_CONT( 0.2) WITHIN GROUP (ORDER BY AOD)as p20_AOD_last_180_dys
     ,PERCENTILE_CONT( 0.5) WITHIN GROUP (ORDER BY AOD)as p50_AOD_last_180_dys
     ,PERCENTILE_CONT( 0.7) WITHIN GROUP (ORDER BY AOD)as p70_AOD_last_180_dys
     ,PERCENTILE_CONT( 0.9) WITHIN GROUP (ORDER BY AOD)as p90_AOD_last_180_dys
  
     ,avg(EOD)as avg_EOD_last_180_dys
     ,PERCENTILE_CONT( 0.1) WITHIN GROUP (ORDER BY EOD)as p10_EOD_last_180_dys
     ,PERCENTILE_CONT( 0.2) WITHIN GROUP (ORDER BY EOD)as p20_EOD_last_180_dys
     ,PERCENTILE_CONT( 0.5) WITHIN GROUP (ORDER BY EOD)as p50_EOD_last_180_dys
     ,PERCENTILE_CONT( 0.7) WITHIN GROUP (ORDER BY EOD)as p70_EOD_last_180_dys
     ,PERCENTILE_CONT( 0.9) WITHIN GROUP (ORDER BY EOD)as p90_EOD_last_180_dys


      ,avg(ERAT)as avg_ERAT_last_180_dys
     ,PERCENTILE_CONT( 0.1) WITHIN GROUP (ORDER BY ERAT)as p10_ERAT_last_180_dys
     ,PERCENTILE_CONT( 0.2) WITHIN GROUP (ORDER BY ERAT)as p20_ERAT_last_180_dys
     ,PERCENTILE_CONT( 0.5) WITHIN GROUP (ORDER BY ERAT)as p50_ERAT_last_180_dys
     ,PERCENTILE_CONT( 0.7) WITHIN GROUP (ORDER BY ERAT)as p70_ERAT_last_180_dys
     ,PERCENTILE_CONT( 0.9) WITHIN GROUP (ORDER BY ERAT)as p90_ERAT_last_180_dys
      
     ,avg(RIDER_TO_RESTAURANT_MINS)as avg_RIDER_TO_RESTAURANT_MINS_last_180_dys
     ,PERCENTILE_CONT( 0.1) WITHIN GROUP (ORDER BY RIDER_TO_RESTAURANT_MINS)as p10_RIDER_TO_RESTAURANT_MINS_last_180_dys
     ,PERCENTILE_CONT( 0.2) WITHIN GROUP (ORDER BY RIDER_TO_RESTAURANT_MINS)as p20_RIDER_TO_RESTAURANT_MINS_last_180_dys
     ,PERCENTILE_CONT( 0.5) WITHIN GROUP (ORDER BY RIDER_TO_RESTAURANT_MINS)as p50_RIDER_TO_RESTAURANT_MINS_last_180_dys
     ,PERCENTILE_CONT( 0.7) WITHIN GROUP (ORDER BY RIDER_TO_RESTAURANT_MINS)as p70_RIDER_TO_RESTAURANT_MINS_last_180_dys
     ,PERCENTILE_CONT( 0.9) WITHIN GROUP (ORDER BY RIDER_TO_RESTAURANT_MINS)as p90_RIDER_TO_RESTAURANT_MINS_last_180_dys

     ,avg(FPT)as avg_FPT_last_180_dys
     ,PERCENTILE_CONT( 0.1) WITHIN GROUP (ORDER BY FPT)as p10_FPT_last_180_dys
     ,PERCENTILE_CONT( 0.2) WITHIN GROUP (ORDER BY FPT)as p20_FPT_last_180_dys
     ,PERCENTILE_CONT( 0.5) WITHIN GROUP (ORDER BY FPT)as p50_FPT_last_180_dys
     ,PERCENTILE_CONT( 0.7) WITHIN GROUP (ORDER BY FPT)as p70_FPT_last_180_dys
     ,PERCENTILE_CONT( 0.9) WITHIN GROUP (ORDER BY FPT)as p90_FPT_last_180_dys

     ,avg(RESTO_CUSTOMER_MINS) as avg_RESTO_CUSTOMER_MINS_last_180_dys
     ,PERCENTILE_CONT( 0.1) WITHIN GROUP (ORDER BY RESTO_CUSTOMER_MINS)as p10_RESTO_CUSTOMER_MINS_last_180_dys
     ,PERCENTILE_CONT( 0.2) WITHIN GROUP (ORDER BY RESTO_CUSTOMER_MINS)as p20_RESTO_CUSTOMER_MINS_last_180_dys
     ,PERCENTILE_CONT( 0.5) WITHIN GROUP (ORDER BY RESTO_CUSTOMER_MINS)as p50_RESTO_CUSTOMER_MINS_last_180_dys
     ,PERCENTILE_CONT( 0.7) WITHIN GROUP (ORDER BY RESTO_CUSTOMER_MINS)as p70_RESTO_CUSTOMER_MINS_last_180_dys
     ,PERCENTILE_CONT( 0.9) WITHIN GROUP (ORDER BY RESTO_CUSTOMER_MINS)as p90_RESTO_CUSTOMER_MINS_last_180_dys
 
     ,avg(WAIT_AT_CUSTOMER)as avg_WAIT_AT_CUSTOMER_last_180_dys
     ,avg(LATENESS)as avg_lateness_last_180_dys
     ,sum(LATE_BY_5_MINS)/ count(distinct ID)as late_by_5_mins_perc_last_180_dys
     ,sum(LATE_BY_10_MINS)/ count(distinct ID)as late_by_10_mins_perc_last_180_dys
     ,sum(LATE_BY_15_MINS)/ count(distinct ID)as late_by_15_mins_perc_last_180_dys
     ,avg(TEMPERATURE)as avg_TEMPERATURE_last_180_dys
     ,avg(WIND_SPEED)as avg_WIND_SPEED_last_180_dys
     ,sum (CROSS_ZONE_PICKUP_FLAG)/ count(distinct ID)as perc_cross_zone_pickup_last_180_dys
     ,sum (CROSS_ZONE_DLVRY_FLAG)/ count(distinct ID)as perc_cross_zone_dlvry_last_180_dys
     
     
     from rolling_180_days  as a
     left join top_cuisine_180_dys_3 as b
          on a.zone_code=b.zone_code
          and a.city_name =b.city_name
          and a.order_month=b.order_month
      where
      a.order_month between  dateadd('month', -6, date_trunc('month',  current_date())) and   LAST_DAY(dateadd('month', -1, date_trunc('month',  current_date())), MONTH) -- remove the data prior to this
    
      group by 1,2,3
      order by 1 desc;



      -- select * from scratch.riders.last_180_dys_metrics  where zone_code='KCE' and order_month='2024-03-01'
