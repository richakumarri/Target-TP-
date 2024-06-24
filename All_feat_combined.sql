create  or replace table scratch.riders.RET_all_feat_24_06 as
select
base.*
,lst_30_days.*exclude (zone_code, city_name, order_month)
,lst_90_days.*exclude(zone_code, city_name, order_month)
,lst_180_days.*exclude(zone_code, city_name, order_month)
,lst_30_dys_same_hr.*exclude(zone_code, city_name, order_month,delivered_hour)
,lst_90_dys_same_hr.*exclude(zone_code, city_name, order_month,delivered_hour)
,last_180_dys_same_hr.*exclude(zone_code, city_name, order_month,delivered_hour)
,last_30_dys_same_hr_WOD.*exclude(zone_code, city_name, order_month,delivered_hour,WOD)
,last_90_dys_same_hr_WOD.*exclude(zone_code, city_name, order_month,delivered_hour,WOD)
,last_180_dys_same_hr_wod.*exclude(zone_code, city_name, order_month,delivered_hour,WOD)
,last_21_dys_metrics.*exclude (zone_code, city_name, order_month)
,last_14_dys_metrics.*exclude (zone_code, city_name, order_month)
,last_7_dys_metrics.*exclude (zone_code, city_name, order_month)
,last_5_dys_metrics.*exclude (zone_code, city_name, order_month)
,last_3_dys_metrics.*exclude (zone_code, city_name, order_month)
,last_21_dys_same_hr_same_hour.*exclude(zone_code, city_name, order_month,delivered_hour)
,last_14_dys_same_hr_same_hour.*exclude(zone_code, city_name, order_month,delivered_hour)
,last_7_dys_same_hr_same_hour.*exclude(zone_code, city_name, order_month,delivered_hour)
,last_5_dys_same_hr.*exclude(zone_code, city_name, order_month,delivered_hour)
,last_3_dys_same_hr.*exclude(zone_code, city_name, order_month,delivered_hour)
,last_21_dys_same_hr_WOD.*exclude(zone_code, city_name, order_month,delivered_hour,WOD)
,last_14_dys_same_hr_WOD.*exclude(zone_code, city_name, order_month,delivered_hour,WOD)
,last_7_dys_same_hr_WOD.*exclude(zone_code, city_name, order_month,delivered_hour,WOD)
,last_5_dys_same_hr_WOD.*exclude(zone_code, city_name, order_month,delivered_hour,WOD)
,last_3_dys_same_hr_WOD.*exclude(zone_code, city_name, order_month,delivered_hour,WOD)
,avg_route_distance_last_30_dys.*exclude(zone_code, city_name, order_month,delivered_hour,WOD)

from scratch.riders.TP_y_variable  as base

left join
scratch.riders.last_30_dys_metrics as lst_30_days 
on base.ZONE_CODE    =lst_30_days.ZONE_CODE
and base.CITY_NAME   =lst_30_days.CITY_NAME
and base.ORDER_MONTH =lst_30_days.ORDER_MONTH

left join
scratch.riders.last_90_dys_metrics as lst_90_days 
on base.ZONE_CODE    =lst_90_days.ZONE_CODE
and base.CITY_NAME   =lst_90_days.CITY_NAME
and base.ORDER_MONTH =lst_90_days.ORDER_MONTH

left join
scratch.riders.last_180_dys_metrics as lst_180_days 
on base.ZONE_CODE    =lst_180_days.ZONE_CODE
and base.CITY_NAME   =lst_180_days.CITY_NAME
and base.ORDER_MONTH =lst_180_days.ORDER_MONTH

left join
scratch.riders.last_30_dys_same_hr_same_hour as lst_30_dys_same_hr
on base.ZONE_CODE    =lst_30_dys_same_hr.ZONE_CODE
and base.CITY_NAME   =lst_30_dys_same_hr.CITY_NAME
and base.ORDER_MONTH =lst_30_dys_same_hr.ORDER_MONTH
and base.HOUR_OF_DAY =lst_30_dys_same_hr.delivered_hour

left join
scratch.riders.last_90_dys_same_hr as lst_90_dys_same_hr
on base.ZONE_CODE    =lst_90_dys_same_hr.ZONE_CODE
and base.CITY_NAME   =lst_90_dys_same_hr.CITY_NAME
and base.ORDER_MONTH =lst_90_dys_same_hr.ORDER_MONTH
and base.HOUR_OF_DAY =lst_90_dys_same_hr.delivered_hour

left join
scratch.riders.last_180_dys_same_hr as last_180_dys_same_hr
on base.ZONE_CODE    =last_180_dys_same_hr.ZONE_CODE
and base.CITY_NAME   =last_180_dys_same_hr.CITY_NAME
and base.ORDER_MONTH =last_180_dys_same_hr.ORDER_MONTH
and base.HOUR_OF_DAY =last_180_dys_same_hr.delivered_hour

left join
scratch.riders.last_30_dys_same_hr_WOD as last_30_dys_same_hr_WOD
on base.ZONE_CODE    =last_30_dys_same_hr_WOD.ZONE_CODE
and base.CITY_NAME   =last_30_dys_same_hr_WOD.CITY_NAME
and base.ORDER_MONTH =last_30_dys_same_hr_WOD.ORDER_MONTH
and base.HOUR_OF_DAY =last_30_dys_same_hr_WOD.delivered_hour
and base.WEEK_OF_DAY =last_30_dys_same_hr_WOD.WOD

left join
scratch.riders.last_90_dys_same_hr_WOD as last_90_dys_same_hr_WOD
on base.ZONE_CODE    =last_90_dys_same_hr_WOD.ZONE_CODE
and base.CITY_NAME   =last_90_dys_same_hr_WOD.CITY_NAME
and base.ORDER_MONTH =last_90_dys_same_hr_WOD.ORDER_MONTH
and base.HOUR_OF_DAY =last_90_dys_same_hr_WOD.delivered_hour
and base.WEEK_OF_DAY =last_90_dys_same_hr_WOD.WOD

left join
scratch.riders.last_180_dys_same_hr_wod as last_180_dys_same_hr_wod
on base.ZONE_CODE    =last_180_dys_same_hr_wod.ZONE_CODE
and base.CITY_NAME   =last_180_dys_same_hr_wod.CITY_NAME
and base.ORDER_MONTH =last_180_dys_same_hr_wod.ORDER_MONTH
and base.HOUR_OF_DAY =last_180_dys_same_hr_wod.delivered_hour
and base.WEEK_OF_DAY =last_180_dys_same_hr_wod.WOD

left join
scratch.riders.last_21_dys_metrics as last_21_dys_metrics
on base.ZONE_CODE    =last_21_dys_metrics.ZONE_CODE
and base.CITY_NAME   =last_21_dys_metrics.CITY_NAME
and base.ORDER_MONTH =last_21_dys_metrics.ORDER_MONTH

left join
scratch.riders.last_14_dys_metrics as last_14_dys_metrics
on base.ZONE_CODE    =last_14_dys_metrics.ZONE_CODE
and base.CITY_NAME   =last_14_dys_metrics.CITY_NAME
and base.ORDER_MONTH =last_14_dys_metrics.ORDER_MONTH

left join
scratch.riders.last_7_dys_metrics as last_7_dys_metrics
on base.ZONE_CODE    =last_7_dys_metrics.ZONE_CODE
and base.CITY_NAME   =last_7_dys_metrics.CITY_NAME
and base.ORDER_MONTH =last_7_dys_metrics.ORDER_MONTH

left join
scratch.riders.last_5_dys_metrics as last_5_dys_metrics
on base.ZONE_CODE    =last_5_dys_metrics.ZONE_CODE
and base.CITY_NAME   =last_5_dys_metrics.CITY_NAME
and base.ORDER_MONTH =last_5_dys_metrics.ORDER_MONTH

left join
scratch.riders.last_3_dys_metrics as last_3_dys_metrics
on base.ZONE_CODE    =last_3_dys_metrics.ZONE_CODE
and base.CITY_NAME   =last_3_dys_metrics.CITY_NAME
and base.ORDER_MONTH =last_3_dys_metrics.ORDER_MONTH

left join
scratch.riders.last_21_dys_same_hr_same_hour as last_21_dys_same_hr_same_hour
on base.ZONE_CODE    =last_21_dys_same_hr_same_hour.ZONE_CODE
and base.CITY_NAME   =last_21_dys_same_hr_same_hour.CITY_NAME
and base.ORDER_MONTH =last_21_dys_same_hr_same_hour.ORDER_MONTH
and base.HOUR_OF_DAY =last_21_dys_same_hr_same_hour.delivered_hour

left join
scratch.riders.last_14_dys_same_hr_same_hour as last_14_dys_same_hr_same_hour
on base.ZONE_CODE    =last_14_dys_same_hr_same_hour.ZONE_CODE
and base.CITY_NAME   =last_14_dys_same_hr_same_hour.CITY_NAME
and base.ORDER_MONTH =last_14_dys_same_hr_same_hour.ORDER_MONTH
and base.HOUR_OF_DAY =last_14_dys_same_hr_same_hour.delivered_hour

left join
scratch.riders.last_7_dys_same_hr_same_hour as last_7_dys_same_hr_same_hour
on base.ZONE_CODE    =last_7_dys_same_hr_same_hour.ZONE_CODE
and base.CITY_NAME   =last_7_dys_same_hr_same_hour.CITY_NAME
and base.ORDER_MONTH =last_7_dys_same_hr_same_hour.ORDER_MONTH
and base.HOUR_OF_DAY =last_7_dys_same_hr_same_hour.delivered_hour

left join
scratch.riders.last_5_dys_same_hr as last_5_dys_same_hr
on base.ZONE_CODE    =last_5_dys_same_hr.ZONE_CODE
and base.CITY_NAME   =last_5_dys_same_hr.CITY_NAME
and base.ORDER_MONTH =last_5_dys_same_hr.ORDER_MONTH
and base.HOUR_OF_DAY =last_5_dys_same_hr.delivered_hour


left join
scratch.riders.last_3_dys_same_hr as last_3_dys_same_hr
on base.ZONE_CODE    =last_3_dys_same_hr.ZONE_CODE
and base.CITY_NAME   =last_3_dys_same_hr.CITY_NAME
and base.ORDER_MONTH =last_3_dys_same_hr.ORDER_MONTH
and base.HOUR_OF_DAY =last_3_dys_same_hr.delivered_hour

left join
scratch.riders.last_21_dys_same_hr_WOD as last_21_dys_same_hr_WOD
on base.ZONE_CODE    =last_21_dys_same_hr_WOD.ZONE_CODE
and base.CITY_NAME   =last_21_dys_same_hr_WOD.CITY_NAME
and base.ORDER_MONTH =last_21_dys_same_hr_WOD.ORDER_MONTH
and base.HOUR_OF_DAY =last_21_dys_same_hr_WOD.delivered_hour
and base.WEEK_OF_DAY =last_21_dys_same_hr_WOD.WOD

left join
scratch.riders.last_14_dys_same_hr_WOD as last_14_dys_same_hr_WOD
on base.ZONE_CODE    =last_14_dys_same_hr_WOD.ZONE_CODE
and base.CITY_NAME   =last_14_dys_same_hr_WOD.CITY_NAME
and base.ORDER_MONTH =last_14_dys_same_hr_WOD.ORDER_MONTH
and base.HOUR_OF_DAY =last_14_dys_same_hr_WOD.delivered_hour
and base.WEEK_OF_DAY =last_14_dys_same_hr_WOD.WOD

left join
scratch.riders.last_7_dys_same_hr_WOD as last_7_dys_same_hr_WOD
on base.ZONE_CODE    =last_7_dys_same_hr_WOD.ZONE_CODE
and base.CITY_NAME   =last_7_dys_same_hr_WOD.CITY_NAME
and base.ORDER_MONTH =last_7_dys_same_hr_WOD.ORDER_MONTH
and base.HOUR_OF_DAY =last_7_dys_same_hr_WOD.delivered_hour
and base.WEEK_OF_DAY =last_7_dys_same_hr_WOD.WOD

left join
scratch.riders.last_5_dys_same_hr_WOD as last_5_dys_same_hr_WOD
on base.ZONE_CODE    =last_5_dys_same_hr_WOD.ZONE_CODE
and base.CITY_NAME   =last_5_dys_same_hr_WOD.CITY_NAME
and base.ORDER_MONTH =last_5_dys_same_hr_WOD.ORDER_MONTH
and base.HOUR_OF_DAY =last_5_dys_same_hr_WOD.delivered_hour
and base.WEEK_OF_DAY =last_5_dys_same_hr_WOD.WOD

left join
scratch.riders.last_3_dys_same_hr_WOD as last_3_dys_same_hr_WOD
on base.ZONE_CODE    =last_3_dys_same_hr_WOD.ZONE_CODE
and base.CITY_NAME   =last_3_dys_same_hr_WOD.CITY_NAME
and base.ORDER_MONTH =last_3_dys_same_hr_WOD.ORDER_MONTH
and base.HOUR_OF_DAY =last_3_dys_same_hr_WOD.delivered_hour
and base.WEEK_OF_DAY =last_3_dys_same_hr_WOD.WOD
 
left join
scratch.riders.avg_route_distance_last_30_dys as avg_route_distance_last_30_dys
on base.ZONE_CODE    =avg_route_distance_last_30_dys.ZONE_CODE
and base.CITY_NAME   =avg_route_distance_last_30_dys.CITY_NAME
and base.ORDER_MONTH =avg_route_distance_last_30_dys.ORDER_MONTH
and base.HOUR_OF_DAY =avg_route_distance_last_30_dys.delivered_hour
and base.WEEK_OF_DAY =avg_route_distance_last_30_dys.WOD;

