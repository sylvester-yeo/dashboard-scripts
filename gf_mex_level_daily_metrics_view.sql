create view slide.gf_mex_level_daily_metrics_view as 
select 
    date_local
    ,country_name
    ,city_name
    ,merchant_id
    ,restaurant_partner_status
    ,business_model
    ,cashless_status
    ,payment_type
    ,time_from_order_create_to_completed
    ,completed_orders_without_null_time_2
    ,all_incoming_orders_gf
    ,mex_accepted_orders_gf
    ,completed_orders_gf
    ,delivery_distance_gf
    ,driver_distance_gf
    ,delivery_fare_gf
    ,delivery_fare_gf_local
    ,dax_delivery_fare
    ,dax_delivery_fare_local
    ,gmv_usd_gf
    ,gmv_local
    ,basket_size
    ,basket_size_local
    ,sub_total
    ,sub_total_local
    ,mex_commission
    ,mex_commission_local
    ,base_for_mex_commission_local
    ,base_for_mex_commission
    ,driver_commission
    ,driver_commission_local
    ,promo_expense
    ,promo_expense_local
    ,promo_code_expense
    ,promo_code_expense_local
    ,promo_incoming_orders
    ,promo_completed_orders
    ,cancellations
    ,cancellations_passenger
    ,cancellations_driver
    ,cancellations_operator
    ,cancellations_merchant
    ,allocated_orders
    ,unallocated_orders
    ,pre_accept_cancellations
    ,pre_accept_cancellations_pax
    ,pre_accept_cancellations_operator
    ,pre_accept_expired_orders
    ,pre_allocation_cancellations
    ,pre_allocation_cancellations_pax
    ,pre_allocation_cancellations_operator
    ,first_allocated_orders
    ,effective_first_allocated_orders
    ,tsp_subsidy_local
    ,tsp_subsidy_usd
    ,incentives_local
    ,incentives_usd
    ,spot_incentive_bonus_local
    ,spot_incentive_bonus_usd
    ,sof_local
    ,sof_usd
    ,convenience_fee_local
    ,convenience_fee_usd
    ,pax_platform_fee_local
    ,pax_platform_fee_usd
    ,total_takeaway_orders
    ,total_takeaway_completed_orders
    ,takeaway_gmv_local
    ,takeaway_gmv_usd
    ,takeaway_mex_commission_local
    ,takeaway_mex_commission_usd
    ,takeaway_base_for_mex_commission_local
    ,takeaway_base_for_mex_commission
    ,takeaway_basket_size_usd
    ,takeaway_basket_size_local
    ,takeaway_sub_total_usd
    ,takeaway_sub_total_local
    ,takeaway_time_from_order_create_to_completed
    ,tips_local
    ,tips_usd
    ,total_scheduled_orders
    ,total_scheduled_completed_orders
    ,scheduled_gmv_local
    ,scheduled_gmv_usd
    ,scheduled_mex_commission
    ,scheduled_mex_commission_local
    ,scheduled_base_for_mex_commission_local
    ,scheduled_base_for_mex_commission
    ,scheduled_basket_size_usd
    ,scheduled_basket_size_local
    ,scheduled_sub_total_usd
    ,scheduled_sub_total_local
    ,scheduled_total_date_diff
    ,jobs_accepted
    ,jobs_received
    ,jobs_unread
    ,partition_date_local

from slide.gf_mex_level_daily_metrics_2020
where partition_date_local >= date('2020-01-01')

union all 

(select 
    date_local
    ,country_name
    ,city_name
    ,merchant_id
    ,restaurant_partner_status
    ,business_model
    ,cashless_status
    ,payment_type
    ,time_from_order_create_to_completed
    ,completed_orders_without_null_time_2
    ,all_incoming_orders_gf
    ,mex_accepted_orders_gf
    ,completed_orders_gf
    ,delivery_distance_gf
    ,driver_distance_gf
    ,delivery_fare_gf
    ,delivery_fare_gf_local
    ,dax_delivery_fare
    ,dax_delivery_fare_local
    ,gmv_usd_gf
    ,gmv_local
    ,basket_size
    ,basket_size_local
    ,sub_total
    ,sub_total_local
    ,mex_commission
    ,mex_commission_local
    ,base_for_mex_commission_local
    ,base_for_mex_commission
    ,driver_commission
    ,driver_commission_local
    ,promo_expense
    ,promo_expense_local
    ,promo_code_expense
    ,promo_code_expense_local
    ,promo_incoming_orders
    ,promo_completed_orders
    ,cancellations
    ,cancellations_passenger
    ,cancellations_driver
    ,cancellations_operator
    ,cancellations_merchant
    ,allocated_orders
    ,unallocated_orders
    ,pre_accept_cancellations
    ,pre_accept_cancellations_pax
    ,pre_accept_cancellations_operator
    ,pre_accept_expired_orders
    ,pre_allocation_cancellations
    ,pre_allocation_cancellations_pax
    ,pre_allocation_cancellations_operator
    ,first_allocated_orders
    ,effective_first_allocated_orders
    ,tsp_subsidy_local
    ,tsp_subsidy_usd
    ,incentives_local
    ,incentives_usd
    ,spot_incentive_bonus_local
    ,spot_incentive_bonus_usd
    ,sof_local
    ,sof_usd
    ,0 as convenience_fee_local
    ,0 as convenience_fee_usd
    ,0 as pax_platform_fee_local
    ,0 as pax_platform_fee_usd
    ,total_takeaway_orders
    ,total_takeaway_completed_orders
    ,takeaway_gmv_local
    ,takeaway_gmv_usd
    ,takeaway_mex_commission_local
    ,takeaway_mex_commission_usd
    ,takeaway_base_for_mex_commission_local
    ,takeaway_base_for_mex_commission
    ,takeaway_basket_size_usd
    ,takeaway_basket_size_local
    ,takeaway_sub_total_usd
    ,takeaway_sub_total_local
    ,takeaway_time_from_order_create_to_completed
    ,tips_local
    ,tips_usd
    ,total_scheduled_orders
    ,total_scheduled_completed_orders
    ,scheduled_gmv_local
    ,scheduled_gmv_usd
    ,scheduled_mex_commission
    ,scheduled_mex_commission_local
    ,scheduled_base_for_mex_commission_local
    ,scheduled_base_for_mex_commission
    ,scheduled_basket_size_usd
    ,scheduled_basket_size_local
    ,scheduled_sub_total_usd
    ,scheduled_sub_total_local
    ,scheduled_total_date_diff
    ,jobs_accepted
    ,jobs_received
    ,jobs_unread
    ,partition_date_local
from slide.gf_mex_level_daily_metrics_2019
where partition_date_local < date('2020-01-01'))
