/*
    Name: slide.gf_dash_bd
    Refresh Time: Daily, 21:00 UTC
    Aggregation Mode: Overwrite
    Lighthouse Dependancy Tables:
        slide.gf_mex_level_daily_metrics 21:00
        slide.gf_mfc_brand 21:00
        slide.gf_mfp_merchant 21:00

*/

with mex_con as (
  select *
  from
    (
      select
        merchant_id
        ,partner
        ,date(valid_from) as date_mex_snapshots
        ,valid_from
        ,row_number() over (partition by merchant_id, date(valid_from) order by valid_from asc) as row_num
      from
        snapshots.food_data_service_merchant_contracts
      where
        date(concat(year,'-',month,'-',day)) >= date('2019-06-20')
        and date(concat(year,'-',month,'-',day)) >= date([[inc_start_date]])
        and date(concat(year,'-',month,'-',day)) <= date([[inc_end_date]])
    )
    where row_num = 1
    union all (
    select * from (
        select 
            id as merchant_id
            ,case when json_extract_scalar(contract,'$.partner') = '1' then true else false end as partner
            ,date(valid_from + interval '1' second) as date_mex_snapshots
            ,valid_from
            ,row_number() over (partition by id, date(valid_from + interval '1' second) order by valid_from + interval '1' second asc) as row_num
        from snapshots.grab_mall_grab_mall_seller
        where date(concat(year,'-',month,'-',day)) < date('2019-06-20')
	        and date(concat(year,'-',month,'-',day)) >= date([[inc_start_date]])
            and date(concat(year,'-',month,'-',day)) <= date([[inc_end_date]])
        )
    where row_num = 1
    )
)
,mex as (
    select *
    from
    (
      select
        merchant_id
        ,model_type
        ,date(valid_from) as date_mex_snapshots
        ,valid_from
        --,chain_name as brand_name
        ,row_number() over (partition by merchant_id, date(valid_from) order by valid_from asc) as row_num
      from
        snapshots.food_data_service_merchants
      where
        date(concat(year,'-',month,'-',day)) >= date('2019-06-20')
        and date(concat(year,'-',month,'-',day)) >= date([[inc_start_date]])
        and date(concat(year,'-',month,'-',day)) <= date([[inc_end_date]])
    )
    where row_num = 1
    union all 
    select * from (
  	    select
            id as merchant_id
            ,model_type
            ,date(valid_from + interval '1' second) as date_mex_snapshots
            ,valid_from
            ,row_number() over (partition by id, date(valid_from + interval '1' second) order by valid_from + interval '1' second asc) as row_num
        from snapshots.grab_mall_grab_mall_seller
        where
            date(concat(year,'-',month,'-',day)) < date('2019-06-20')
            and date(concat(year,'-',month,'-',day)) >= date([[inc_start_date]])
            and date(concat(year,'-',month,'-',day)) <= date([[inc_end_date]])
    )
  where row_num = 1
)
,mex_snapshots as (
  select
    mex.merchant_id
    ,mex_dim.merchant_name as merchant_name
    ,mex_dim.business_name as business_name
    ,mex_dim.bd_date
    ,mex_dim.is_bd_account as bd_account_flag 
    ,mex_dim.is_bd_partner as bd_partner_flag
    --,mex.brand_name
    ,mex.date_mex_snapshots
    ,mex.model_type as original_model_type
    ,mex_con.partner as partner_status
  from mex
  left join mex_con
    on mex.merchant_id = mex_con.merchant_id
    and mex.date_mex_snapshots = mex_con.date_mex_snapshots
  left join datamart.dim_merchants mex_dim /*change #1*/
	 ON mex.merchant_id = mex_dim.merchant_id
)
,orders as (
  SELECT
    a.date_local
    ,a.merchant_id
    ,b.merchant_name as merchant_name
    ,b.business_name as business_name
    ,a.country_name
    ,a.city_name
    -- ,b.bd_date as activation_date
    -- ,b.bd_account_flag
    -- ,b.bd_partner_flag
    ,CASE WHEN b.partner_status = TRUE THEN 1 ELSE 0 END AS partner_status
    ,(CASE WHEN b.original_model_type = 1 THEN 'Integrated' ELSE 'Concierge' END) AS business_model

    /*general metrics*/
    ,sum(a.gmv_usd_gf) as gmv_usd
    ,sum(a.gmv_local) as gmv_local
    ,sum(a.basket_size) as basket_size_usd
    ,sum(a.basket_size_local) as basket_size_local
    ,sum(a.sub_total) as sub_total_usd
    ,sum(a.sub_total_local) as sub_total_local
    ,sum(a.all_incoming_orders_gf) as all_incoming_orders
    ,sum(a.completed_orders_gf) as completed_orders
    ,sum(a.allocated_orders) as allocated_orders
    ,sum(a.unallocated_orders) as unallocated_orders
    -- ,sum(a.completed_orders_gf_item) as completed_orders_gf_item
    ,sum(a.mex_commission) as mex_commission_usd
    ,sum(a.mex_commission_local) as mex_commission_local
    ,sum(a.delivery_fare_gf) as delivery_fare_usd
    ,sum(a.delivery_fare_gf_local) as delivery_fare_local
    ,sum(a.dax_delivery_fare) as dax_delivery_fare_usd
    ,sum(a.dax_delivery_fare_local) as dax_delivery_fare_local
    ,sum(a.driver_commission) as driver_commission_usd
    ,sum(a.driver_commission_local) as driver_commission_local_usd
    ,sum(a.cancellations) as total_cancellations
    ,sum(a.cancellations_passenger) as total_pax_cancellations
    ,sum(a.cancellations_driver) as total_dax_cancellations
    ,sum(a.cancellations_operator) as total_operator_cancellations
    ,sum(a.cancellations_merchant) as total_mex_cancellations
    ,sum(a.incentives_local) as incentives_local
    ,sum(a.incentives_usd) as incentives_usd
    ,sum(a.spot_incentive_bonus_local) as spot_incentive_bonus_local
    ,sum(a.spot_incentive_bonus_usd) as spot_incentive_bonus_usd
    ,sum(a.tsp_subsidy_local) as tsp_subsidy_local
    ,sum(a.tsp_subsidy_usd) as tsp_subsidy_usd
    ,sum(a.sof_local) as sof_local
    ,sum(a.sof_usd) as sof_usd
    ,sum(a.convenience_fee_local) as convenience_fee_local
    ,sum(a.convenience_fee_usd) as convenience_fee_usd
    ,sum(a.pax_platform_fee_usd) as pax_platform_fee_usd
    ,sum(a.pax_platform_fee_local) as pax_platform_fee_local
    ,sum(a.promo_expense) as promo_expense_usd
    ,sum(a.promo_expense_local) as promo_expense_local
    
    ,sum(a.promo_incoming_orders) as promo_incoming_orders
    ,sum(a.promo_completed_orders) as promo_completed_orders
    ,sum(a.pre_accept_cancellations) as pre_accept_cancellations
    ,sum(a.pre_accept_cancellations_pax) as pre_accept_cancellations_pax
    ,sum(a.pre_accept_cancellations_operator) as pre_accept_cancellations_operator
    ,sum(a.pre_allocation_cancellations) as pre_allocation_cancellations
    ,sum(a.pre_allocation_cancellations_pax) as pre_allocation_cancellations_pax
    ,sum(a.pre_allocation_cancellations_operator) as pre_allocation_cancellations_operator
    ,sum(a.pre_accept_expired_orders) as pre_accept_expired_orders
    ,sum(a.time_from_order_create_to_completed) as delivery_time
    ,sum(a.completed_orders_without_null_time_2) as completed_orders_with_delivery_time
    

    /*new addition*/
    ,sum(a.promo_code_expense) as promo_code_expense_usd
    ,sum(a.promo_code_expense_local) as promo_code_expense_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.promo_code_expense END) as total_partner_promo_code_expense_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.promo_code_expense_local END) as total_partner_promo_code_expense_local
    ,sum(a.base_for_mex_commission) as base_for_mex_commission
    ,sum(a.base_for_mex_commission_local) as base_for_mex_commission_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.base_for_mex_commission end) as total_partner_base_for_mex_commission
    ,sum(case when a.restaurant_partner_status = 'partner' then a.base_for_mex_commission_local end) as total_partner_base_for_mex_commission_local


    /*case for total partner metrics*/
    ,sum(case when a.restaurant_partner_status = 'partner' then a.gmv_usd_gf END) as total_partner_gmv_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.gmv_local END) as total_partner_gmv_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.basket_size END) as total_partner_basket_size_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.basket_size_local END) as total_partner_basket_size_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.sub_total END) as total_partner_sub_total_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.sub_total_local END) as total_partner_sub_total_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.all_incoming_orders_gf END) as total_partner_all_incoming_orders
    ,sum(case when a.restaurant_partner_status = 'partner' then a.completed_orders_gf END) as total_partner_completed_orders
    ,sum(case when a.restaurant_partner_status = 'partner' then a.allocated_orders END) as total_partner_allocated_orders
    ,sum(case when a.restaurant_partner_status = 'partner' then a.unallocated_orders END) as total_partner_unallocated_orders
    -- ,sum(case when a.restaurant_partner_status = 'partner' then a.completed_orders_gf_item END) as total_partner_completed_orders_gf_item
    ,sum(case when a.restaurant_partner_status = 'partner' then a.mex_commission END) as total_partner_mex_commission_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.mex_commission_local END) as total_partner_mex_commission_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.delivery_fare_gf END) as total_partner_delivery_fare_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.delivery_fare_gf_local END) as total_partner_delivery_fare_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.dax_delivery_fare END) as total_partner_dax_delivery_fare_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.dax_delivery_fare_local END) as total_partner_dax_delivery_fare_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.driver_commission END) as total_partner_driver_commission_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.driver_commission_local END) as total_partner_driver_commission_local_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.cancellations END) as total_partner_total_cancellations
    ,sum(case when a.restaurant_partner_status = 'partner' then a.cancellations_passenger END) as total_partner_total_pax_cancellations
    ,sum(case when a.restaurant_partner_status = 'partner' then a.cancellations_driver END) as total_partner_total_dax_cancellations
    ,sum(case when a.restaurant_partner_status = 'partner' then a.cancellations_operator END) as total_partner_total_operator_cancellations
    ,sum(case when a.restaurant_partner_status = 'partner' then a.cancellations_merchant END) as total_partner_total_mex_cancellations
    ,sum(case when a.restaurant_partner_status = 'partner' then a.incentives_local END) as total_partner_incentives_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.incentives_usd END) as total_partner_incentives_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.tsp_subsidy_local END) as total_partner_tsp_subsidy_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.tsp_subsidy_usd END) as total_partner_tsp_subsidy_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.sof_local END) as total_partner_sof_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.sof_usd END) as total_partner_sof_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.spot_incentive_bonus_local END) as total_partner_spot_incentive_bonus_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.spot_incentive_bonus_usd END) as total_partner_spot_incentive_bonus_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.promo_expense END) as total_partner_promo_expense_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.promo_expense_local END) as total_partner_promo_expense_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.promo_incoming_orders END) as total_partner_promo_incoming_orders
    ,sum(case when a.restaurant_partner_status = 'partner' then a.promo_completed_orders END) as total_partner_promo_completed_orders
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_accept_cancellations END) as total_partner_pre_accept_cancellations
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_accept_cancellations_pax END) as total_partner_pre_accept_cancellations_pax
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_accept_cancellations_operator END) as total_partner_pre_accept_cancellations_operator
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_allocation_cancellations END) as total_partner_pre_allocation_cancellations
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_allocation_cancellations_pax END) as total_partner_pre_allocation_cancellations_pax
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_allocation_cancellations_operator END) as total_partner_pre_allocation_cancellations_operator
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_accept_expired_orders END) as total_partner_pre_accept_expired_orders
    ,sum(case when a.restaurant_partner_status = 'partner' then a.time_from_order_create_to_completed END) as total_partner_delivery_time
    ,sum(case when a.restaurant_partner_status = 'partner' then a.completed_orders_without_null_time_2 END) as total_partner_completed_orders_with_delivery_time

    /*specific metrics*/
    ,sum(case when a.business_model = 'Integrated' then gmv_usd_gf else 0 end) as im_gmv_usd
    ,sum(case when a.business_model = 'Integrated' then gmv_local else 0 end) as im_gmv_local
    ,sum(case when a.business_model = 'Integrated' then a.basket_size else 0 end) as im_basket_size
    ,sum(case when a.business_model = 'Integrated' then a.basket_size_local else 0 end) as im_basket_size_local

  FROM
    slide.gf_mex_level_daily_metrics_view a
  LEFT JOIN mex_snapshots b
    on a.merchant_id = b.merchant_id
    AND date(a.date_local) = b.date_mex_snapshots
  WHERE date(partition_date_local) >= date([[inc_start_date]])
    and date(partition_date_local) <= date([[inc_end_date]])
  GROUP BY 1,2,3,4,5,6,7,8
)
,mfd as (
    select 
        merchant_id
        ,date(date_local) as date_local
        ,city_name
        ,sum(total_promo_spend_local) as total_promo_spend_local
        ,sum(total_promo_spend_usd) as total_promo_spend_usd
        ,sum(total_grab_promo_spend_local) as total_grab_promo_spend_local
        ,sum(total_grab_promo_spend_usd) as total_grab_promo_spend_usd
        ,sum(total_mfd_local) as total_mfd_local
        ,sum(total_mfd_usd) as total_mfd_usd
        ,sum(mfc_orders) as mfc_orders
        ,sum(mfp_orders) as mfp_orders
        ,sum(double_dipped_orders) as double_dipped_orders
        ,sum(no_of_mfc_campaign) as no_of_mfc_campaign
        ,sum(total_mfc_mex_promo_spend_local) as total_mfc_mex_promo_spend_local
        ,sum(total_mfc_mex_promo_spend_usd) as total_mfc_mex_promo_spend_usd
        ,sum(mfc_prod_mex_promo_spend_local) as mfc_prod_mex_promo_spend_local
        ,sum(mfc_prod_mex_promo_spend_usd) as mfc_prod_mex_promo_spend_usd
        ,sum(mfc_gsheet_mex_promo_spend_local) as mfc_gsheet_mex_promo_spend_local
        ,sum(mfc_gsheet_mex_promo_spend_usd) as mfc_gsheet_mex_promo_spend_usd
        ,sum(mfc_prod_grab_promo_spend_local) as mfc_prod_grab_promo_spend_local
        ,sum(mfc_prod_grab_promo_spend_usd) as mfc_prod_grab_promo_spend_usd
        ,sum(mfc_gsheet_grab_promo_spend_local) as mfc_gsheet_grab_promo_spend_local
        ,sum(mfc_gsheet_grab_promo_spend_usd) as mfc_gsheet_grab_promo_spend_usd
        ,sum(total_mfp_promo_code_expense_local) as total_mfp_promo_code_expense_local
        ,sum(total_mfp_promo_code_expense_usd) as total_mfp_promo_code_expense_usd
        ,sum(mfp_mex_promo_spend_local) as mfp_mex_promo_spend_local
        ,sum(mfp_mex_promo_spend_usd) as mfp_mex_promo_spend_usd
        ,sum(mfp_prod_mex_promo_spend_local) as mfp_prod_mex_promo_spend_local
        ,sum(mfp_prod_mex_promo_spend_usd) as mfp_prod_mex_promo_spend_usd
        ,sum(mfp_gsheet_mex_promo_spend_local) as mfp_gsheet_mex_promo_spend_local
        ,sum(mfp_gsheet_mex_promo_spend_usd) as mfp_gsheet_mex_promo_spend_usd
        ,sum(mfp_grab_promo_code_spend_local) as mfp_grab_promo_code_spend_local
        ,sum(mfp_grab_promo_code_spend_usd) as mfp_grab_promo_code_spend_usd
        ,sum(grab_promo_code_spend_local) as grab_promo_code_spend_local
        ,sum(grab_promo_code_spend_usd) as grab_promo_code_spend_usd
        ,sum(grab_mfc_prod_spend_local) as grab_mfc_prod_spend_local
        ,sum(grab_mfc_prod_spend_usd) as grab_mfc_prod_spend_usd

        ,sum(case when restaurant_partner_status = 'partner' then mfc_prod_mex_promo_spend_usd else 0 end) as partner_mfc_prod_mex_promo_spend_usd
        ,sum(case when restaurant_partner_status = 'partner' then mfc_prod_mex_promo_spend_local else 0 end) as partner_mfc_prod_mex_promo_spend_local
        ,sum(case when restaurant_partner_status = 'partner' then mfc_gsheet_mex_promo_spend_usd else 0 end) as partner_mfc_gsheet_mex_promo_spend_usd
        ,sum(case when restaurant_partner_status = 'partner' then mfc_gsheet_mex_promo_spend_local else 0 end) as partner_mfc_gsheet_mex_promo_spend_local

        ,sum(case when restaurant_partner_status = 'partner' then mfc_prod_grab_promo_spend_usd else 0 end) as partner_mfc_prod_grab_promo_spend_usd
        ,sum(case when restaurant_partner_status = 'partner' then mfc_prod_grab_promo_spend_local else 0 end) as partner_mfc_prod_grab_promo_spend_local
        ,sum(case when restaurant_partner_status = 'partner' then mfc_gsheet_grab_promo_spend_usd else 0 end) as partner_mfc_gsheet_grab_promo_spend_usd
        ,sum(case when restaurant_partner_status = 'partner' then mfc_gsheet_grab_promo_spend_local else 0 end) as partner_mfc_gsheet_grab_promo_spend_local
    from slide.gf_mfd_mex_agg
    where date(partition_date_local) >= date([[inc_start_date]])
        and date(partition_date_local) <= date([[inc_end_date]])
    group by 1,2,3
)
,ads as (
  select 
    indiv_mex as merchant_id
    ,city_name
    ,date_local
    ,sum(avg_ad_spend_local) as avg_ad_spend_local
    ,sum(avg_ad_spend_usd) as avg_ad_spend_usd
  from slide.gf_ads_mex_daily
  where partition_date_local >= date([[inc_start_date]])
    and partition_date_local <= date([[inc_end_date]])
    and indiv_mex is not null
  group by 1,2,3
)
,batching as (
    select
        date_local, merchant_id, city as city_name
        ,sum(case when tried_batching = 'true' then 1 else 0 end) as tried_batching
        ,sum(is_batched_order) as batched_orders
    from slide.food_order_batching_dashboard_order_lvl
    where date(partition_date) >= date([[inc_start_date]])
        and date(partition_date) <= date([[inc_end_date]])
        and order_state = 'COMPLETED'
    group by 1,2,3
)
,mbp as (
    select 
      mbp.restaurant_id as merchant_id
      ,mbp.date_local 
      ,cities.name as city_name 
      ,sum(mbp_paid_by_mex) as mbp_paid_by_mex, sum(mbp_paid_by_pax) as mbp_paid_by_pax, sum(tsp_paid_by_us) as tsp_paid_by_us
    from slide.gf_tsp_mbp_breakdown mbp
    left join public.cities on mbp.city_id = cities.id
    where date(date_local) >= date([[inc_start_date]])
        and date(date_local) <= date([[inc_end_date]])
        and state = 'COMPLETED'
    group by 1,2,3
)
SELECT
    orders.*

    ,coalesce(total_promo_spend_local,0) as total_promo_spend_local
    ,coalesce(total_promo_spend_usd,0) as total_promo_spend_usd
    ,coalesce(total_grab_promo_spend_local,0) as total_grab_promo_spend_local
    ,coalesce(total_grab_promo_spend_usd,0) as total_grab_promo_spend_usd
    ,coalesce(total_mfd_local,0) as total_mfd_local
    ,coalesce(total_mfd_usd,0) as total_mfd_usd
    ,coalesce(mfc_orders,0) as mfc_orders
    ,coalesce(mfp_orders,0) as mfp_orders
    ,coalesce(double_dipped_orders,0) as double_dipped_orders
    ,coalesce(no_of_mfc_campaign,0) as no_of_mfc_campaign
    ,coalesce(total_mfc_mex_promo_spend_local,0) as total_mfc_mex_promo_spend_local
    ,coalesce(total_mfc_mex_promo_spend_usd,0) as total_mfc_mex_promo_spend_usd
    ,coalesce(mfc_prod_mex_promo_spend_local,0) as mfc_prod_mex_promo_spend_local
    ,coalesce(mfc_prod_mex_promo_spend_usd,0) as mfc_prod_mex_promo_spend_usd
    ,coalesce(mfc_gsheet_mex_promo_spend_local,0) as mfc_gsheet_mex_promo_spend_local
    ,coalesce(mfc_gsheet_mex_promo_spend_usd,0) as mfc_gsheet_mex_promo_spend_usd
    ,coalesce(mfc_prod_grab_promo_spend_local,0) as mfc_prod_grab_promo_spend_local
    ,coalesce(mfc_prod_grab_promo_spend_usd,0) as mfc_prod_grab_promo_spend_usd
    ,coalesce(mfc_gsheet_grab_promo_spend_local,0) as mfc_gsheet_grab_promo_spend_local
    ,coalesce(mfc_gsheet_grab_promo_spend_usd,0) as mfc_gsheet_grab_promo_spend_usd
    ,coalesce(total_mfp_promo_code_expense_local,0) as total_mfp_promo_code_expense_local
    ,coalesce(total_mfp_promo_code_expense_usd,0) as total_mfp_promo_code_expense_usd
    ,coalesce(mfp_mex_promo_spend_local,0) as mfp_mex_promo_spend_local
    ,coalesce(mfp_mex_promo_spend_usd,0) as mfp_mex_promo_spend_usd
    ,coalesce(mfp_prod_mex_promo_spend_local,0) as mfp_prod_mex_promo_spend_local
    ,coalesce(mfp_prod_mex_promo_spend_usd,0) as mfp_prod_mex_promo_spend_usd
    ,coalesce(mfp_gsheet_mex_promo_spend_local,0) as mfp_gsheet_mex_promo_spend_local
    ,coalesce(mfp_gsheet_mex_promo_spend_usd,0) as mfp_gsheet_mex_promo_spend_usd
    ,coalesce(mfp_grab_promo_code_spend_local,0) as mfp_grab_promo_code_spend_local
    ,coalesce(mfp_grab_promo_code_spend_usd,0) as mfp_grab_promo_code_spend_usd
    ,coalesce(grab_promo_code_spend_local,0) as grab_promo_code_spend_local
    ,coalesce(grab_promo_code_spend_usd,0) as grab_promo_code_spend_usd
    ,coalesce(grab_mfc_prod_spend_local,0) as grab_mfc_prod_spend_local
    ,coalesce(grab_mfc_prod_spend_usd,0) as grab_mfc_prod_spend_usd

    ,coalesce(partner_mfc_prod_mex_promo_spend_usd,0) as partner_mfc_prod_mex_promo_spend_usd
    ,coalesce(partner_mfc_prod_mex_promo_spend_local,0) as partner_mfc_prod_mex_promo_spend_local
    ,coalesce(partner_mfc_gsheet_mex_promo_spend_usd,0) as partner_mfc_gsheet_mex_promo_spend_usd
    ,coalesce(partner_mfc_gsheet_mex_promo_spend_local,0) as partner_mfc_gsheet_mex_promo_spend_local
    
    ,coalesce(partner_mfc_prod_grab_promo_spend_usd,0) as partner_mfc_prod_grab_promo_spend_usd
    ,coalesce(partner_mfc_prod_grab_promo_spend_local,0) as partner_mfc_prod_grab_promo_spend_local
    ,coalesce(partner_mfc_gsheet_grab_promo_spend_usd,0) as partner_mfc_gsheet_grab_promo_spend_usd
    ,coalesce(partner_mfc_gsheet_grab_promo_spend_local,0) as partner_mfc_gsheet_grab_promo_spend_local
    
    ,coalesce(ads.avg_ad_spend_local,0) as ad_spend_local
    ,coalesce(ads.avg_ad_spend_usd,0) as ad_spend_usd

    ,coalesce(batching.tried_batching,0) as tried_batching
    ,coalesce(batching.batched_orders,0) as batched_orders

    ,orders.date_local as partition_date_local

FROM
    orders

LEFT JOIN mfd
    on orders.merchant_id = mfd.merchant_id
    and orders.city_name = mfd.city_name
    and date(orders.date_local) = mfd.date_local

/*LEFT JOIN mf_promo_code_per_outlet
    ON orders.business_name = mf_promo_code_per_outlet.business_name
    AND orders.country_name = mf_promo_code_per_outlet.country_name
    AND orders.city_name = mf_promo_code_per_outlet.city_name
    AND orders.date_local = date(mf_promo_code_per_outlet.date_local)*/

/*LEFT JOIN mfp
    on mfp.merchant_id = orders.merchant_id 
    and orders.city_name = mfp.city
    and date(orders.date_local) = date(mfp.date_local)*/

LEFT JOIN ads 
    on orders.merchant_id = ads.merchant_id
    and orders.city_name = ads.city_name
    and date(orders.date_local) = date(ads.date_local)

left join batching 
    on orders.merchant_id = batching.merchant_id
    and orders.city_name = batching.city_name
    and date(orders.date_local) = date(batching.date_local)