/*
  Name: slide.gf_pnl_dash
  Refresh Time: Daily, 21:00 UTC
  Aggregation Mode: Overwrite
  Lighthouse dependency Tables:
    slide.gf_mex_level_daily_metrics 21:00
    
*/

/*regional bd pnl*/

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
      from snapshots.food_data_service_merchant_contracts
      where date(concat(year,'-',month,'-',day)) >= date('2019-06-20')
    )
    where row_num = 1
    /*not applicable for p&l at the moment*//*
    union all (
        select * 
        from (
            select 
                id as merchant_id
                ,case when json_extract_scalar(contract,'$.partner') = '1' then true else false end as partner
                ,date(valid_from + interval '1' second) as date_mex_snapshots
                ,valid_from
                ,row_number() over (partition by id, date(valid_from + interval '1' second) order by valid_from + interval '1' second asc) as row_num
            from snapshots.grab_mall_grab_mall_seller
            where date(concat(year,'-',month,'-',day)) < date('2019-06-20')
            and date(concat(year,'-',month,'-',day)) >= date('2019-04-01')
            )
        where row_num = 1
    )
    */
)
,mex as (
  select *
  from (
    select
        merchant_id
        ,model_type
        ,date(valid_from) as date_mex_snapshots
        ,valid_from
        --,chain_name as brand_name
        ,row_number() over (partition by merchant_id, date(valid_from) order by valid_from asc) as row_num
    from snapshots.food_data_service_merchants
    where date(concat(year,'-',month,'-',day)) >= date('2019-06-20')
    )
  where row_num = 1
  /*not applicable for p&l at the moment*//*
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
        date(concat(year,'-',month,'-',day)) >= date('2019-04-01')
        and date(concat(year,'-',month,'-',day)) < date('2019-06-20')
  )
  where row_num = 1
      */
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
  from
    mex
  left join mex_con
    on mex.merchant_id = mex_con.merchant_id
    and mex.date_mex_snapshots = mex_con.date_mex_snapshots
  left join datamart.dim_merchants mex_dim
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
    ,b.bd_date as activation_date
    ,b.bd_account_flag
    ,b.bd_partner_flag
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
    ,sum(a.base_for_mex_commission_local) as base_for_mex_commission_local
    ,sum(a.base_for_mex_commission) as base_for_mex_commission
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
    ,sum(a.promo_expense) as promo_expense_usd
    ,sum(a.promo_expense_local) as promo_expense_local
    ,sum(a.promo_code_expense) as promo_code_expense
    ,sum(a.promo_code_expense_local) as promo_code_expense_local
    ,sum(a.promo_incoming_orders) as promo_incoming_orders
    ,sum(a.promo_completed_orders) as promo_completed_orders
    ,sum(a.pre_accept_cancellations) as pre_accept_cancellations
    ,sum(a.pre_accept_cancellations_pax) as pre_accept_cancellations_pax
    ,sum(a.pre_accept_cancellations_operator) as pre_accept_cancellations_operator
    ,sum(a.pre_allocation_cancellations) as pre_allocation_cancellations
    ,sum(a.pre_allocation_cancellations_pax) as pre_allocation_cancellations_pax
    ,sum(a.pre_allocation_cancellations_operator) as pre_allocation_cancellations_operator

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
    ,sum(case when a.restaurant_partner_status = 'partner' then a.base_for_mex_commission_local END) as total_partner_base_for_mex_commission_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.base_for_mex_commission END) as total_partner_base_for_mex_commission
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
    ,sum(case when a.restaurant_partner_status = 'partner' then a.spot_incentive_bonus_local END) as total_partner_spot_incentive_bonus_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.spot_incentive_bonus_usd END) as total_partner_spot_incentive_bonus_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.promo_expense END) as total_partner_promo_expense_usd
    ,sum(case when a.restaurant_partner_status = 'partner' then a.promo_expense_local END) as total_partner_promo_expense_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.promo_code_expense END) as total_partner_promo_code_expense
    ,sum(case when a.restaurant_partner_status = 'partner' then a.promo_code_expense_local END) as total_partner_promo_code_expense_local
    ,sum(case when a.restaurant_partner_status = 'partner' then a.promo_incoming_orders END) as total_partner_promo_incoming_orders
    ,sum(case when a.restaurant_partner_status = 'partner' then a.promo_completed_orders END) as total_partner_promo_completed_orders
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_accept_cancellations END) as total_partner_pre_accept_cancellations
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_accept_cancellations_pax END) as total_partner_pre_accept_cancellations_pax
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_accept_cancellations_operator END) as total_partner_pre_accept_cancellations_operator
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_allocation_cancellations END) as total_partner_pre_allocation_cancellations
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_allocation_cancellations_pax END) as total_partner_pre_allocation_cancellations_pax
    ,sum(case when a.restaurant_partner_status = 'partner' then a.pre_allocation_cancellations_operator END) as total_partner_pre_allocation_cancellations_operator

    /*specific metrics*/
    ,sum(case when a.business_model = 'Integrated' then gmv_usd_gf else 0 end) as im_gmv_usd
    ,sum(case when a.business_model = 'Integrated' then gmv_local else 0 end) as im_gmv_local

    /*takeaway metrics */
    ,sum(a.takeaway_gmv_local) as takeaway_gmv_local
    ,sum(a.takeaway_gmv_usd) as takeaway_gmv_usd
    ,sum(a.takeaway_mex_commission_local) as takeaway_mex_commission_local
    ,sum(a.takeaway_mex_commission_usd) as takeaway_mex_commission_usd
    ,sum(a.takeaway_base_for_mex_commission_local) as takeaway_base_for_mex_commission_local
    ,sum(a.takeaway_base_for_mex_commission) as takeaway_base_for_mex_commission
    ,sum(a.takeaway_basket_size_usd) as takeaway_basket_size_usd
    ,sum(a.takeaway_basket_size_local) as takeaway_basket_size_local
    ,sum(a.takeaway_sub_total_usd) as takeaway_sub_total_usd
    ,sum(a.takeaway_sub_total_local) as takeaway_sub_total_local
    ,sum(a.takeaway_time_from_order_create_to_completed) as takeaway_time_from_order_create_to_completed
  FROM 
    slide.gf_mex_level_daily_metrics a
  LEFT JOIN mex_snapshots b
    on a.merchant_id = b.merchant_id
    AND date(a.date_local) = b.date_mex_snapshots
  WHERE date_trunc('month', date(a.partition_date_local)) >= date_trunc('month', current_date) - interval '2' month
    and a.country_name = 'Indonesia'
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
,biz_outlet as (
    SELECT
        business_name,
        country_name,
        city_name,
        date_local,
        count(merchant_id) AS active_outlet
    FROM orders
    GROUP BY 1,2,3,4
)
/*,mf_promo_code_per_outlet as (
    select * from (
        select 
            biz_outlet.business_name,
            biz_outlet.country_name,
            biz_outlet.city_name,
            biz_outlet.date_local,
            sum(mf_promo_code.mex_funding_amount_perday_local/cast(active_outlet AS double)) AS mf_promo_code_perday_outlet_local,
            sum(mf_promo_code.mex_funding_amount_perday_usd/cast(active_outlet AS double)) AS mf_promo_code_perday_outlet_usd
        FROM
            slide.mex_funded_promo_code_by_brand_cg mf_promo_code
        inner join biz_outlet
        ON lower(trim(biz_outlet.business_name)) = lower(trim(mf_promo_code.business_name))
            AND biz_outlet.city_name = mf_promo_code.city
            AND biz_outlet.country_name = mf_promo_code.country
            AND biz_outlet.date_local = mf_promo_code.date_local
        where date(mf_promo_code.date_local) < date('2019-07-01') and biz_outlet.date_local < date('2019-07-01')
        group by 1,2,3,4
    ) 
)*/
,mfp as (
    select 
        merchant_id
        ,city
        ,date_local
        ,sum(mex_mfp_spend_usd) as mex_mfp_spend_usd
        ,sum(mex_mfp_spend_local) as mex_mfp_spend_local
    from slide.gf_mfp_merchant
    where date_trunc('month', date(date_local)) >= date_trunc('month', current_date) - interval '3' month
        and country = 'Indonesia'
    group by 1,2,3
)
,batching as (
    select
        date_local, merchant_id, city as city_name
        ,sum(case when tried_batching = 'true' then 1 else 0 end) as tried_batching
        ,sum(is_batched_order) as batched_orders
    from slide.food_order_batching_dashboard_order_lvl
    where date_trunc('month', date(partition_date)) >= date_trunc('month', current_date) - interval '3' month
        /*[[(partition_date) >= date_format(date({{start_date}}) - interval '1' day,'%Y-%m-%d')]]
        and [[(partition_date) <= date_format(date({{end_date}}) + interval '1' day,'%Y-%m-%d')]]
        and [[date(date_local) >= date({{start_date}})]]
        and [[date(date_local) <= date({{end_date}})]]*/
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
    where date_trunc('month', date_local) >= date_trunc('month', current_date) - interval '3' month
        and state = 'COMPLETED'
    group by 1,2,3
)
,mfc as (
    select 
      city
      ,country
      ,merchant_id
      ,date_local
      ,sum(mfc.completed_orders_promo_item) as completed_orders_promo_item
      --,sum(mfc.gf_promo_spend_usd) as gf_promo_spend_usd
      --,sum(mfc.gf_promo_spend_local) as gf_promo_spend_local
      --,sum(mfc.mex_promo_spend_usd) as mex_promo_spend_usd
      --,sum(mfc.mex_promo_spend_local) as mex_promo_spend_local
      ,sum(mfc.mex_promo_spend_n_usd) as mex_promo_spend_n_usd
      ,sum(mfc.mex_promo_spend_n_local) as mex_promo_spend_n_local
      --,sum(mfc.grab_promo_spend_usd) as grab_promo_spend_usd
      --,sum(mfc.grab_promo_spend_local) as grab_promo_spend_local
      ,sum(mfc.grab_promo_spend_n_usd) as grab_promo_spend_n_usd
      ,sum(mfc.grab_promo_spend_n_local) as grab_promo_spend_n_local
      --,sum(mfc.promo_item_normal_price_usd - mfc.promo_item_promo_price_usd) as general_promo_item_price_diff_usd 
      --,sum(mfc.promo_item_normal_price_local - mfc.promo_item_promo_price_local) as general_promo_item_price_diff_local
      ,sum(mfc.promo_item_n_normal_price_usd - mfc.promo_item_n_promo_price_usd) as general_promo_item_price_diff_n_usd
      ,sum(mfc.promo_item_n_normal_price_local - mfc.promo_item_n_promo_price_local) as general_promo_item_price_diff_n_local

      --,sum(mfc.gf_promo_spend_usd_non_mfc) as gf_promo_spend_usd_non_mfc
      --,sum(mfc.gf_promo_spend_local_non_mfc) as gf_promo_spend_local_non_mfc
      --,sum(mfc.mex_promo_spend_usd_non_mfc) as mex_promo_spend_usd_non_mfc
      --,sum(mfc.mex_promo_spend_local_non_mfc) as mex_promo_spend_local_non_mfc
      ,sum(mfc.mex_promo_spend_n_usd_non_mfc) as mex_promo_spend_n_usd_non_mfc
      ,sum(mfc.mex_promo_spend_n_local_non_mfc) as mex_promo_spend_n_local_non_mfc
      --,sum(mfc.grab_promo_spend_usd_non_mfc) as grab_promo_spend_usd_non_mfc
      --,sum(mfc.grab_promo_spend_local_non_mfc) as grab_promo_spend_local_non_mfc
      ,sum(mfc.grab_promo_spend_n_usd_non_mfc) as grab_promo_spend_n_usd_non_mfc
      ,sum(mfc.grab_promo_spend_n_local_non_mfc) as grab_promo_spend_n_local_non_mfc
      --,sum(mfc.promo_item_normal_price_usd_non_mfc - mfc.promo_item_promo_price_usd_non_mfc) as general_promo_item_price_diff_usd_non_mfc
      --,sum(mfc.promo_item_normal_price_local_non_mfc - mfc.promo_item_promo_price_local_non_mfc) as general_promo_item_price_diff_local_non_mfc
      ,sum(mfc.promo_item_n_normal_price_usd_non_mfc - mfc.promo_item_n_promo_price_usd_non_mfc) as general_promo_item_price_diff_n_usd_non_mfc
      ,sum(mfc.promo_item_n_normal_price_local_non_mfc - mfc.promo_item_n_promo_price_local_non_mfc) as general_promo_item_price_diff_n_local_non_mfc

      --,sum(mfc.partner_gf_promo_spend_usd) as partner_gf_promo_spend_usd
      --,sum(mfc.partner_gf_promo_spend_local) as partner_gf_promo_spend_local
      --,sum(mfc.partner_mex_promo_spend_usd) as partner_mex_promo_spend_usd
      --,sum(mfc.partner_mex_promo_spend_local) as partner_mex_promo_spend_local
      ,sum(mfc.partner_mex_promo_spend_n_usd) as partner_mex_promo_spend_n_usd
      ,sum(mfc.partner_mex_promo_spend_n_local) as partner_mex_promo_spend_n_local
      --,sum(mfc.partner_grab_promo_spend_usd) as partner_grab_promo_spend_usd
      --,sum(mfc.partner_grab_promo_spend_local) as partner_grab_promo_spend_local
      ,sum(mfc.partner_grab_promo_spend_n_usd) as partner_grab_promo_spend_n_usd
      ,sum(mfc.partner_grab_promo_spend_n_local) as partner_grab_promo_spend_n_local
      --,sum(mfc.partner_promo_item_normal_price_usd - mfc.partner_promo_item_promo_price_usd) as partner_promo_item_price_diff_usd 
      --,sum(mfc.partner_promo_item_normal_price_local - mfc.partner_promo_item_promo_price_local) as partner_promo_item_price_diff_local 
      ,sum(mfc.partner_promo_item_n_normal_price_usd - mfc.partner_promo_item_n_promo_price_usd) as partner_promo_item_price_diff_n_usd
      ,sum(mfc.partner_promo_item_n_normal_price_local - mfc.partner_promo_item_n_promo_price_local) as partner_promo_item_price_diff_n_local 

      --,sum(mfc.partner_gf_promo_spend_usd_non_mfc) as partner_gf_promo_spend_usd_non_mfc
      --,sum(mfc.partner_gf_promo_spend_local_non_mfc) as partner_gf_promo_spend_local_non_mfc
      --,sum(mfc.partner_mex_promo_spend_usd_non_mfc) as partner_mex_promo_spend_usd_non_mfc
      --,sum(mfc.partner_mex_promo_spend_local_non_mfc) as partner_mex_promo_spend_local_non_mfc
      ,sum(mfc.partner_mex_promo_spend_n_usd_non_mfc) as partner_mex_promo_spend_n_usd_non_mfc
      ,sum(mfc.partner_mex_promo_spend_n_local_non_mfc) as partner_mex_promo_spend_n_local_non_mfc
      --,sum(mfc.partner_grab_promo_spend_usd_non_mfc) as partner_grab_promo_spend_usd_non_mfc
      --,sum(mfc.partner_grab_promo_spend_local_non_mfc) as partner_grab_promo_spend_local_non_mfc
      ,sum(mfc.partner_grab_promo_spend_n_usd_non_mfc) as partner_grab_promo_spend_n_usd_non_mfc
      ,sum(mfc.partner_grab_promo_spend_n_local_non_mfc) as partner_grab_promo_spend_n_local_non_mfc
      --,sum(mfc.partner_promo_item_normal_price_usd_non_mfc - mfc.partner_promo_item_promo_price_usd_non_mfc) as partner_promo_item_price_diff_usd_non_mfc
      --,sum(mfc.partner_promo_item_normal_price_local_non_mfc - mfc.partner_promo_item_promo_price_local_non_mfc) as partner_promo_item_price_diff_local_non_mfc
      ,sum(mfc.partner_promo_item_n_normal_price_usd_non_mfc - mfc.partner_promo_item_n_promo_price_usd_non_mfc) as partner_promo_item_price_diff_n_usd_non_mfc
      ,sum(mfc.partner_promo_item_n_normal_price_local_non_mfc - mfc.partner_promo_item_n_promo_price_local_non_mfc) as partner_promo_item_price_diff_n_local_non_mfc
    from slide.gf_mfc_brand mfc
    where date_trunc('month', date(mfc.date_local)) >= date_trunc('month', current_date) - interval '2' month
    group by 1,2,3,4
)
/*,comms as (
    select 
        merchant_id
        ,country_name
        ,city_name
        ,merchant_name
        ,start_date_of_month
        ,min(blended_collection_rate) as blended_collection_rate
    from slide.gf_id_collection_rate_cm
    group by 1,2,3,4,5
)*/
,final_table as (
  SELECT
    orders.*
    ,coalesce(mfc.completed_orders_promo_item,0) as completed_orders_promo_item
    --,coalesce(mfc.gf_promo_spend_usd,0) as gf_promo_spend_usd
    --,coalesce(mfc.gf_promo_spend_local,0) as gf_promo_spend_local
    --,coalesce(mfc.mex_promo_spend_usd,0) as mex_promo_spend_usd
    --,coalesce(mfc.mex_promo_spend_local,0) as mex_promo_spend_local
    ,coalesce(mfc.mex_promo_spend_n_usd,0) as mex_promo_spend_n_usd
    ,coalesce(mfc.mex_promo_spend_n_local,0) as mex_promo_spend_n_local
    --,coalesce(mfc.grab_promo_spend_usd,0) as grab_promo_spend_usd
    --,coalesce(mfc.grab_promo_spend_local,0) as grab_promo_spend_local
    ,coalesce(mfc.grab_promo_spend_n_usd,0) as grab_promo_spend_n_usd
    ,coalesce(mfc.grab_promo_spend_n_local,0) as grab_promo_spend_n_local
    --,coalesce(mfc.promo_item_normal_price_usd - mfc.promo_item_promo_price_usd,0) as general_promo_item_price_diff_usd 
    --,coalesce(mfc.promo_item_normal_price_local - mfc.promo_item_promo_price_local,0) as general_promo_item_price_diff_local
    ,coalesce(general_promo_item_price_diff_n_usd,0) as general_promo_item_price_diff_n_usd
    ,coalesce(general_promo_item_price_diff_n_local,0) as general_promo_item_price_diff_n_local

    --,coalesce(mfc.gf_promo_spend_usd_non_mfc,0) as gf_promo_spend_usd_non_mfc
    --,coalesce(mfc.gf_promo_spend_local_non_mfc,0) as gf_promo_spend_local_non_mfc
    --,coalesce(mfc.mex_promo_spend_usd_non_mfc,0) as mex_promo_spend_usd_non_mfc
    --,coalesce(mfc.mex_promo_spend_local_non_mfc,0) as mex_promo_spend_local_non_mfc
    ,coalesce(mfc.mex_promo_spend_n_usd_non_mfc,0) as mex_promo_spend_n_usd_non_mfc
    ,coalesce(mfc.mex_promo_spend_n_local_non_mfc,0) as mex_promo_spend_n_local_non_mfc
    --,coalesce(mfc.grab_promo_spend_usd_non_mfc,0) as grab_promo_spend_usd_non_mfc
    --,coalesce(mfc.grab_promo_spend_local_non_mfc,0) as grab_promo_spend_local_non_mfc
    ,coalesce(mfc.grab_promo_spend_n_usd_non_mfc,0) as grab_promo_spend_n_usd_non_mfc
    ,coalesce(mfc.grab_promo_spend_n_local_non_mfc,0) as grab_promo_spend_n_local_non_mfc
    --,coalesce(mfc.promo_item_normal_price_usd_non_mfc - mfc.promo_item_promo_price_usd_non_mfc,0) as general_promo_item_price_diff_usd_non_mfc
    --,coalesce(mfc.promo_item_normal_price_local_non_mfc - mfc.promo_item_promo_price_local_non_mfc,0) as general_promo_item_price_diff_local_non_mfc
    ,coalesce(general_promo_item_price_diff_n_usd_non_mfc,0) as general_promo_item_price_diff_n_usd_non_mfc
    ,coalesce(general_promo_item_price_diff_n_local_non_mfc,0) as general_promo_item_price_diff_n_local_non_mfc

    --,coalesce(mfc.partner_gf_promo_spend_usd,0) as partner_gf_promo_spend_usd
    --,coalesce(mfc.partner_gf_promo_spend_local,0) as partner_gf_promo_spend_local
    --,coalesce(mfc.partner_mex_promo_spend_usd,0) as partner_mex_promo_spend_usd
    --,coalesce(mfc.partner_mex_promo_spend_local,0) as partner_mex_promo_spend_local
    ,coalesce(mfc.partner_mex_promo_spend_n_usd,0) as partner_mex_promo_spend_n_usd
    ,coalesce(mfc.partner_mex_promo_spend_n_local,0) as partner_mex_promo_spend_n_local
    --,coalesce(mfc.partner_grab_promo_spend_usd,0) as partner_grab_promo_spend_usd
    --,coalesce(mfc.partner_grab_promo_spend_local,0) as partner_grab_promo_spend_local
    ,coalesce(mfc.partner_grab_promo_spend_n_usd,0) as partner_grab_promo_spend_n_usd
    ,coalesce(mfc.partner_grab_promo_spend_n_local,0) as partner_grab_promo_spend_n_local
    --,coalesce(mfc.partner_promo_item_normal_price_usd - mfc.partner_promo_item_promo_price_usd,0) as partner_promo_item_price_diff_usd 
    --,coalesce(mfc.partner_promo_item_normal_price_local - mfc.partner_promo_item_promo_price_local,0) as partner_promo_item_price_diff_local 
    ,coalesce(partner_promo_item_price_diff_n_usd,0) as partner_promo_item_price_diff_n_usd
    ,coalesce(partner_promo_item_price_diff_n_local,0) as partner_promo_item_price_diff_n_local 

    --,coalesce(mfc.partner_gf_promo_spend_usd_non_mfc,0) as partner_gf_promo_spend_usd_non_mfc
    --,coalesce(mfc.partner_gf_promo_spend_local_non_mfc,0) as partner_gf_promo_spend_local_non_mfc
    --,coalesce(mfc.partner_mex_promo_spend_usd_non_mfc,0) as partner_mex_promo_spend_usd_non_mfc
    --,coalesce(mfc.partner_mex_promo_spend_local_non_mfc,0) as partner_mex_promo_spend_local_non_mfc
    ,coalesce(mfc.partner_mex_promo_spend_n_usd_non_mfc,0) as partner_mex_promo_spend_n_usd_non_mfc
    ,coalesce(mfc.partner_mex_promo_spend_n_local_non_mfc,0) as partner_mex_promo_spend_n_local_non_mfc
    --,coalesce(mfc.partner_grab_promo_spend_usd_non_mfc,0) as partner_grab_promo_spend_usd_non_mfc
    --,coalesce(mfc.partner_grab_promo_spend_local_non_mfc,0) as partner_grab_promo_spend_local_non_mfc
    ,coalesce(mfc.partner_grab_promo_spend_n_usd_non_mfc,0) as partner_grab_promo_spend_n_usd_non_mfc
    ,coalesce(mfc.partner_grab_promo_spend_n_local_non_mfc,0) as partner_grab_promo_spend_n_local_non_mfc
    --,coalesce(mfc.partner_promo_item_normal_price_usd_non_mfc - mfc.partner_promo_item_promo_price_usd_non_mfc,0) as partner_promo_item_price_diff_usd_non_mfc
    --,coalesce(mfc.partner_promo_item_normal_price_local_non_mfc - mfc.partner_promo_item_promo_price_local_non_mfc,0) as partner_promo_item_price_diff_local_non_mfc
    ,coalesce(mfc.partner_promo_item_price_diff_n_usd_non_mfc,0) as partner_promo_item_price_diff_n_usd_non_mfc
    ,coalesce(mfc.partner_promo_item_price_diff_n_local_non_mfc,0) as partner_promo_item_price_diff_n_local_non_mfc
    ,coalesce(mfp.mex_mfp_spend_usd, 0) as mf_promo_code_perday_outlet_usd 
    ,coalesce(mfp.mex_mfp_spend_local, 0) as mf_promo_code_perday_outlet_local
    -- ,case 
    --     when orders.date_local < date('2019-07-01') then COALESCE(mf_promo_code_perday_outlet_local,0)
    --     else coalesce(mfp.mex_mfp_spend_local, 0) end 
    -- AS mf_promo_code_perday_outlet_local
    -- ,case 
    --     when orders.date_local < date('2019-07-01') then COALESCE(mf_promo_code_perday_outlet_usd,0)
    --     else coalesce(mfp.mex_mfp_spend_usd, 0) end 
    -- AS mf_promo_code_perday_outlet_usd

    /* batching related metrics*/
    ,COALESCE(batching.tried_batching, 0) as tried_batching
    ,COALESCE(batching.batched_orders, 0) as batched_orders

  /* mbp related metrics*/
    ,COALESCE(mbp.mbp_paid_by_mex,0) as mbp_paid_by_mex
    ,COALESCE(mbp.mbp_paid_by_pax,0) as mbp_paid_by_pax
    ,COALESCE(mbp.tsp_paid_by_us,0) as tsp_paid_by_us

    /*comms related*/
    /*,COALESCE(case when orders.business_model = 'Integrated' then 100 else comms.blended_collection_rate end,0) as blended_collection_rate*/

    /*fx for MBP*/
    ,rer.exchange_one_usd as fx_one_usd
    
  FROM orders

  LEFT JOIN mfc
    on orders.merchant_id = mfc.merchant_id
    and orders.city_name = mfc.city
    and date(orders.date_local) = date(mfc.date_local)

--   LEFT JOIN mf_promo_code_per_outlet
--     ON lower(trim(orders.business_name)) = lower(trim(mf_promo_code_per_outlet.business_name))
--     AND orders.country_name = mf_promo_code_per_outlet.country_name
--     AND orders.city_name = mf_promo_code_per_outlet.city_name
--     AND orders.date_local = date(mf_promo_code_per_outlet.date_local)

  LEFT JOIN mfp
    on mfp.merchant_id = orders.merchant_id 
    and orders.city_name = mfp.city
    and date(orders.date_local) = date(mfp.date_local)

  LEFT JOIN batching
    on orders.merchant_id = batching.merchant_id
    and orders.date_local = batching.date_local
    and orders.city_name = batching.city_name

  LEFT JOIN mbp
    on orders.merchant_id = mbp.merchant_id
    and orders.date_local = mbp.date_local
    and orders.city_name = mbp.city_name

  /*LEFT JOIN comms
    on orders.merchant_id = comms.merchant_id
    and date_trunc('month',date(orders.date_local)) = comms.start_date_of_month
    and orders.city_name = comms.city_name*/

  LEFT JOIN public.countries on orders.country_name = countries.name

  LEFT JOIN datamart.ref_exchange_rates rer on countries.id = rer.country_id and (orders.date_local between rer.start_date and rer.end_date)
)
,am_list as (
  SELECT am_merchant_id, MAX(am_name) as am_name FROM (
    (SELECT merchant_id as am_merchant_id, MAX(am) as am_name FROM holistics.gf_am_mapping_nondelta group by 1)
    UNION all
    (SELECT merchant_id as am_merchant_id, MAX(am) as am_name FROM holistics.gf_am_mapping_delta group by 1))
  GROUP BY 1
)
select
  a.*, coalesce(am_list.am_name, 'Non-AM') as am_name
from final_table a
left join am_list on a.merchant_id = am_list.am_merchant_id
where a.date_local >= date_trunc('month', current_date) - interval '2' month
and a.country_name = 'Indonesia'