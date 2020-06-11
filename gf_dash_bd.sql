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
	        and date(concat(year,'-',month,'-',day)) >= date('2019-04-01')
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
            date(concat(year,'-',month,'-',day)) >= date('2019-04-01')
            and date(concat(year,'-',month,'-',day)) < date('2019-06-20')
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
    slide.gf_mex_level_daily_metrics a
  LEFT JOIN mex_snapshots b
    on a.merchant_id = b.merchant_id
    AND date(a.date_local) = b.date_mex_snapshots
  WHERE date(a.partition_date_local) >= date('2019-04-01')
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
,mf_promo_code_per_outlet as (
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
    ) /*union all (
        select 
            biz_outlet.business_name
            ,biz_outlet.country_name
            ,biz_outlet.city_name
            ,biz_outlet.date_local
            ,sum(mfp.mex_funding_amount_perday_local/cast(active_outlet AS double)) AS mf_promo_code_perday_outlet_local
            ,sum(mfp.mex_funding_amount_perday_usd/cast(active_outlet AS double)) AS mf_promo_code_perday_outlet_usd
        from 
            slide.gf_mfp_campaign_daily mfp
        inner join biz_outlet
        ON lower(trim(biz_outlet.business_name)) = lower(trim(mfp.business_name))
            AND biz_outlet.city_name = mfp.city
            AND biz_outlet.country_name = mfp.country
            AND biz_outlet.date_local = date(mfp.date_local)
        where 
            date(mfp.date_local) >= date('2019-07-01')
            --and date_trunc('month', date(mfp.date_local)) >= date_trunc('month', current_date) - interval '3' month
            and biz_outlet.date_local >= date('2019-07-01')
            --and date_trunc('month', date(biz_outlet.date_local)) >= date_trunc('month', current_date) - interval '3' month
        group by 1,2,3,4*/
    --)
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
    where date_local >= date('2019-04-01')
    group by 1,2,3,4
)
,mfp as (
    select 
        merchant_id
        ,city
        ,date_local
        ,sum(mex_mfp_spend_usd) as mex_mfp_spend_usd
        ,sum(mex_mfp_spend_local) as mex_mfp_spend_local
    from slide.gf_mfp_merchant
    group by 1,2,3
)
/*,ads as (
  select 
    indiv_mex as merchant_id
    ,city_name
    ,date_local
    ,sum(avg_ad_spend) as ad_spend_local
    ,sum(avg_ad_spend_usd) as ad_spend_usd
  from slide.gf_ads_mex_daily
  where partition_date >= date('2019-04-01')
    and indiv_mex is not null
  group by 1,2,3
)*/
SELECT
    activation_date
    -- ,date_signed
    ,date_local
    ,bd_merchant_id as merchant_id --need to remove this, change to bd_merchant_id
    ,bd_merchant_name as merchant_name
    ,brand_name
    ,country_name
    ,city_name
    ,partner_status
    ,business_model
    -- ,status

    ,sum(gmv_usd) as gmv_usd
    ,sum(gmv_local) as gmv_local
    ,sum(basket_size_usd) as basket_size_usd
    ,sum(basket_size_local) as basket_size_local
    ,sum(sub_total_usd) as sub_total_usd
    ,sum(sub_total_local) as sub_total_local
    ,sum(all_incoming_orders) as all_incoming_orders
    ,sum(completed_orders) as completed_orders
    ,sum(allocated_orders) as allocated_orders
    ,sum(unallocated_orders) as unallocated_orders
    -- ,sum(completed_orders_gf_item) as completed_orders_gf_item
    ,sum(mex_commission_usd) as mex_commission_usd
    ,sum(mex_commission_local) as mex_commission_local
    ,sum(delivery_fare_usd) as delivery_fare_usd
    ,sum(delivery_fare_local) as delivery_fare_local
    ,sum(dax_delivery_fare_usd) as dax_delivery_fare_usd
    ,sum(dax_delivery_fare_local) as dax_delivery_fare_local
    ,sum(driver_commission_usd) as driver_commission_usd
    ,sum(driver_commission_local_usd) as driver_commission_local_usd
    ,sum(total_cancellations) as total_cancellations
    ,sum(total_pax_cancellations) as total_pax_cancellations
    ,sum(total_dax_cancellations) as total_dax_cancellations
    ,sum(total_operator_cancellations) as total_operator_cancellations
    ,sum(total_mex_cancellations) as total_mex_cancellations
    ,sum(tsp_subsidy_local) as tsp_subsidy_local
    ,sum(tsp_subsidy_usd) as tsp_subsidy_usd
    ,sum(incentives_local) as incentives_local
    ,sum(incentives_usd) as incentives_usd
    ,sum(spot_incentive_bonus_local) as spot_incentive_bonus_local
    ,sum(spot_incentive_bonus_usd) as spot_incentive_bonus_usd
    ,sum(sof_local) as sof_local
    ,sum(sof_usd) as sof_usd
    ,sum(promo_expense_usd) as promo_expense_usd
    ,sum(promo_expense_local) as promo_expense_local
    ,sum(promo_incoming_orders) as promo_incoming_orders
    ,sum(promo_completed_orders) as promo_completed_orders
    ,sum(pre_accept_cancellations) as pre_accept_cancellations
    ,sum(pre_accept_cancellations_pax) as pre_accept_cancellations_pax
    ,sum(pre_accept_cancellations_operator) as pre_accept_cancellations_operator
    ,sum(pre_allocation_cancellations) as pre_allocation_cancellations
    ,sum(pre_allocation_cancellations_pax) as pre_allocation_cancellations_pax
    ,sum(pre_allocation_cancellations_operator) as pre_allocation_cancellations_operator
    ,sum(pre_accept_expired_orders) as pre_accept_expired_orders
    ,sum(delivery_time) as delivery_time
    ,sum(completed_orders_with_delivery_time) as completed_orders_with_delivery_time

    ,sum(total_partner_gmv_usd) as total_partner_gmv_usd
    ,sum(total_partner_gmv_local) as total_partner_gmv_local
    ,sum(total_partner_basket_size_usd) as total_partner_basket_size_usd
    ,sum(total_partner_basket_size_local) as total_partner_basket_size_local
    ,sum(total_partner_sub_total_usd) as total_partner_sub_total_usd
    ,sum(total_partner_sub_total_local) as total_partner_sub_total_local
    ,sum(total_partner_all_incoming_orders) as total_partner_all_incoming_orders
    ,sum(total_partner_completed_orders) as total_partner_completed_orders
    ,sum(total_partner_allocated_orders) as total_partner_allocated_orders
    ,sum(total_partner_unallocated_orders) as total_partner_unallocated_orders
    -- ,sum(total_partner_completed_orders_gf_item) as total_partner_completed_orders_gf_item
    ,sum(total_partner_mex_commission_usd) as total_partner_mex_commission_usd
    ,sum(total_partner_mex_commission_local) as total_partner_mex_commission_local
    ,sum(total_partner_delivery_fare_usd) as total_partner_delivery_fare_usd
    ,sum(total_partner_delivery_fare_local) as total_partner_delivery_fare_local
    ,sum(total_partner_dax_delivery_fare_usd) as total_partner_dax_delivery_fare_usd
    ,sum(total_partner_dax_delivery_fare_local) as total_partner_dax_delivery_fare_local
    ,sum(total_partner_driver_commission_usd) as total_partner_driver_commission_usd
    ,sum(total_partner_driver_commission_local_usd) as total_partner_driver_commission_local_usd
    ,sum(total_partner_total_cancellations) as total_partner_total_cancellations
    ,sum(total_partner_total_pax_cancellations) as total_partner_total_pax_cancellations
    ,sum(total_partner_total_dax_cancellations) as total_partner_total_dax_cancellations
    ,sum(total_partner_total_operator_cancellations) as total_partner_total_operator_cancellations
    ,sum(total_partner_total_mex_cancellations) as total_partner_total_mex_cancellations
    ,sum(total_partner_tsp_subsidy_local) as total_partner_tsp_subsidy_local
    ,sum(total_partner_tsp_subsidy_usd) as total_partner_tsp_subsidy_usd
    ,sum(total_partner_incentives_local) as total_partner_incentives_local
    ,sum(total_partner_incentives_usd) as total_partner_incentives_usd
    ,sum(total_partner_spot_incentive_bonus_local) as total_partner_spot_incentive_bonus_local
    ,sum(total_partner_spot_incentive_bonus_usd) as total_partner_spot_incentive_bonus_usd
    ,sum(total_partner_sof_local) as total_partner_sof_local
    ,sum(total_partner_sof_usd) as total_partner_sof_usd
    ,sum(total_partner_promo_expense_usd) as total_partner_promo_expense_usd
    ,sum(total_partner_promo_expense_local) as total_partner_promo_expense_local
    ,sum(total_partner_promo_incoming_orders) as total_partner_promo_incoming_orders
    ,sum(total_partner_promo_completed_orders) as total_partner_promo_completed_orders
    ,sum(total_partner_pre_accept_cancellations) as total_partner_pre_accept_cancellations
    ,sum(total_partner_pre_accept_cancellations_pax) as total_partner_pre_accept_cancellations_pax
    ,sum(total_partner_pre_accept_cancellations_operator) as total_partner_pre_accept_cancellations_operator
    ,sum(total_partner_pre_allocation_cancellations) as total_partner_pre_allocation_cancellations
    ,sum(total_partner_pre_allocation_cancellations_pax) as total_partner_pre_allocation_cancellations_pax
    ,sum(total_partner_pre_allocation_cancellations_operator) as total_partner_pre_allocation_cancellations_operator
    ,sum(total_partner_pre_accept_expired_orders) as total_partner_pre_accept_expired_orders
    ,sum(total_partner_delivery_time) as total_partner_delivery_time
    ,sum(total_partner_completed_orders_with_delivery_time) as total_partner_completed_orders_with_delivery_time

    ,sum(promo_code_expense_usd) as promo_code_expense_usd
    ,sum(promo_code_expense_local) as promo_code_expense_local
    ,sum(total_partner_promo_code_expense_usd) as total_partner_promo_code_expense_usd
    ,sum(total_partner_promo_code_expense_local) as total_partner_promo_code_expense_local
    ,sum(base_for_mex_commission) as base_for_mex_commission
    ,sum(base_for_mex_commission_local) as base_for_mex_commission_local
    ,sum(total_partner_base_for_mex_commission) as total_partner_base_for_mex_commission
    ,sum(total_partner_base_for_mex_commission_local) as total_partner_base_for_mex_commission_local

    ,sum(im_gmv_usd) as im_gmv_usd
    ,sum(im_gmv_local) as im_gmv_local
    ,sum(im_basket_size) as im_basket_size
    ,sum(im_basket_size_local) as im_basket_size_local

    ,sum(completed_orders_promo_item) as completed_orders_promo_item
    --,sum(gf_promo_spend_usd) as gf_promo_spend_usd
    --,sum(gf_promo_spend_local) as gf_promo_spend_local
    --,sum(mex_promo_spend_usd) as mex_promo_spend_usd
    --,sum(mex_promo_spend_local) as mex_promo_spend_local
    ,sum(mex_promo_spend_n_usd) as mex_promo_spend_n_usd
    ,sum(mex_promo_spend_n_local) as mex_promo_spend_n_local
    --,sum(grab_promo_spend_usd) as grab_promo_spend_usd
    --,sum(grab_promo_spend_local) as grab_promo_spend_local
    ,sum(grab_promo_spend_n_usd) as grab_promo_spend_n_usd
    ,sum(grab_promo_spend_n_local) as grab_promo_spend_n_local
    --,sum(general_promo_item_price_diff_usd) as general_promo_item_price_diff_usd 
    --,sum(general_promo_item_price_diff_local) as general_promo_item_price_diff_local
    ,sum(general_promo_item_price_diff_n_usd) as general_promo_item_price_diff_n_usd
    ,sum(general_promo_item_price_diff_n_local) as general_promo_item_price_diff_n_local

    --,sum(gf_promo_spend_usd_non_mfc) as gf_promo_spend_usd_non_mfc
    --,sum(gf_promo_spend_local_non_mfc) as gf_promo_spend_local_non_mfc
    --,sum(mex_promo_spend_usd_non_mfc) as mex_promo_spend_usd_non_mfc
    --,sum(mex_promo_spend_local_non_mfc) as mex_promo_spend_local_non_mfc
    ,sum(mex_promo_spend_n_usd_non_mfc) as mex_promo_spend_n_usd_non_mfc
    ,sum(mex_promo_spend_n_local_non_mfc) as mex_promo_spend_n_local_non_mfc
    --,sum(grab_promo_spend_usd_non_mfc) as grab_promo_spend_usd_non_mfc
    --,sum(grab_promo_spend_local_non_mfc) as grab_promo_spend_local_non_mfc
    ,sum(grab_promo_spend_n_usd_non_mfc) as grab_promo_spend_n_usd_non_mfc
    ,sum(grab_promo_spend_n_local_non_mfc) as grab_promo_spend_n_local_non_mfc
    --,sum(general_promo_item_price_diff_usd_non_mfc) as general_promo_item_price_diff_usd_non_mfc
    --,sum(general_promo_item_price_diff_local_non_mfc) as general_promo_item_price_diff_local_non_mfc
    ,sum(general_promo_item_price_diff_n_usd_non_mfc) as general_promo_item_price_diff_n_usd_non_mfc
    ,sum(general_promo_item_price_diff_n_local_non_mfc) as general_promo_item_price_diff_n_local_non_mfc

    --,sum(partner_gf_promo_spend_usd) as partner_gf_promo_spend_usd
    --,sum(partner_gf_promo_spend_local) as partner_gf_promo_spend_local
    --,sum(partner_mex_promo_spend_usd) as partner_mex_promo_spend_usd
    --,sum(partner_mex_promo_spend_local) as partner_mex_promo_spend_local
    ,sum(partner_mex_promo_spend_n_usd) as partner_mex_promo_spend_n_usd
    ,sum(partner_mex_promo_spend_n_local) as partner_mex_promo_spend_n_local
    --,sum(partner_grab_promo_spend_usd) as partner_grab_promo_spend_usd
    --,sum(partner_grab_promo_spend_local) as partner_grab_promo_spend_local
    ,sum(partner_grab_promo_spend_n_usd) as partner_grab_promo_spend_n_usd
    ,sum(partner_grab_promo_spend_n_local) as partner_grab_promo_spend_n_local
    --,sum(partner_promo_item_price_diff_usd) as partner_promo_item_price_diff_usd 
    --,sum(partner_promo_item_price_diff_local) as partner_promo_item_price_diff_local 
    ,sum(partner_promo_item_price_diff_n_usd) as partner_promo_item_price_diff_n_usd
    ,sum(partner_promo_item_price_diff_n_local) as partner_promo_item_price_diff_n_local 

    --,sum(partner_gf_promo_spend_usd_non_mfc) as partner_gf_promo_spend_usd_non_mfc
    --,sum(partner_gf_promo_spend_local_non_mfc) as partner_gf_promo_spend_local_non_mfc
    --,sum(partner_mex_promo_spend_usd_non_mfc) as partner_mex_promo_spend_usd_non_mfc
    --,sum(partner_mex_promo_spend_local_non_mfc) as partner_mex_promo_spend_local_non_mfc
    ,sum(partner_mex_promo_spend_n_usd_non_mfc) as partner_mex_promo_spend_n_usd_non_mfc
    ,sum(partner_mex_promo_spend_n_local_non_mfc) as partner_mex_promo_spend_n_local_non_mfc
    --,sum(partner_grab_promo_spend_usd_non_mfc) as partner_grab_promo_spend_usd_non_mfc
    --,sum(partner_grab_promo_spend_local_non_mfc) as partner_grab_promo_spend_local_non_mfc
    ,sum(partner_grab_promo_spend_n_usd_non_mfc) as partner_grab_promo_spend_n_usd_non_mfc
    ,sum(partner_grab_promo_spend_n_local_non_mfc) as partner_grab_promo_spend_n_local_non_mfc
    --,sum(partner_promo_item_price_diff_usd_non_mfc) as partner_promo_item_price_diff_usd_non_mfc
    --,sum(partner_promo_item_price_diff_local_non_mfc) as partner_promo_item_price_diff_local_non_mfc
    ,sum(partner_promo_item_price_diff_n_usd_non_mfc) as partner_promo_item_price_diff_n_usd_non_mfc
    ,sum(partner_promo_item_price_diff_n_local_non_mfc) as partner_promo_item_price_diff_n_local_non_mfc


    ,SUM(mf_promo_code_perday_outlet_local) AS mf_promo_code_perday_outlet_local
    ,SUM(mf_promo_code_perday_outlet_usd) AS mf_promo_code_perday_outlet_usd

    -- ,sum(ad_spend_usd) as ad_spend_usd
    -- ,sum(ad_spend_local) as ad_spend_local
    
FROM(
    SELECT
        
        --,CASE WHEN bd_list.merchant_id is not null THEN 1 ELSE 0 END as bd_account_flag
        orders.*
        ,CASE WHEN not orders.bd_account_flag THEN 'Others' ELSE orders.merchant_id END AS bd_merchant_id
        ,CASE WHEN not orders.bd_account_flag THEN 'Others' ELSE orders.merchant_name END AS bd_merchant_name
        -- ,CASE WHEN not orders.bd_account_flag THEN 'Others' ELSE bd_list.status END AS status
        ,CASE WHEN not orders.bd_account_flag  THEN 'Others' ELSE orders.business_name END AS brand_name

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
        --,coalesce(mfc.general_promo_item_price_diff_usd,0) as general_promo_item_price_diff_usd 
        --,coalesce(mfc.general_promo_item_price_diff_local,0) as general_promo_item_price_diff_local
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
        --,coalesce(mfc.general_promo_item_price_diff_usd_non_mfc,0) as general_promo_item_price_diff_usd_non_mfc
        --,coalesce(mfc.general_promo_item_price_diff_local_non_mfc,0) as general_promo_item_price_diff_local_non_mfc
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
        --,coalesce(mfc.partner_promo_item_price_diff_usd ,0) as partner_promo_item_price_diff_usd 
        --,coalesce(mfc.partner_promo_item_price_diff_local,0) as partner_promo_item_price_diff_local 
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
        --,coalesce(mfc.partner_promo_item_price_diff_usd_non_mfc,0) as partner_promo_item_price_diff_usd_non_mfc
        --,coalesce(mfc.partner_promo_item_price_diff_local_non_mfc,0) as partner_promo_item_price_diff_local_non_mfc
        ,coalesce(mfc.partner_promo_item_price_diff_n_usd_non_mfc,0) as partner_promo_item_price_diff_n_usd_non_mfc
        ,coalesce(mfc.partner_promo_item_price_diff_n_local_non_mfc,0) as partner_promo_item_price_diff_n_local_non_mfc

        ,case 
            when orders.date_local < date('2019-07-01') then COALESCE(mf_promo_code_perday_outlet_local,0)
            else coalesce(mfp.mex_mfp_spend_local, 0) end 
        AS mf_promo_code_perday_outlet_local
        ,case 
            when orders.date_local < date('2019-07-01') then COALESCE(mf_promo_code_perday_outlet_usd,0)
            else coalesce(mfp.mex_mfp_spend_usd, 0) end 
        AS mf_promo_code_perday_outlet_usd
        -- ,COALESCE(mf_promo_code_perday_outlet_usd,0) AS mf_promo_code_perday_outlet_usd
        
        -- ,ads.ad_spend_local
        -- ,ads.ad_spend_usd
    FROM
      orders

    LEFT JOIN mfc
      on orders.merchant_id = mfc.merchant_id
      and orders.city_name = mfc.city
      and date(orders.date_local) = mfc.date_local

    LEFT JOIN mf_promo_code_per_outlet
      ON orders.business_name = mf_promo_code_per_outlet.business_name
      AND orders.country_name = mf_promo_code_per_outlet.country_name
      AND orders.city_name = mf_promo_code_per_outlet.city_name
      AND orders.date_local = date(mf_promo_code_per_outlet.date_local)

    LEFT JOIN mfp
        on mfp.merchant_id = orders.merchant_id 
        and orders.city_name = mfp.city
        and date(orders.date_local) = date(mfp.date_local)
    
    -- LEFT JOIN ads 
    --   on orders.merchant_id = ads.merchant_id
    --   and orders.city_name = ads.city_name
    --   and date(orders.date_local) = date(ads.date_local)
    )
group by 1,2,3,4,5,6,7,8,9