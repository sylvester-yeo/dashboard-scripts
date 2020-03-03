/*
    Total MFD table
    Base table: 
        1. base_bookings as the base table for all orders
        2. slide.mex_campaign_booking
        3. slide.mfp_orders 
*/
with mfc as (
    select 
        order_id
        ,count(1) as no_of_mfc_campaign
        ,sum(mex_promo_spend_n) as total_mfc_mex_promo_spend
        ,sum(case when product_flag = 'MFC' then mex_promo_spend_n else 0 end) as mfc_prod_mex_promo_spend
        ,sum(case when product_flag = 'Non MFC' then mex_promo_spend_n else 0 end) as mfc_gsheet_mex_promo_spend
        ,sum(case when product_flag = 'MFC' then grab_promo_spend_n else 0 end) as mfc_prod_grab_promo_spend
        ,sum(case when product_flag = 'Non MFC' then grab_promo_spend_n else 0 end) as mfc_gsheet_grab_promo_spend
    from slide.mex_campaign_booking
    where partition_date >= [[inc_start_date]]
        and partition_date <= [[inc_end_date]]
    group by 1
)
,mfp as (
    select 
        order_id
        ,sum(total_promo_spend) as total_mfp_promo_code_expense
        ,sum(mex_promo_spend) as mfp_mex_promo_spend
        ,sum(case when product_flag = 'MFP' then mex_promo_spend else 0 end) as mfp_prod_mex_promo_spend
        ,sum(case when product_flag = 'Non MFP' then mex_promo_spend else 0 end) as mfp_gsheet_mex_promo_spend
        ,sum(total_promo_spend - mex_promo_spend) as mfp_grab_promo_code_spend
    from slide.mfp_orders
    where date_local >= [[inc_start_date]]
        and date_local <= [[inc_end_date]]
    group by 1
)
,intermediate as (
    select 
        bb.order_id 
        ,bb.last_booking_code as booking_code
        ,countries.name as country_name
        ,cities.name as city_name
        ,bb.date_local
        ,bb.merchant_id
        ,bb.booking_state_simple

        /* non monetary order info */
        ,CASE WHEN bb.is_partner_merchant= 1 THEN 'partner' ELSE 'non-partner' END AS restaurant_partner_status
        ,CASE WHEN bb.is_integrated_model = 1 THEN 'Integrated' ELSE 'Concierge' END AS business_model
        ,CASE WHEN bb.is_grabpay = 1 THEN 'Cashless' ELSE 'Cash' END AS cashless_status
        
        /* base_bookings order information */
        ,bb.pax_fare 
        ,bb.small_order_fee
        ,bb.dax_fare
        ,bb.subsidy

        ,bb.sub_total
        ,bb.basket_size

        ,bb.commission_total
        ,bb.commission_from_driver
        ,bb.commission_from_merchant

        ,bb.incentives
        ,bb.spot_incentive_bonus

        ,bb.gross_merchandise_value as gmv

        ,case when mfc.order_id is not null then 'MFC order' end as mfc_indicator
        ,case when mfp.order_id is not null then 'MFP order' end as mfp_indicator
        
        ,bb.promo_expense as bb_promo_expense
        ,bb.promo_code_expense as bb_promo_code_expense

        ,coalesce(mfc.no_of_mfc_campaign,0) as no_of_mfc_campaign
        ,coalesce(mfc.total_mfc_mex_promo_spend,0) as total_mfc_mex_promo_spend
        ,coalesce(mfc.mfc_prod_mex_promo_spend,0) as mfc_prod_mex_promo_spend
        ,coalesce(mfc.mfc_gsheet_mex_promo_spend,0) as mfc_gsheet_mex_promo_spend
        ,coalesce(mfc.mfc_prod_grab_promo_spend,0) as mfc_prod_grab_promo_spend
        ,coalesce(mfc.mfc_gsheet_grab_promo_spend,0) as mfc_gsheet_grab_promo_spend
        
        ,coalesce(mfp.total_mfp_promo_code_expense,0) as total_mfp_promo_code_expense
        ,coalesce(mfp.mfp_mex_promo_spend,0) as mfp_mex_promo_spend
        ,coalesce(mfp.mfp_prod_mex_promo_spend,0) as mfp_prod_mex_promo_spend
        ,coalesce(mfp.mfp_gsheet_mex_promo_spend,0) as mfp_gsheet_mex_promo_spend
        ,coalesce(mfp.mfp_grab_promo_code_spend,0) as mfp_grab_promo_code_spend

        ,bb.promo_code_expense - coalesce(mfp.mfp_mex_promo_spend,0) as grab_promo_code_spend
        ,bb.promo_expense - bb.promo_code_expense - coalesce(mfc.mfc_prod_mex_promo_spend,0) as grab_mfc_prod_spend

        ,bb.promo_expense + coalesce(mfc.mfc_gsheet_mex_promo_spend, 0) + coalesce(mfc.mfc_gsheet_grab_promo_spend, 0) as total_promo_spend

        ,bb.reward_id as reward_id
        ,bb.promo_code 
        ,rer.exchange_one_usd as fx_one_usd

    from slide.datamart_bb_grabfood bb
    left join mfc on bb.order_id = mfc.order_id
    left join mfp on bb.order_id = mfp.order_id
    left join public.cities on bb.city_id = cities.id
    left join public.countries on cities.country_id = countries.id
    left join datamart.ref_exchange_rates rer 
        on countries.id  = rer.country_id 
        and (bb.date_local) >= date_format(rer.start_date, '%Y-%m-%d') 
        and (bb.date_local) <= date_format(rer.end_date, '%Y-%m-%d')
    where bb.date_local >= [[inc_start_date]]
        and bb.date_local <= [[inc_end_date]]
        -- and booking_state_simple = 'COMPLETED'
)
select 
    order_id 
    ,booking_code
    ,country_name
    ,city_name
    ,merchant_id
    ,booking_state_simple

    /* non monetary order info */
    ,restaurant_partner_status
    ,business_model
    ,cashless_status
    
    /*profitability numbers*/
    ,commission_total
    ,(subsidy + incentives + spot_incentive_bonus) as total_incentive_inc_tsp
    ,total_promo_spend
    ,grab_promo_code_spend + grab_mfc_prod_spend + mfc_gsheet_grab_promo_spend as total_grab_promo_spend
    ,total_mfc_mex_promo_spend + mfp_mex_promo_spend as total_mfd
    ,(commission_total + small_order_fee - subsidy - incentives - spot_incentive_bonus - grab_promo_code_spend - grab_mfc_prod_spend - mfc_gsheet_grab_promo_spend) as net_revenue

    /* MFD stuff */
    ,mfc_indicator
    ,mfp_indicator

    ,no_of_mfc_campaign
    ,total_mfc_mex_promo_spend
    ,mfc_prod_mex_promo_spend
    ,mfc_gsheet_mex_promo_spend
    ,mfc_prod_grab_promo_spend
    ,mfc_gsheet_grab_promo_spend
    
    ,total_mfp_promo_code_expense
    ,mfp_mex_promo_spend
    ,mfp_prod_mex_promo_spend
    ,mfp_gsheet_mex_promo_spend
    ,mfp_grab_promo_code_spend

    ,grab_promo_code_spend
    ,grab_mfc_prod_spend

    /*promo code related*/
    ,reward_id
    ,promo_code

    /* base_bookings order information */
    ,pax_fare 
    ,small_order_fee
    ,dax_fare
    ,subsidy
    ,incentives
    ,spot_incentive_bonus

    ,sub_total
    ,basket_size
    ,gmv

    ,commission_from_driver
    ,commission_from_merchant  

    ,bb_promo_expense
    ,bb_promo_code_expense

    ,fx_one_usd
    ,date_local
from intermediate