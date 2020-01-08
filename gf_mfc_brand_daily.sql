/*
    Name: slide.gf_mfc_brand_daily
    Refresh Time: Daily, 21:00 UTC
    Aggregation Mode: Incremental, lookback window 60 days
    Lighthouse Dependancy Tables:
        slide.mex_campaign_booking 21:00
*/

/*
 	Reading from MX table: slide.mex_campaign_booking
 	Structure of preceding table: order level, mfc/non-mfc, daily
 	Resulting table: merchant level, mfc/non-mfc, daily
*/
SELECT
    city
    ,country
    ,merchant_id
    ,case when model_type = 1 then 'Integrated' else 'Concierge' end as business_model 
    ,case when is_cashless_booking = true then 'Cashless' else 'Cash' end as cashless_status
    ,count(distinct(order_id)) as completed_orders_promo_item
    ,sum(gf_promo_spend/exchange_one_usd) as gf_promo_spend_usd
    ,sum(gf_promo_spend) as gf_promo_spend_local
    ,sum(mex_promo_spend/exchange_one_usd) as mex_promo_spend_usd
    ,sum(mex_promo_spend) as mex_promo_spend_local
    ,sum(mex_promo_spend_n/exchange_one_usd) as mex_promo_spend_n_usd
    ,sum(mex_promo_spend_n) as mex_promo_spend_n_local
    ,sum(grab_promo_spend/exchange_one_usd) as grab_promo_spend_usd
    ,sum(grab_promo_spend) as grab_promo_spend_local
    ,sum(grab_promo_spend_n/exchange_one_usd) as grab_promo_spend_n_usd
    ,sum(grab_promo_spend_n) as grab_promo_spend_n_local
    ,sum(promo_item_promo_price_local/exchange_one_usd) as promo_item_promo_price_usd
    ,sum(promo_item_promo_price_local) as promo_item_promo_price_local
    ,sum(promo_item_normal_price_local/exchange_one_usd) as promo_item_normal_price_usd
    ,sum(promo_item_normal_price_local) as promo_item_normal_price_local
    ,sum(promo_item_n_promo_price_local/exchange_one_usd) as promo_item_n_promo_price_usd
    ,sum(promo_item_n_promo_price_local) as promo_item_n_promo_price_local
    ,sum(promo_item_n_normal_price_local/exchange_one_usd) as promo_item_n_normal_price_usd
    ,sum(promo_item_n_normal_price_local) as promo_item_n_normal_price_local
    
    /*general, non-mfc*/
    ,sum(case when product_flag = 'Non MFC' then gf_promo_spend/exchange_one_usd else 0 end) as gf_promo_spend_usd_non_mfc
    ,sum(case when product_flag = 'Non MFC' then gf_promo_spend else 0 end) as gf_promo_spend_local_non_mfc
    ,sum(case when product_flag = 'Non MFC' then mex_promo_spend/exchange_one_usd else 0 end) as mex_promo_spend_usd_non_mfc
    ,sum(case when product_flag = 'Non MFC' then mex_promo_spend else 0 end) as mex_promo_spend_local_non_mfc
    ,sum(case when product_flag = 'Non MFC' then mex_promo_spend_n/exchange_one_usd else 0 end) as mex_promo_spend_n_usd_non_mfc
    ,sum(case when product_flag = 'Non MFC' then mex_promo_spend_n else 0 end) as mex_promo_spend_n_local_non_mfc
    ,sum(case when product_flag = 'Non MFC' then grab_promo_spend/exchange_one_usd else 0 end) as grab_promo_spend_usd_non_mfc
    ,sum(case when product_flag = 'Non MFC' then grab_promo_spend else 0 end) as grab_promo_spend_local_non_mfc
    ,sum(case when product_flag = 'Non MFC' then grab_promo_spend_n/exchange_one_usd else 0 end) as grab_promo_spend_n_usd_non_mfc
    ,sum(case when product_flag = 'Non MFC' then grab_promo_spend_n else 0 end) as grab_promo_spend_n_local_non_mfc
    ,sum(case when product_flag = 'Non MFC' then promo_item_promo_price_local/exchange_one_usd else 0 end) as promo_item_promo_price_usd_non_mfc
    ,sum(case when product_flag = 'Non MFC' then promo_item_promo_price_local else 0 end) as promo_item_promo_price_local_non_mfc
    ,sum(case when product_flag = 'Non MFC' then promo_item_normal_price_local/exchange_one_usd else 0 end) as promo_item_normal_price_usd_non_mfc
    ,sum(case when product_flag = 'Non MFC' then promo_item_normal_price_local else 0 end) as promo_item_normal_price_local_non_mfc
    ,sum(case when product_flag = 'Non MFC' then promo_item_n_promo_price_local/exchange_one_usd else 0 end) as promo_item_n_promo_price_usd_non_mfc
    ,sum(case when product_flag = 'Non MFC' then promo_item_n_promo_price_local else 0 end) as promo_item_n_promo_price_local_non_mfc
    ,sum(case when product_flag = 'Non MFC' then promo_item_n_normal_price_local/exchange_one_usd else 0 end) as promo_item_n_normal_price_usd_non_mfc
    ,sum(case when product_flag = 'Non MFC' then promo_item_n_normal_price_local else 0 end) as promo_item_n_normal_price_local_non_mfc

    /* partner, mfc+non-mfc*/
    ,sum(case when partner = 1 then gf_promo_spend/exchange_one_usd else 0 end) as partner_gf_promo_spend_usd
    ,sum(case when partner = 1 then gf_promo_spend else 0 end) as partner_gf_promo_spend_local
    ,sum(case when partner = 1 then mex_promo_spend/exchange_one_usd else 0 end) as partner_mex_promo_spend_usd
    ,sum(case when partner = 1 then mex_promo_spend else 0 end) as partner_mex_promo_spend_local
    ,sum(case when partner = 1 then mex_promo_spend_n/exchange_one_usd else 0 end) as partner_mex_promo_spend_n_usd
    ,sum(case when partner = 1 then mex_promo_spend_n else 0 end) as partner_mex_promo_spend_n_local
    ,sum(case when partner = 1 then grab_promo_spend/exchange_one_usd else 0 end) as partner_grab_promo_spend_usd
    ,sum(case when partner = 1 then grab_promo_spend else 0 end) as partner_grab_promo_spend_local
    ,sum(case when partner = 1 then grab_promo_spend_n/exchange_one_usd else 0 end) as partner_grab_promo_spend_n_usd
    ,sum(case when partner = 1 then grab_promo_spend_n else 0 end) as partner_grab_promo_spend_n_local
    ,sum(case when partner = 1 then promo_item_promo_price_local/exchange_one_usd else 0 end) as partner_promo_item_promo_price_usd
    ,sum(case when partner = 1 then promo_item_promo_price_local else 0 end) as partner_promo_item_promo_price_local
    ,sum(case when partner = 1 then promo_item_normal_price_local/exchange_one_usd else 0 end) as partner_promo_item_normal_price_usd
    ,sum(case when partner = 1 then promo_item_normal_price_local else 0 end) as partner_promo_item_normal_price_local
    ,sum(case when partner = 1 then promo_item_n_promo_price_local/exchange_one_usd else 0 end) as partner_promo_item_n_promo_price_usd
    ,sum(case when partner = 1 then promo_item_n_promo_price_local else 0 end) as partner_promo_item_n_promo_price_local
    ,sum(case when partner = 1 then promo_item_n_normal_price_local/exchange_one_usd else 0 end) as partner_promo_item_n_normal_price_usd
    ,sum(case when partner = 1 then promo_item_n_normal_price_local else 0 end) as partner_promo_item_n_normal_price_local

    /*partner, non-mfc*/
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then gf_promo_spend/exchange_one_usd else 0 end) as partner_gf_promo_spend_usd_non_mfc
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then gf_promo_spend else 0 end) as partner_gf_promo_spend_local_non_mfc
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then mex_promo_spend/exchange_one_usd else 0 end) as partner_mex_promo_spend_usd_non_mfc
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then mex_promo_spend else 0 end) as partner_mex_promo_spend_local_non_mfc
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then mex_promo_spend_n/exchange_one_usd else 0 end) as partner_mex_promo_spend_n_usd_non_mfc
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then mex_promo_spend_n else 0 end) as partner_mex_promo_spend_n_local_non_mfc
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then grab_promo_spend/exchange_one_usd else 0 end) as partner_grab_promo_spend_usd_non_mfc
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then grab_promo_spend else 0 end) as partner_grab_promo_spend_local_non_mfc
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then grab_promo_spend_n/exchange_one_usd else 0 end) as partner_grab_promo_spend_n_usd_non_mfc
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then grab_promo_spend_n else 0 end) as partner_grab_promo_spend_n_local_non_mfc
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then promo_item_promo_price_local/exchange_one_usd else 0 end) as partner_promo_item_promo_price_usd_non_mfc
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then promo_item_promo_price_local else 0 end) as partner_promo_item_promo_price_local_non_mfc
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then promo_item_normal_price_local/exchange_one_usd else 0 end) as partner_promo_item_normal_price_usd_non_mfc
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then promo_item_normal_price_local else 0 end) as partner_promo_item_normal_price_local_non_mfc
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then promo_item_n_promo_price_local/exchange_one_usd else 0 end) as partner_promo_item_n_promo_price_usd_non_mfc
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then promo_item_n_promo_price_local else 0 end) as partner_promo_item_n_promo_price_local_non_mfc
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then promo_item_n_normal_price_local/exchange_one_usd else 0 end) as partner_promo_item_n_normal_price_usd_non_mfc
    ,sum(case when partner = 1 and product_flag = 'Non MFC' then promo_item_n_normal_price_local else 0 end) as partner_promo_item_n_normal_price_local_non_mfc
    
    ,date_local
FROM
    slide.mex_campaign_booking
where
    partition_date >= [[inc_start_date]]
    and partition_date <= [[inc_end_date]]
group by 1,2,3,4,5,79