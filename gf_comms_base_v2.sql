/*
    Name: slide.gf_comms_base_v2
    Refresh Time: Daily, 21:00 UTC
    Aggregation Mode: Incremental, 15 days lookback
    Lighthouse Dependancy Tables:
        slide.mfp_orders 21:00
*/


with fo as (
    select 
        order_id
        ,merchant_id
        ,city_id
        ,date(created_time) as date_local
        ,cast(comms_rate as double) as comms_rate
        ,tax_rate
        ,sub_total as sub_total
        ,case when comms_pre_vat = 0 then NULL else comms_pre_vat/currency_exponent end as comms_pre_vat
        ,sum(case when 
                json_extract_scalar(discount, '$.source') = 'campaign' and json_extract_scalar(discount, '$.deductedPart') <> 'delivery_fee' 
            then 
                cast(json_extract_scalar(discount, '$.mexFundedAmountExcludeTaxInMin') as double)/currency_exponent else 0 end) 
        as total_order_mfc_discount_affecting_comms /*exclude del fee discount*/
        ,sum(cast(json_extract_scalar(discount, '$.mexFundedAmountInMin') as double)/currency_exponent) as total_mfc_inc_tax
        ,currency_exponent
        ,partition_date
    from ( 
        select 
            *
            ,json_extract(snapshot_detail, '$.cartWithQuote.discounts') as array_discount
            ,power(double '10.0', coalesce(cast(json_extract_scalar(snapshot_detail, '$.currency.exponent') as int),0)) as currency_exponent
            ,coalesce(cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.mexCommissionPreVAT') as double), 0)
    + coalesce(cast(json_extract_scalar(snapshot_detail, '$.cartWithQuote.foodQuoteInMin.gkCommissionPreVAT') as double), 0) as comms_pre_vat
            ,json_extract_scalar(snapshot_detail,'$.cartWithQuote.merchantCartWithQuoteList[0].merchantInfoObj.commission') as comms_rate
            ,CAST(json_extract_scalar(snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList[0].merchantInfoObj.taxRate') AS double) AS tax_rate
        from public.prejoin_food_order 
        where date(partition_date) >= date([[inc_start_date]]) - interval '1' day
            AND date(partition_date) <= DATE([[inc_end_date]]) + interval '1' day
            AND date(created_time) >= date([[inc_start_date]])
            AND date(created_time) <= date([[inc_end_date]])
            and order_state = 11
    )
    cross join unnest(cast(array_discount AS array(json))) AS a(discount)
    group by 1,2,3,4,5,6,7,8,11,12 
)
select 
    order_id
    ,merchant_id
    ,city_id
    ,date_local
    ,comms_rate
    ,tax_rate
    ,sub_total
    ,case when date_local <= date('2019-10-30') then 
      coalesce(cast((sub_total-total_order_mfc_discount_affecting_comms)*comms_rate as double), comms_pre_vat) 
     else comms_pre_vat end as comms_pre_vat
    ,total_order_mfc_discount_affecting_comms
    ,total_mfc_inc_tax
    ,currency_exponent
    ,partition_date
from fo