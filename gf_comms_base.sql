with fo as (
    select 
        order_id
        ,last_booking_code
        ,coalesce(last_booking_code, order_id) as key
        ,case when json_extract_scalar(CAST(json_extract(fo.snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList') AS ARRAY(json))[1], '$.merchantInfoObj.specialMerchantType') in ('GRABKITCHENMIXMATCH') then TRUE else FALSE end as is_grabkitchen_mixmatch
        ,json_extract(CAST(json_extract(fo.snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList') AS ARRAY(json))[1], '$.subMerchants') as mixmatch_sub_merchants
        ,cast(json_extract_scalar(fo.snapshot_detail, '$.cartWithQuote.merchantCartWithQuoteList[0].merchantInfoObj.commission') as double) as comms_rate
        ,cast(json_extract_scalar(fo.snapshot_detail, '$.cartWithQuote.foodQuoteInMin.mexCommissionPreVAT') as double) / power(double '10.0', coalesce(cast(json_extract_scalar(snapshot_detail, '$.currency.exponent') as int),0)) comms_pre_vat
        ,(case 
            when cities.country_id in (1,2) then cast(json_extract_scalar(fo.snapshot_detail, '$.cartWithQuote.foodQuoteInMin.mexCommission') as double) --(MY,PH)
            when cities.country_id in (3,4) then cast(json_extract_scalar(fo.snapshot_detail, '$.cartWithQuote.foodQuoteInMin.mexCommission') as double)/1.07 --(TH, SG)
            when cities.country_id in (5,6) then cast(json_extract_scalar(fo.snapshot_detail, '$.cartWithQuote.foodQuoteInMin.mexCommission') as double)/1.1 --(VN, ID)
        end) / power(double '10.0', coalesce(cast(json_extract_scalar(snapshot_detail, '$.currency.exponent') as int),0)) as old_comms_pre_vat
        ,coalesce(cast(json_extract_scalar(snapshot_detail, '$.currency.exponent') as int) ,0) as currency_exponent
        ,cast(json_extract_scalar(fo.snapshot_detail, '$.cartWithQuote.foodQuoteInMin.priceExcludeTaxInMinorUnit') as double) as priceExcludeTaxInMinorUnit
        ,case when json_extract_scalar(fo.snapshot_detail, '$.newTaxFlow') = 'true' then true else false end as newTaxFlow
        ,cast(json_extract_scalar(fo.snapshot_detail, '$.cartWithQuote.foodQuoteInMin.priceAfterCampaignInMinorUnit') as double) as priceAfterCampaignInMinorUnit
        , coalesce(cast(json_extract_scalar(fo.snapshot_detail, '$.cartWithQuote.foodQuoteInMin.inclTaxInMin') as double),0) as taxValue
        ,json_extract(snapshot_detail, '$.cartWithQuote.discounts') as array_discount
        ,date(created_time) as date_local
    from public.prejoin_food_order fo
    left join public.cities on fo.city_id = cities.id
    where date(partition_date) >= date([[inc_start_date]]) - interval '1' day
        AND date(partition_date) <= DATE([[inc_end_date]]) + interval '1' day
        AND date(created_time) >= date([[inc_start_date]])
        AND date(created_time) <= date([[inc_end_date]]) 
)
,fo_gkmm as (
    select 
        order_id
        ,last_booking_code
        ,key 
        ,json_extract_scalar(sub_merchant, '$.merchantID') as merchant_id
        ,cast(json_extract_scalar(sub_merchant, '$.merchantInfoObj.gkCommissionRate') as double) as gk_comms_rate
        ,cast(json_extract_scalar(sub_merchant, '$.subFoodQuoteInMin.gkCommissionPreVAT') as double) / power(double '10.0', currency_exponent) as gk_comms_pre_vat
        ,cast(json_extract_scalar(sub_merchant, '$.merchantInfoObj.commission') as double) as gf_comms_rate
        ,cast(json_extract_scalar(sub_merchant, '$.subFoodQuoteInMin.mexCommissionPreVAT') as double) / power(double '10.0', currency_exponent) as gf_comms_pre_vat
    from fo 
    CROSS JOIN UNNEST(CAST(COALESCE(fo.mixmatch_sub_merchants, cast(array['No Sub Merchants'] AS json)) AS array(json))) AS a(sub_merchant)
    where is_grabkitchen_mixmatch
)
,fo_gkmm_agg as (
    select 
        order_id
        ,last_booking_code
        ,key
        ,sum(gf_comms_pre_vat/gf_comms_rate) as total_base_from_gf
        ,sum(gk_comms_pre_vat/gk_comms_rate) as total_base_from_gk
    from fo_gkmm
    group by 1,2,3
)
select 
    fo.order_id
    ,fo.last_booking_code
    ,fo.key
    ,case when fo_gkmm_agg.key is not null then 1 else 0 end as is_gkmm
    ,round(coalesce(fo_gkmm_agg.total_base_from_gf, fo.comms_pre_vat/fo.comms_rate, fo.old_comms_pre_vat/fo.comms_rate),3) as gf_base
    ,round(total_base_from_gk,3) as gk_base
    ,case 
        when newTaxFlow then (fo.priceAfterCampaignInMinorUnit - fo.taxValue) / power(double '10.0', currency_exponent)
        else fo.priceAfterCampaignInMinorUnit / power(double '10.0', currency_exponent) end as priceAfterCampaignInMinorUnit
    ,case 
        when newTaxFlow then (fo.priceExcludeTaxInMinorUnit - taxValue) / power(double '10.0', currency_exponent)
        else fo.priceExcludeTaxInMinorUnit / power(double '10.0', currency_exponent) end as priceExcludeTaxInMinorUnit
    ,date_local
from fo
left join fo_gkmm_agg on fo.key = fo_gkmm_agg.key 