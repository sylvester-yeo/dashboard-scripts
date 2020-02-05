with mex_funding_table_combined as (
  select 
    country
    ,city
    ,date(date_local) as date_local
    ,date_trunc('week', date(date_local)) as week_of
    ,date_trunc('month', date(date_local)) as month_of
    ,business_model
    ,cashless_status
    ,partner_status
    ,sum(mex_mfp_spend_local) as mex_funding_amount_perday_local
    ,sum(mex_mfp_spend_usd) as mex_funding_amount_perday_usd

    ,sum(mex_promo_spend_non_mfp_local) as mex_promo_spend_non_mfp_local
    ,sum(mex_promo_spend_non_mfp_usd) as mex_promo_spend_non_mfp_usd
   from slide.gf_mfp_merchant
    where date(date_local) >= date('2019-07-01')
    and country is not null
    group by 1,2,3,4,5,6,7,8
)
SELECT * FROM (
SELECT 
    case
        when country is null and city is null then 'By Region'
        when country is not null and city is null then 'By Country'
        when city is not null then 'By City'
    end as by_city_country

    ,case 
        when date_local is not null then 'By Day'
        when week_of is not null then 'By Week'
        when month_of is not null then 'By Month'
    end as by_day_week_month

    ,case
        when date_local is not null then date_local
        when week_of is not null then week_of
        when month_of is not null then month_of
    end as time_period

    ,case 
        when country is null and city is null then 'All'
        else country 
    end as country 

    ,case 
        when city is null then 'All'
        else city 
    end as city

    ,case 
        when business_model is null then 'All'
        else business_model
    end as business_model

    ,case 
        when cashless_status is null then 'All'
        else cashless_status
    end as cashless_status

    ,case 
        when partner_status is null then 'All'
        else partner_status 
    end as partner_status

    ,sum(mex_funding_amount_perday_local) AS mex_funding_amount_perday_local
    ,sum(mex_funding_amount_perday_usd) AS mex_funding_amount_perday_usd

    ,sum(mex_promo_spend_non_mfp_local) as mex_promo_spend_non_mfp_local
    ,sum(mex_promo_spend_non_mfp_usd) as mex_promo_spend_non_mfp_usd

FROM mex_funding_table_combined

GROUP BY GROUPING SETS
    (
        (date_local,country),(week_of,country),(month_of,country),
        (date_local,country,business_model),(week_of,country,business_model),(month_of,country,business_model),
        (date_local,country,partner_status),(week_of,country,partner_status),(month_of,country,partner_status),
        (date_local,country,business_model,partner_status),(week_of,country,business_model,partner_status),(month_of,country,business_model,partner_status),
        (date_local,country,cashless_status),(week_of,country,cashless_status),(month_of,country,cashless_status),
        (date_local,country,business_model,cashless_status),(week_of,country,business_model,cashless_status),(month_of,country,business_model,cashless_status),
        (date_local,country,business_model,cashless_status,partner_status),(week_of,country,business_model,cashless_status,partner_status),(month_of,country,business_model,cashless_status,partner_status),
        
        
        (date_local,country,city),(week_of,country,city),(month_of,country,city),
        (date_local,country,city,business_model),(week_of,country,city,business_model),(month_of,country,city,business_model),
        (date_local,country,city,partner_status),(week_of,country,city,partner_status),(month_of,country,city,partner_status),
        (date_local,country,city,cashless_status),(week_of,country,city,cashless_status),(month_of,country,city,cashless_status),
        (date_local,country,city,business_model,cashless_status),(week_of,country,city,business_model,cashless_status),(month_of,country,city,business_model,cashless_status),
        (date_local,country,city,business_model,cashless_status,partner_status),(week_of,country,city,business_model,cashless_status,partner_status),(month_of,country,city,business_model,cashless_status,partner_status),
        (date_local,country,city,business_model,partner_status),(week_of,country,city,business_model,partner_status),(month_of,country,city,business_model,partner_status),
        
        
        (date_local),(week_of),(month_of),
        (date_local,business_model),(week_of,business_model),(month_of,business_model),
        (date_local,partner_status),(week_of,partner_status),(month_of,partner_status),
        (date_local,cashless_status),(week_of,cashless_status),(month_of,cashless_status),	
        (date_local,business_model,cashless_status),(week_of,business_model,cashless_status),(month_of,business_model,cashless_status),				
        (date_local,business_model,cashless_status,partner_status),(week_of,business_model,cashless_status,partner_status),(month_of,business_model,cashless_status,partner_status),				
        (date_local,business_model,partner_status),(week_of,business_model,partner_status),(month_of,business_model,partner_status)				
    )
)
UNION ALL 
(
    SELECT  
        by_city_country
        ,by_day_week_month
        ,time_period
        ,country
        ,city
        ,business_model
        ,cashless_status
        ,'All' as partner_status
        ,mex_funding_amount_perday_local
        ,mex_funding_amount_perday_usd
        ,null as mex_promo_spend_non_mfp_local
        ,null as mex_promo_spend_non_mfp_usd
    FROM slide.gf_mfp_redemption_aggregated_pre_july_2019
)