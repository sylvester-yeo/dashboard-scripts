with base as (
    select 
        country_name as country
        ,city_name as city
        ,date_local
        ,date_trunc('week',date(date_local)) as week_of
        ,date_trunc('month',date(date_local)) as month_of
        ,sum(coalesce(avg_ad_spend_usd,0)) as ad_spend_usd
        ,sum(coalesce(avg_ad_spend_local,0)) as ad_spend_local
    from slide.gf_ads_mex_daily
    /*where partition_date >= date_trunc('month', date([[inc_start_date]])) - interval '1' month
        and partition_date >= date_trunc('month', date([[inc_end_date]]) + interval '1' month)*/
    where country_name is not null
    group by 1,2,3,4,5
)
select 
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

    ,'All' as business_model

    ,'All' as cashless_status

    ,'All' as partner_status

    ,sum(coalesce(ad_spend_usd,0)) AS ad_spend_usd
    ,sum(ad_spend_local) AS ad_spend_local
from base 
GROUP BY GROUPING SETS
    (
        (date_local,country),(week_of,country),(month_of,country),
    
        (date_local,country,city),(week_of,country,city),(month_of,country,city),
        
        (date_local),(week_of),(month_of)
        
    )