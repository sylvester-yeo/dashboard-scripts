with mex_funding_table_combined as (
  SELECT * FROM 
  (SELECT
    country
    ,city
    ,date_local
    ,date_trunc('week', date(date_local)) as week_of
    ,date_trunc('month', date(date_local)) as month_of
    ,'All' as business_model
    ,'All' as cashless_status
    ,(mex_funding_amount_perday_local)
    ,(mex_funding_amount_perday_usd) 
  FROM
    slide.mex_funded_promo_code_by_brand_cg
  WHERE
    country IS NOT NULL
    and date_local < date('2019-07-01'))
)
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

    ,'All' as business_model

    ,'All' as cashless_status

    ,sum(mex_funding_amount_perday_local) AS mex_funding_amount_perday_local
    ,sum(mex_funding_amount_perday_usd) AS mex_funding_amount_perday_usd

FROM mex_funding_table_combined

GROUP BY GROUPING SETS
    (
        (date_local,country),(week_of,country),(month_of,country),
        
        (date_local,country,city),(week_of,country,city),(month_of,country,city),
        
        (date_local),(week_of),(month_of)		
    )