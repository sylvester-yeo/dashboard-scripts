/*
    Name: slide.gf_mbp_tsp_aggregated
    Refresh Time: Daily, 21:00 UTC
    Aggregation Mode: Incremental, 10 days windows
    Lighthouse Dependancy Tables:
*/
with daily as (
  SELECT
    cities.name as city_name
    ,countries.name as country_name
    ,mbp.date_local
    ,date_trunc('month', mbp.date_local) as month_of
    ,date_trunc('week', mbp.date_local) as week_of
    ,state
    ,a.cashless_status
    ,a.business_model
    ,a.partner_status
    ,sum(mbp_paid_by_mex) as mbp_paid_by_mex_local
    ,sum(mbp_paid_by_pax) as mbp_paid_by_pax_local
    ,sum(tsp_paid_by_us) as tsp_paid_by_us_local
    ,sum(mbp_paid_by_mex/fx_one_usd) as mbp_paid_by_mex_usd
    ,sum(mbp_paid_by_pax/fx_one_usd) as mbp_paid_by_pax_usd
    ,sum(tsp_paid_by_us/fx_one_usd) as tsp_paid_by_us_usd

  FROM slide.gf_tsp_mbp_breakdown mbp
  LEFT JOIN public.cities
    on mbp.city_id = cities.id
  left join public.countries
    on cities.country_id = countries.id
  INNER JOIN(
    SELECT 
        booking_code,
        is_partner_merchant,
        order_id,
        fx_one_usd,
        CASE WHEN is_grabpay=1 THEN 'Cashless' ELSE 'Cash' END AS cashless_status,
        CASE WHEN is_integrated_model = 1 THEN 'Integrated' ELSE 'Concierge' END AS business_model,
        case when is_partner_merchant = 1 then 'Partner' else 'Non-Partner' end as partner_status
    FROM datamart_grabfood.base_bookings
    WHERE date_trunc('month', date(date_local)) >= date_add('month', -1, date_trunc('month',date([[inc_start_date]])))
        and date_trunc('month', date(date_local)) < date_add('month', 1, date_trunc('month',date([[inc_end_date]])))
        and booking_state_simple = 'COMPLETED'
  ) a
  ON mbp.booking_code = a.booking_code
  WHERE
    mbp.state = 'COMPLETED'
        and date_trunc('month', date(mbp.date_local)) >= date_add('month', -1, date_trunc('month',date([[inc_start_date]])))
        and date_trunc('month', date(mbp.date_local)) < date_add('month', 1, date_trunc('month',date([[inc_end_date]])))
  GROUP BY 1,2,3,4,5,6,7,8,9
)
SELECT * FROM (
SELECT
  CASE
    WHEN country_name IS NULL AND city_name IS NULL THEN 'By Region'
    WHEN country_name IS NOT NULL AND city_name IS NULL THEN 'By Country'
    WHEN city_name IS NOT NULL THEN 'By City'
  END AS by_city_country
  ,CASE
    WHEN date_local IS NOT NULL THEN 'By Day'
    WHEN week_of IS NOT NULL THEN 'By Week'
    WHEN month_of IS NOT NULL THEN 'By Month'
  END AS by_day_week_month
  ,CASE
    WHEN date_local IS NOT NULL THEN date_local
    WHEN week_of IS NOT NULL THEN week_of
    WHEN month_of IS NOT NULL THEN month_of
  END AS time_period
  ,CASE
      WHEN country_name IS NULL AND city_name IS NULL THEN 'All' ELSE country_name
  END AS country
  ,CASE
     WHEN city_name IS NULL THEN 'All' ELSE city_name
  END AS city
  ,CASE
        WHEN business_model IS NULL THEN 'All'
        ELSE business_model
    END AS business_model
  ,CASE
        WHEN cashless_status IS NULL THEN 'All'
        ELSE cashless_status
    END AS cashless_status

    ,case
        when partner_status is null then 'All'
        else partner_status
    end as partner_status 

    ,sum(mbp_paid_by_mex_local) as mbp_paid_by_mex_local
    ,sum(mbp_paid_by_pax_local) as mbp_paid_by_pax_local
    ,sum(tsp_paid_by_us_local) as tsp_paid_by_us_local
    ,sum(mbp_paid_by_mex_usd) as mbp_paid_by_mex_usd
    ,sum(mbp_paid_by_pax_usd) as mbp_paid_by_pax_usd
    ,sum(tsp_paid_by_us_usd) as tsp_paid_by_us_usd
FROM daily

GROUP by grouping sets
(
    (date_local,country_name),(week_of,country_name),(month_of,country_name),
    (date_local,country_name,business_model),(week_of,country_name,business_model),(month_of,country_name,business_model),
    (date_local,country_name,partner_status),(week_of,country_name,partner_status),(month_of,country_name,partner_status),
    (date_local,country_name,business_model,partner_status),(week_of,country_name,business_model,partner_status),(month_of,country_name,business_model,partner_status),
    (date_local,country_name,cashless_status),(week_of,country_name,cashless_status),(month_of,country_name,cashless_status),
    (date_local,country_name,business_model,cashless_status),(week_of,country_name,business_model,cashless_status),(month_of,country_name,business_model,cashless_status),
    (date_local,country_name,business_model,cashless_status,partner_status),(week_of,country_name,business_model,cashless_status,partner_status),(month_of,country_name,business_model,cashless_status,partner_status),
    
    
    (date_local,country_name,city_name),(week_of,country_name,city_name),(month_of,country_name,city_name),
    (date_local,country_name,city_name,business_model),(week_of,country_name,city_name,business_model),(month_of,country_name,city_name,business_model),
    (date_local,country_name,city_name,partner_status),(week_of,country_name,city_name,partner_status),(month_of,country_name,city_name,partner_status),
    (date_local,country_name,city_name,cashless_status),(week_of,country_name,city_name,cashless_status),(month_of,country_name,city_name,cashless_status),
    (date_local,country_name,city_name,business_model,cashless_status),(week_of,country_name,city_name,business_model,cashless_status),(month_of,country_name,city_name,business_model,cashless_status),
    (date_local,country_name,city_name,business_model,cashless_status,partner_status),(week_of,country_name,city_name,business_model,cashless_status,partner_status),(month_of,country_name,city_name,business_model,cashless_status,partner_status),
    (date_local,country_name,city_name,business_model,partner_status),(week_of,country_name,city_name,business_model,partner_status),(month_of,country_name,city_name,business_model,partner_status),
    
    
    (date_local),(week_of),(month_of),
    (date_local,business_model),(week_of,business_model),(month_of,business_model),
    (date_local,partner_status),(week_of,partner_status),(month_of,partner_status),
    (date_local,cashless_status),(week_of,cashless_status),(month_of,cashless_status),	
    (date_local,business_model,cashless_status),(week_of,business_model,cashless_status),(month_of,business_model,cashless_status),				
    (date_local,business_model,cashless_status,partner_status),(week_of,business_model,cashless_status,partner_status),(month_of,business_model,cashless_status,partner_status),				
    (date_local,business_model,partner_status),(week_of,business_model,partner_status),(month_of,business_model,partner_status)
)
)
WHERE 
time_period >= (CASE 
    WHEN by_day_week_month = 'By Day' THEN date([[inc_start_date]])
    WHEN by_day_week_month = 'By Week' THEN date_trunc('week',date([[inc_start_date]]))
    WHEN by_day_week_month = 'By Month' THEN date_trunc('month',date([[inc_start_date]]))
 END)