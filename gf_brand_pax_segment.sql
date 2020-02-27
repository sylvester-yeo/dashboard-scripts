with first_brand_order_label as (
    select 
      aa.passenger_id
      ,aa.merchant_id
      ,aa.mex_city_name
      ,date_trunc('week', date(aa.date_local)) as week_of
      ,cast(min(pax_first_gf_order_date_by_chain) as date) AS first_brand_order_date
    from slide.grabfood_pax_mex_state aa 
    where date_trunc('week', date(aa.pax_first_gf_order_date_by_chain)) = date_trunc('week', date([[inc_start_date]]))
    --   and aa.mex_country_id = 4
        and date(aa.date_local) >= date([[inc_start_date]]) - interval '14' day
        and date(aa.date_local) <= date([[inc_start_date]]) + interval '7' day
        group by 1,2,3,4
)
,base_bookings as (
    select 
        bb.passenger_id
        ,bb.merchant_id
        ,cities.name as city_name
        ,cities.country_id as country_id
        ,cast(bb.date_local as date) as date_local
        ,mex.business_name
        ,bb.promo_expense
        ,bb.promo_code_expense
        ,first_brand_order_label.first_brand_order_date
        ,bb.basket_size
    from slide.datamart_bb_grabfood bb 
    left join public.cities on bb.city_id = cities.id
    -- left join public.countries on cities.country_id = countries.id
    left join first_brand_order_label
        on bb.passenger_id = first_brand_order_label.passenger_id
        and bb.merchant_id = first_brand_order_label.merchant_id
        and cities.name = first_brand_order_label.mex_city_name
    left join slide.dim_merchants mex on bb.merchant_id = mex.merchant_id
    where date_trunc('week', date(bb.date_local)) = date_trunc('week', date([[inc_start_date]]))
        and booking_state_simple = 'COMPLETED'
        -- and bb.city_id = 6
)
,current_base as (
    select 
        bb.passenger_id
        ,bb.city_name
        ,bb.country_id
        ,business_name
        ,min(first_brand_order_date) as first_brand_order_date
        ,count(1) as no_of_completed_orders
        ,sum(case when promo_expense > 0 then 1 else 0 end) as no_of_promo_orders
        ,cast(min(date_local) as date) as min_date_local
        ,cast(max(date_local) as date) as max_date_local
        ,sum(promo_expense) as total_promo_expense
        ,sum(basket_size) as total_basket_size
    from base_bookings bb
    group by 1,2,3,4
)
,prev_eight_weeks as (
    select 
        bb.passenger_id
        ,cities.name as city_name
        ,cities.country_id as country_id
        ,mex.business_name
        ,cast(max(date_local) as date) as max_date_local
        ,count(1) as no_of_prev_completed_orders
        ,sum(promo_expense) as total_promo_expense
        ,sum(basket_size) as total_basket_size
    from slide.datamart_bb_grabfood bb 
    left join public.cities on bb.city_id = cities.id
    left join slide.dim_merchants mex on bb.merchant_id = mex.merchant_id
    where date_trunc('week', date(date_local)) >= date([[inc_start_date]]) - interval '28' day
        and date(date_local) < date([[inc_start_date]])
        and booking_state_simple = 'COMPLETED'
        -- and bb.city_id = 6 
    group by 1,2,3,4
)
,next_four_weeks as (
    select 
        bb.passenger_id
        ,mex.business_name
        ,cities.name as city_name
        ,cities.country_id as country_id
        ,count(1) as no_of_orders
        ,cast(max(date_local) as date) as max_date_local
        ,cast(min(date_local) as date) as next_date_local
        ,sum(promo_expense) as total_promo_expense
        ,sum(basket_size) as total_basket_size
    from slide.datamart_bb_grabfood bb 
    left join public.cities on bb.city_id = cities.id
    left join slide.dim_merchants mex on bb.merchant_id = mex.merchant_id
    where date_trunc('week', date(date_local)) >= date([[inc_start_date]]) + interval '7' day
        and date_trunc('week', date(date_local)) < date([[inc_start_date]]) + interval '62' day
        and booking_state_simple = 'COMPLETED'
        -- and bb.city_id = 6 
    group by 1,2,3,4
)
,pax_label as (
    select
        current_base.passenger_id
        ,current_base.business_name
        ,current_base.city_name
        ,current_base.country_id
        ,case
            when date_trunc('week', current_base.first_brand_order_date) = date_trunc('week', date([[inc_start_date]])) then 'First Brand Order'
            when datediff(current_base.min_date_local,prev_eight_weeks.max_date_local) <= 28 then 'Active Pax'
            else 'Revived Pax' end as pax_label
        ,case 
            when next_four_weeks.passenger_id is null then 'Churn' 
            when datediff(next_four_weeks.next_date_local,current_base.max_date_local) <= 28 then 'Active'
            when datediff(next_four_weeks.next_date_local,current_base.max_date_local) >= 28 then 'Revived'
            else 'Error' end as future_label
        ,current_base.no_of_completed_orders
        ,current_base.no_of_promo_orders
        ,prev_eight_weeks.no_of_prev_completed_orders
        ,current_base.max_date_local as last_order_in_the_week
        ,next_four_weeks.next_date_local
        ,next_four_weeks.no_of_orders as next_four_weeks_no_of_orders
        ,current_base.total_basket_size
        ,current_base.total_promo_expense
        ,next_four_weeks.total_basket_size as next_four_weeks_basket_size
        ,prev_eight_weeks.total_basket_size as prev_eight_weeks_basket_size
    from current_base
    left join prev_eight_weeks 
        on current_base.passenger_id = prev_eight_weeks.passenger_id
        and current_base.business_name = prev_eight_weeks.business_name
        and current_base.city_name = prev_eight_weeks.city_name
    left join next_four_weeks 
        on current_base.passenger_id = next_four_weeks.passenger_id
        and current_base.business_name = next_four_weeks.business_name
        and current_base.city_name = next_four_weeks.city_name
)
select 
    *
    ,week_of as partition_date
from (
    select
        business_name
        ,countries.name as country_name
        ,city_name
        ,date_trunc('week',date([[inc_start_date]])) as week_of
        ,pax_label
        ,future_label
        ,count(1) as no_of_pax
        ,sum(no_of_completed_orders) as no_of_completed_orders_in_the_week
        ,sum(no_of_promo_orders) as no_of_promo_orders_in_the_week
        ,sum(no_of_prev_completed_orders) as no_of_prev_completed_orders
        ,sum(next_four_weeks_no_of_orders) as next_four_weeks_no_of_orders
        ,avg(datediff(next_date_local,last_order_in_the_week)) as avg_reorder_duration
        ,sum(total_basket_size) as total_basket_size
        ,sum(total_promo_expense) as total_promo_expense
        ,sum(next_four_weeks_basket_size) as next_four_weeks_basket_size
        ,sum(prev_eight_weeks_basket_size) as prev_eight_weeks_basket_size
    from pax_label
    left join public.countries on pax_label.country_id = countries.id 
    group by 1,2,3,4,5,6
)