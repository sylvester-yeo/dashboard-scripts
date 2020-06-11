/*v2*/
with first_brand_order_label as (
    select 
      aa.passenger_id
      ,aa.merchant_id
      ,aa.mex_city_name
      ,cast(min(pax_first_gf_order_date_by_chain) as date) AS first_brand_order_date
    from slide.grabfood_pax_mex_state aa 
    where date_trunc('week',date(aa.date_local)) >= date_trunc('week', date([[inc_end_date]])) - interval '28' day 
        and date_trunc('week',date(aa.date_local)) <= date_trunc('week', date([[inc_end_date]])) - interval '7' day 
        and date_trunc('week', date(aa.pax_first_gf_order_date_by_chain)) >= date_trunc('week', date([[inc_end_date]])) - interval '28' day 
        and date_trunc('week', date(aa.pax_first_gf_order_date_by_chain)) <= date_trunc('week', date([[inc_end_date]])) - interval '7' day 
        group by 1,2,3
)
,base_bookings as (
    select 
        bb.passenger_id
        ,bb.merchant_id
        ,cities.name as city_name
        ,countries.name as country_name
        ,cast(bb.date_local as date) as date_local
        ,mex.business_name
        ,bb.promo_expense
        ,bb.promo_code_expense
        ,first_brand_order_label.first_brand_order_date
        ,bb.basket_size
    from slide.datamart_bb_grabfood bb 
    left join public.cities on bb.city_id = cities.id
    LEFT JOIN public.countries ON cities.country_id = countries.id
    left join first_brand_order_label
        on bb.passenger_id = first_brand_order_label.passenger_id
        and bb.merchant_id = first_brand_order_label.merchant_id
        and cities.name = first_brand_order_label.mex_city_name
    left join slide.dim_merchants mex on bb.merchant_id = mex.merchant_id
    where date_trunc('week', date(bb.date_local)) >= date_trunc('week', date([[inc_end_date]])) - interval '28' day 
        and date_trunc('week', date(bb.date_local)) <= date_trunc('week', date([[inc_end_date]])) - interval '7' day 
        and booking_state_simple = 'COMPLETED'
        -- and bb.city_id = 6
)
,current_base as (
    select 
        bb.passenger_id
        ,bb.city_name
        ,country_name
        ,business_name
        ,date_trunc('week',date(date_local)) as week_of
        ,min(first_brand_order_date) as first_brand_order_date
        ,count(1) as no_of_completed_orders
        ,sum(case when promo_expense > 0 then 1 else 0 end) as no_of_promo_orders
        ,cast(min(date_local) as date) as min_date_local
        ,cast(max(date_local) as date) as max_date_local
        ,sum(promo_expense) as total_promo_expense
        ,sum(basket_size) as total_basket_size
    from base_bookings bb
    group by 1,2,3,4,5
)
,prev_weeks as (
    select 
        bb.passenger_id
        ,cities.name as city_name
        ,cities.country_id as country_id
        ,mex.business_name
        ,date_trunc('week',date(date_local)) as prev_week_of
        ,cast(max(date_local) as date) as max_date_local
        ,count(1) as no_of_prev_completed_orders
        ,sum(promo_expense) as total_promo_expense
        ,sum(basket_size) as total_basket_size
    from slide.datamart_bb_grabfood bb 
    left join public.cities on bb.city_id = cities.id
    left join slide.dim_merchants mex on bb.merchant_id = mex.merchant_id
    where date_trunc('week', date(date_local)) <= date_trunc('week', date([[inc_end_date]])) - interval '14' day
        and date_trunc('week', date(date_local)) >= date_trunc('week', date([[inc_end_date]])) - interval '56' day
        and booking_state_simple = 'COMPLETED'
    group by 1,2,3,4,5
)
,bb_cohort as (
    select 
        bb.passenger_id 
        -- ,pax_label.pax_label
        -- ,pax_label.max_date_local
        ,cities.name AS city_name
        ,mex.business_name
        ,countries.name AS country_name
        ,date_trunc('week', date(date_local)) as week_of
        ,cast(min(date_local) as date) as next_date_local
        ,count(1) AS no_of_orders
        ,sum(basket_size) as total_basket_size
        ,sum(promo_expense) as total_promo_expense
    from slide.datamart_bb_grabfood bb 
    left join slide.dim_merchants mex on bb.merchant_id = mex.merchant_id
    left join public.cities on bb.city_id = cities.id 
    left join public.countries on cities.country_id = countries.id
    where date(date_local) >= date_trunc('week', date([[inc_end_date]])) - interval '21' day 
        and date(date_local) <= date_trunc('week', date([[inc_end_date]])) + interval '21' day 
        and booking_state_simple = 'COMPLETED'
    GROUP BY 1,2,3,4,5
)
,current_prev_join as (
    SELECT
        current_base.passenger_id
        ,current_base.city_name
        ,current_base.country_name
        ,current_base.business_name
        ,current_base.week_of
        ,current_base.first_brand_order_date
        ,current_base.no_of_completed_orders
        ,current_base.no_of_promo_orders
        ,current_base.min_date_local
        ,current_base.max_date_local
        ,current_base.total_promo_expense
        ,current_base.total_basket_size
        ,max(prev_weeks.max_date_local) as prev_weeks_max_date_local
        ,sum(no_of_prev_completed_orders) as no_of_prev_completed_orders
        ,sum(case when datediff(bb_cohort.week_of, current_base.week_of) = 7 then bb_cohort.no_of_orders else NULL end) as week_1_no_of_orders
        ,sum(case when datediff(bb_cohort.week_of, current_base.week_of) = 7 then bb_cohort.total_basket_size else NULL end) as week_1_total_basket_size
        ,sum(case when datediff(bb_cohort.week_of, current_base.week_of) = 7 then bb_cohort.total_promo_expense else NULL end) as week_1_total_promo_expense
        ,sum(case when datediff(bb_cohort.week_of, current_base.week_of) = 14 then bb_cohort.no_of_orders else NULL end) as week_2_no_of_orders
        ,sum(case when datediff(bb_cohort.week_of, current_base.week_of) = 14 then bb_cohort.total_basket_size else NULL end) as week_2_total_basket_size
        ,sum(case when datediff(bb_cohort.week_of, current_base.week_of) = 14 then bb_cohort.total_promo_expense else NULL end) as week_2_total_promo_expense
        ,sum(case when datediff(bb_cohort.week_of, current_base.week_of) = 21 then bb_cohort.no_of_orders else NULL end) as week_3_no_of_orders
        ,sum(case when datediff(bb_cohort.week_of, current_base.week_of) = 21 then bb_cohort.total_basket_size else NULL end) as week_3_total_basket_size
        ,sum(case when datediff(bb_cohort.week_of, current_base.week_of) = 21 then bb_cohort.total_promo_expense else NULL end) as week_3_total_promo_expense
        ,sum(case when datediff(bb_cohort.week_of, current_base.week_of) = 28 then bb_cohort.no_of_orders else NULL end) as week_4_no_of_orders
        ,sum(case when datediff(bb_cohort.week_of, current_base.week_of) = 28 then bb_cohort.total_basket_size else NULL end) as week_4_total_basket_size
        ,sum(case when datediff(bb_cohort.week_of, current_base.week_of) = 28 then bb_cohort.total_promo_expense else NULL end) as week_4_total_promo_expense
    from current_base 
    left join prev_weeks
        on current_base.passenger_id = prev_weeks.passenger_id
        and current_base.city_name = prev_weeks.city_name
        and current_base.business_name = prev_weeks.business_name
        and datediff(current_base.week_of,prev_weeks.prev_week_of) >= 7 
        and datediff(current_base.week_of,prev_weeks.prev_week_of) <= 28
        and current_base.week_of > prev_weeks.prev_week_of 
    left join bb_cohort 
        on current_base.passenger_id = bb_cohort.passenger_id
        and current_base.city_name = bb_cohort.city_name
        and current_base.business_name = bb_cohort.business_name
        and current_base.week_of < bb_cohort.week_of 
        and datediff(bb_cohort.week_of, current_base.week_of) >= 7 
        and datediff(bb_cohort.week_of, current_base.week_of) <= 28 
    group by 1,2,3,4,5,6,7,8,9,10,11,12
)
,pax_label as (
    select 
        city_name
        ,country_name
        ,business_name
        ,week_of
        ,case
            when date_trunc('week',first_brand_order_date) = week_of then 'First Brand Order'
            when datediff(min_date_local,prev_weeks_max_date_local) <= 28 then 'Active Pax'
            else 'Revived Pax' end as pax_label
        ,count(1) as no_of_pax
        ,sum(no_of_completed_orders) as no_of_completed_orders
        ,sum(total_promo_expense) as total_promo_expense
        ,sum(total_basket_size) as total_basket_size
        ,sum(case when coalesce(week_1_no_of_orders,0) + coalesce(week_2_no_of_orders,0) + coalesce(week_3_no_of_orders,0) + coalesce(week_4_no_of_orders,0) = 0 then 1 else 0 end) as churned_pax
        ,sum(case when week_1_no_of_orders > 0 then 1 else 0 end) as week_1_pax
        ,sum(case when week_2_no_of_orders > 0 then 1 else 0 end) as week_2_pax
        ,sum(case when week_3_no_of_orders > 0 then 1 else 0 end) as week_3_pax
        ,sum(case when week_4_no_of_orders > 0 then 1 else 0 end) as week_4_pax
        ,sum(week_1_no_of_orders) as week_1_no_of_orders
        ,sum(week_1_total_basket_size) as week_1_total_basket_size
        ,sum(week_1_total_promo_expense) as week_1_total_promo_expense
        ,sum(week_2_no_of_orders) as week_2_no_of_orders
        ,sum(week_2_total_basket_size) as week_2_total_basket_size
        ,sum(week_2_total_promo_expense) as week_2_total_promo_expense
        ,sum(week_3_no_of_orders) as week_3_no_of_orders
        ,sum(week_3_total_basket_size) as week_3_total_basket_size
        ,sum(week_3_total_promo_expense) as week_3_total_promo_expense
        ,sum(week_4_no_of_orders) as week_4_no_of_orders
        ,sum(week_4_total_basket_size) as week_4_total_basket_size
        ,sum(week_4_total_promo_expense) as week_4_total_promo_expense
    from current_prev_join
    group by 1,2,3,4,5
)
SELECT *, week_of as partition_date FROM pax_label