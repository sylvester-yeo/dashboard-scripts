with first_brand_order_label as (
    select 
      aa.passenger_id
      ,aa.merchant_id
      ,aa.mex_city_name
      ,date_trunc('week', date(aa.date_local)) as week_of
      ,cast(min(pax_first_gf_order_date_by_chain) as date) AS first_brand_order_date
    from slide.grabfood_pax_mex_state aa 
    where date_trunc('week', date(aa.pax_first_gf_order_date_by_chain)) = date_trunc('week', date('2019-12-02'))
    --   and aa.mex_country_id = 4
        and date(aa.date_local) >= date('2019-12-02') - interval '14' day
        and date(aa.date_local) <= date('2019-12-02') + interval '7' day
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
    from datamart_grabfood.base_bookings bb 
    left join public.cities on bb.city_id = cities.id
    -- left join public.countries on cities.country_id = countries.id
    left join first_brand_order_label
        on bb.passenger_id = first_brand_order_label.passenger_id
        and bb.merchant_id = first_brand_order_label.merchant_id
        and cities.name = first_brand_order_label.mex_city_name
    left join slide.dim_merchants mex on bb.merchant_id = mex.merchant_id
    where date_trunc('week', date(bb.date_local)) = date_trunc('week', date('2019-12-02'))
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
    from datamart_grabfood.base_bookings bb 
    left join public.cities on bb.city_id = cities.id
    left join slide.dim_merchants mex on bb.merchant_id = mex.merchant_id
    where date_trunc('week', date(date_local)) >= date('2019-12-02') - interval '28' day
        and date(date_local) < date('2019-12-02')
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
            when date_trunc('week', current_base.first_brand_order_date) = date_trunc('week', date('2019-12-02')) then 'First Brand Order'
            when datediff(current_base.min_date_local,prev_eight_weeks.max_date_local) <= 28 then 'Active Pax'
            else 'Revived Pax' end as pax_label
        ,current_base.no_of_completed_orders
        ,current_base.no_of_promo_orders
        ,current_base.total_basket_size
        ,current_base.total_promo_expense       
        ,prev_eight_weeks.no_of_prev_completed_orders
        ,current_base.max_date_local
    from current_base
    left join prev_eight_weeks 
        on current_base.passenger_id = prev_eight_weeks.passenger_id
        and current_base.business_name = prev_eight_weeks.business_name
        and current_base.city_name = prev_eight_weeks.city_name
)
,bb_cohort as (
    select 
        pax_label.pax_label
        ,pax_label.passenger_id
        ,mex.business_name
        ,bb.city_name 
        ,bb.country_name
        ,pax_label.max_date_local
        ,cast(min(date_local) as date) as next_date_local
        ,sum(case when date_trunc('week', date(date_local)) = date_trunc('week', date('2019-12-02') + interval '7' day) then 1 else 0 end) as first_week_completed_order
        ,sum(case when date_trunc('week', date(date_local)) = date_trunc('week', date('2019-12-02') + interval '14' day) then 1 else 0 end) as second_week_completed_order
        ,sum(case when date_trunc('week', date(date_local)) = date_trunc('week', date('2019-12-02') + interval '21' day) then 1 else 0 end) as third_week_completed_order
        ,sum(case when date_trunc('week', date(date_local)) = date_trunc('week', date('2019-12-02') + interval '28' day) then 1 else 0 end) as fourth_week_completed_order
        ,sum(case when date_trunc('week', date(date_local)) = date_trunc('week', date('2019-12-02') + interval '35' day) then 1 else 0 end) as fifth_week_completed_order
        ,sum(case when date_trunc('week', date(date_local)) = date_trunc('week', date('2019-12-02') + interval '42' day) then 1 else 0 end) as sixth_week_completed_order
        ,sum(case when date_trunc('week', date(date_local)) = date_trunc('week', date('2019-12-02') + interval '49' day) then 1 else 0 end) as seventh_week_completed_order
        ,sum(case when date_trunc('week', date(date_local)) = date_trunc('week', date('2019-12-02') + interval '56' day) then 1 else 0 end) as eighth_week_completed_order
        ,sum(pax_label.no_of_completed_orders) as current_week_no_of_completed_orders
        ,sum(pax_label.no_of_promo_orders) as current_week_no_of_promo_orders
        ,sum(pax_label.total_basket_size) as current_week_total_basket_size
        ,sum(pax_label.total_promo_expense) as current_week_total_promo_expense
        ,sum(pax_label.no_of_prev_completed_orders) as current_week_no_of_prev_completed_orders
    from datamart_grabfood.base_bookings bb
    left join slide.dim_merchants mex on bb.merchant_id = mex.merchant_id
    left join public.cities on cities.id = bb.city_id 
    inner join pax_label 
        on pax_label.passenger_id = bb.passenger_id
        and pax_label.business_name = mex.business_name
        and pax_label.city_name = cities.name
    where date(date_local) >= date('2019-12-02') + interval '7' day 
        and date(date_local) < date('2019-12-02') + interval '63' day 
        and booking_state_simple = 'COMPLETED'
        -- and bb.city_id = 6
    group by 1,2,3,4,5,6
)
select *, week_of as partition_date from (
    select 
        pax_label
        ,business_name
,city_name
,country_name
        ,date_trunc('week', date('2019-12-02')) as week_of
        ,count(1) as no_of_pax
        ,count(case when first_week_completed_order > 0 then passenger_id else null end) as first_week
        ,count(case when first_week_completed_order > 0 and second_week_completed_order > 0 then passenger_id else null end) as second_week
        ,count(case when first_week_completed_order > 0 and second_week_completed_order > 0 and third_week_completed_order > 0 then passenger_id else null end) as third_week
        ,count(case when first_week_completed_order > 0 and second_week_completed_order > 0 and third_week_completed_order > 0 and fourth_week_completed_order > 0 then passenger_id else null end) as fourth_week
        ,count(case when first_week_completed_order > 0 and second_week_completed_order > 0 and third_week_completed_order > 0 and fourth_week_completed_order > 0 and fifth_week_completed_order > 0 then passenger_id else null end) as fifth_week
        ,count(case when first_week_completed_order > 0 and second_week_completed_order > 0 and third_week_completed_order > 0 and fourth_week_completed_order > 0 and fifth_week_completed_order > 0 and sixth_week_completed_order > 0 then passenger_id else null end) as sixth_week
        ,count(case when first_week_completed_order > 0 and second_week_completed_order > 0 and third_week_completed_order > 0 and fourth_week_completed_order > 0 and fifth_week_completed_order > 0 and sixth_week_completed_order > 0 and seventh_week_completed_order > 0 then passenger_id else null end) as seventh_week
        ,count(case when first_week_completed_order > 0 and second_week_completed_order > 0 and third_week_completed_order > 0 and fourth_week_completed_order > 0 and fifth_week_completed_order > 0 and sixth_week_completed_order > 0 and seventh_week_completed_order > 0 and eighth_week_completed_order > 0 then passenger_id else null end) as eighth_week
        ,sum(current_week_no_of_completed_orders) as current_week_no_of_completed_orders
        ,sum(current_week_no_of_promo_orders) as current_week_no_of_promo_orders
        ,sum(current_week_total_basket_size) as current_week_total_basket_size
        ,sum(current_week_total_promo_expense) as current_week_total_promo_expense
        ,sum(current_week_no_of_prev_completed_orders) as current_week_no_of_prev_completed_orders
        ,sum(case when datediff(next_date_local, max_date_local) >= 28 then 1 else 0 end) as churned_pax
    from bb_cohort
    group by 1,2,3,4,5
)