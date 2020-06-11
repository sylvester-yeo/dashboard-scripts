with fo as (
    select 
        order_id
        ,date_local
        ,gf_base
    from slide.gf_comms_base
    where date(date_local) >= date([[inc_start_date]])
        AND date(date_local) <= date([[inc_end_date]])
)
, bk_metrics as (
    SELECT
	   date(bookings.date_local) AS date_local
	   ,country_name
	   ,city_name
	  ,bookings.merchant_id
	  ,CASE WHEN bookings.is_partner_merchant= 1 THEN 'partner' ELSE 'non-partner' END AS restaurant_partner_status
	  ,CASE WHEN bookings.is_integrated_model = 1 THEN 'Integrated' ELSE 'Concierge' END AS business_model
	  ,CASE WHEN bookings.is_grabpay = 1 THEN 'Cashless' ELSE 'Cash' END AS cashless_status
	  ,CASE 
	  		WHEN bookings.is_gpc = 1 THEN 'GrabPay-GPC'
	  		WHEN bookings.is_grabpay=1 AND bookings.is_gpc=0 THEN 'GrabPay-Non GPC'
	  		WHEN bookings.is_grabpay = 0 THEN 'Cash'
	   END AS payment_type
	   
	  ,SUM(CASE 
	       WHEN bookings.booking_state_simple = 'COMPLETED' AND is_advance<>1 and is_takeaway <> 1 THEN CAST(bookings.time_to_complete AS double)/60.0 
	       ELSE 0.0 
	       END) AS time_from_order_create_to_completed
	       
	  ,SUM(CASE 
	       WHEN 
	       		bookings.time_to_complete IS NOT NULL AND bookings.booking_state_simple = 'COMPLETED' AND is_advance<>1 and is_takeaway <> 1
	       THEN 1 
	       ELSE 0 END) AS completed_orders_without_null_time_2
	       
	  ,SUM(1) AS all_incoming_orders_gf
	  
	  ,SUM(CASE 
	     WHEN 
	     	bookings.booking_state_simple NOT IN 
	     	('CANCELLED_PASSENGER_PRE_MEX_ACCEPTANCE','CANCELLED_OPERATOR_PRE_MEX_ACCEPTANCE','ORDER_EXPIRED_PRE_MEX_ACCEPTANCE')     	
	     THEN 1 
	     ELSE 0 END) AS mex_accepted_orders_gf
	     
	  ,SUM(CASE 
	       WHEN 
	       		bookings.booking_state_simple = 'COMPLETED' 
	       THEN 1 
	       ELSE 0 
	       END) AS completed_orders_gf
	       
	  ,sum(CASE 
	       WHEN 
	       		bookings.booking_state_simple = 'COMPLETED' and is_takeaway <> 1
	       THEN 
	       		cast(bookings.estimated_distance_of_trip AS double)/cast(1000 AS double) 
	       ELSE 0.0 
	       END) AS delivery_distance_gf
	       
	  ,sum(CASE 
	       WHEN 
	       		bookings.booking_state_simple = 'COMPLETED' and is_takeaway <> 1
	       THEN 
	       		cast(bookings.estimated_distance_to_arrival AS double)/cast(1000 AS double) 
	       ELSE 0.0 
	       END) AS driver_distance_gf
	  
	  ,sum(CASE 
	  	   WHEN 
	  	   		bookings.booking_state_simple = 'COMPLETED' and is_takeaway <> 1
	  	   THEN 
	  	   		pax_fare/fx_one_usd
	  	   ELSE 0.0 
	  	   END) AS delivery_fare_gf
	  	   
	  ,sum(CASE 
	  	   WHEN 
	  	   		bookings.booking_state_simple = 'COMPLETED' and is_takeaway <> 1
	  	   THEN 
	  	   		pax_fare 
	  	   ELSE 0.0 
	  	   END) AS delivery_fare_gf_local
	  
	  ,SUM(CASE 
	  	   WHEN 
	  	   		bookings.booking_state_simple = 'COMPLETED' and is_takeaway <> 1
	  	   THEN 
	  	   		bookings.dax_fare/fx_one_usd
	  	   ELSE 0.0 
	  	   END) as dax_delivery_fare
	  	   
	  ,SUM(CASE 
	  	   WHEN 
	  	   		bookings.booking_state_simple = 'COMPLETED' and is_takeaway <> 1
	  	   THEN 
	  	   		bookings.dax_fare 
	  	   ELSE 0.0 
	  	   END) as dax_delivery_fare_local  
	     
	  ,sum(CASE 
	  	   WHEN 
	  	   		bookings.booking_state_simple = 'COMPLETED' 
	  	   THEN bookings.gross_merchandise_value/fx_one_usd 
	  	   ELSE 0.0
	  	   END) AS gmv_usd_gf
	         
	  ,sum(CASE 
	       WHEN 
	       		bookings.booking_state_simple = 'COMPLETED'
	       THEN 
	       		bookings.gross_merchandise_value 
	       ELSE 0.0 
	       END) AS gmv_local           
	        
	  ,sum(CASE 
	  	   WHEN 
	  	   		bookings.booking_state_simple = 'COMPLETED' 
	       THEN 
	       		basket_size / fx_one_usd
	       ELSE 0.0
	       END) AS basket_size
	       
	  ,sum(CASE
	       WHEN 
	       		bookings.booking_state_simple = 'COMPLETED'
	       THEN 
	       		basket_size       
	       ELSE 0.0
	       END) AS basket_size_local
	       
	 ,sum(CASE
	  	  WHEN 
	  	  		bookings.booking_state_simple = 'COMPLETED'
	  	  THEN	
	  	  		bookings.food_sub_total / fx_one_usd
		  ELSE 0.0
	      END) AS sub_total
	      
	 ,SUM(CASE
	  	  WHEN 
	  	  		bookings.booking_state_simple = 'COMPLETED'
	  	  THEN
	      		bookings.food_sub_total
	  	  ELSE 0.0
	      END) AS sub_total_local
	  -- remove tax from commission calculation 2018-09-23
	  
	 ,sum(CASE
	      WHEN 
	  	  		bookings.booking_state_simple = 'COMPLETED'
	      THEN
	            bookings.commission_from_merchant / fx_one_usd
	     ELSE 0.0
	     END) AS mex_commission
	       
	 ,sum(CASE
	      WHEN 
	      		bookings.booking_state_simple = 'COMPLETED'
	      THEN
				bookings.commission_from_merchant
	     ELSE 0.0
	     END) AS mex_commission_local

    ,sum(CASE 
        WHEN 
            bookings.booking_state_simple = 'COMPLETED' 
        THEN 
            cast(coalesce(fo.gf_base,0) as double)
			-- cast(bookings.food_sub_total - coalesce(fo.total_order_mfc_discount_affecting_comms,0) as double)
        ELSE 0.0
        END) AS base_for_mex_commission_local

    ,sum(CASE 
        WHEN 
            bookings.booking_state_simple = 'COMPLETED' 
        THEN 
            cast(coalesce(fo.gf_base/fx_one_usd,0) as double)
            -- cast(bookings.food_sub_total - coalesce(fo.total_order_mfc_discount_affecting_comms,0) as double) / fx_one_usd
        ELSE 0.0
        END) AS base_for_mex_commission
	       
	 ,sum(CASE
	      WHEN 
	      		bookings.booking_state_simple = 'COMPLETED'
	      THEN
	      		bookings.commission_from_driver/fx_one_usd
	      ELSE 0.0
	      END) AS driver_commission
	      
	 ,sum(CASE
	      WHEN 
	      		bookings.booking_state_simple = 'COMPLETED'
	      THEN
	      		bookings.commission_from_driver
	      ELSE 0.0
	      END) AS driver_commission_local
	      
	,sum(CASE
	     WHEN 
	      		bookings.booking_state_simple = 'COMPLETED'
	     THEN
	     		bookings.promo_expense/fx_one_usd
	     ELSE 0.0
	     END) AS promo_expense
	     
	,sum(CASE
	     WHEN 
	      		bookings.booking_state_simple = 'COMPLETED'
	     THEN
	     		bookings.promo_expense   
	     ELSE 0.0
	     END) AS promo_expense_local
	
    ,sum(CASE
	     WHEN 
	      		bookings.booking_state_simple = 'COMPLETED'
	     THEN
	     		bookings.promo_code_expense/fx_one_usd
	     ELSE 0.0
	     END) AS promo_code_expense
	     
	,sum(CASE
	     WHEN 
	      		bookings.booking_state_simple = 'COMPLETED'
	     THEN
	     		bookings.promo_code_expense   
	     ELSE 0.0
	     END) AS promo_code_expense_local
	     
	,sum(bookings.is_promotion) AS promo_incoming_orders
	     
	,sum(CASE
	     WHEN 
	     		bookings.booking_state_simple = 'COMPLETED' 
	     THEN 
	     		bookings.is_promotion
	     ELSE 0
	     END) AS promo_completed_orders
	     
	,sum(CASE
		 WHEN bookings.booking_state_simple IN 
		('CANCELLED_PASSENGER_PRE_MEX_ACCEPTANCE','CANCELLED_OPERATOR_PRE_MEX_ACCEPTANCE',
		 'CANCELLED_PASSENGER_PRE_ALLOCATION','CANCELLED_OPERATOR_PRE_ALLOCATION',
		 'CANCELLED_DRIVER', 'CANCELLED_OPERATOR', 'CANCELLED_PASSENGER', 'CANCELLED_MERCHANT') 
		 THEN 1 
		 ELSE 0 END) AS cancellations
		
	,sum(CASE
		 WHEN 
		 	bookings.booking_state_simple IN 
		 	('CANCELLED_PASSENGER_PRE_MEX_ACCEPTANCE','CANCELLED_PASSENGER','CANCELLED_PASSENGER_PRE_ALLOCATION') 
		 THEN 1 
		 ELSE 0 END) AS cancellations_passenger
		
	,sum(CASE
		 WHEN 
		 	bookings.booking_state_simple IN ('CANCELLED_DRIVER') 
		 THEN 1 
		 ELSE 0 END) AS cancellations_driver
		
	,sum(CASE
		 WHEN 
		 	bookings.booking_state_simple IN 
		 	('CANCELLED_OPERATOR_PRE_MEX_ACCEPTANCE','CANCELLED_OPERATOR_PRE_ALLOCATION','CANCELLED_OPERATOR') 
		 THEN 1 
		 ELSE 0 END) AS cancellations_operator

	,sum(CASE
		 WHEN 
		 	bookings.booking_state_simple IN ('CANCELLED_MERCHANT') 
		 THEN 1 
		 ELSE 0 END) AS cancellations_merchant
		
	,SUM(is_allocated) AS allocated_orders
	
	,SUM(CASE 
	     WHEN 
	     	bookings.booking_state_simple IN ('UNALLOCATED')     	
	     THEN 1 
	     ELSE 0 END) AS unallocated_orders
	     
	,SUM(CASE 
	     WHEN 
	     	bookings.booking_state_simple IN 
	     	('CANCELLED_PASSENGER_PRE_MEX_ACCEPTANCE','CANCELLED_OPERATOR_PRE_MEX_ACCEPTANCE')     	
	     THEN 1 
	     ELSE 0 END) AS pre_accept_cancellations

	,SUM(CASE 
	     WHEN 
	     	bookings.booking_state_simple IN 
	     	('CANCELLED_PASSENGER_PRE_MEX_ACCEPTANCE')     	
	     THEN 1 
	     ELSE 0 END) AS pre_accept_cancellations_pax

	,SUM(CASE 
	     WHEN 
	     	bookings.booking_state_simple IN 
	     	('CANCELLED_OPERATOR_PRE_MEX_ACCEPTANCE')     	
	     THEN 1 
	     ELSE 0 END) AS pre_accept_cancellations_operator

	,SUM(CASE 
	     WHEN 
	     	bookings.booking_state_simple IN ('ORDER_EXPIRED_PRE_MEX_ACCEPTANCE')     	     	
	     THEN 1 
	     ELSE 0 END) AS pre_accept_expired_orders
	     
	,SUM(CASE 
	     WHEN 
	     	bookings.booking_state_simple IN 
	     	('CANCELLED_PASSENGER_PRE_ALLOCATION','CANCELLED_OPERATOR_PRE_ALLOCATION')     	
	     THEN 1 
	     ELSE 0 END) AS pre_allocation_cancellations

	,SUM(CASE 
	     WHEN 
	     	bookings.booking_state_simple IN 
	     	('CANCELLED_PASSENGER_PRE_ALLOCATION')     	
	     THEN 1 
	     ELSE 0 END) AS pre_allocation_cancellations_pax

	,SUM(CASE 
	     WHEN 
	     	bookings.booking_state_simple IN 
	     	('CANCELLED_OPERATOR_PRE_ALLOCATION')     	
	     THEN 1 
	     ELSE 0 END) AS pre_allocation_cancellations_operator
	     
	,SUM(is_first_allocated) AS first_allocated_orders
	
	,SUM(CASE 
	     WHEN 
	     	is_first_allocated = 1 AND booking_state_simple = 'COMPLETED'
	     THEN 1 
	     ELSE 0 END) AS effective_first_allocated_orders

	,sum(CASE
	     WHEN 
	     	booking_state_simple = 'COMPLETED'
	     THEN subsidy else 0 end) as tsp_subsidy_local

	,sum(CASE
	     WHEN 
	     	booking_state_simple = 'COMPLETED'
	     THEN subsidy/fx_one_usd else 0 end) as tsp_subsidy_usd

	,sum(incentives) AS incentives_local	     
	     
	,sum(incentives/fx_one_usd) AS incentives_usd

	,sum(CASE
	     WHEN 
	     	booking_state_simple = 'COMPLETED'
	     THEN 
	     	spot_incentive_bonus
	     ELSE 0 END) AS spot_incentive_bonus_local
	     
	,sum(CASE
	     WHEN 
	     	booking_state_simple = 'COMPLETED'
	     THEN 
	     	spot_incentive_bonus/fx_one_usd
	     ELSE 0 END) AS spot_incentive_bonus_usd
	
	,sum(CASE
	     WHEN 
	     	booking_state_simple = 'COMPLETED'
	     THEN small_order_fee else 0 end) as sof_local

	,sum(CASE
	     WHEN 
	     	booking_state_simple = 'COMPLETED'
	     THEN small_order_fee/fx_one_usd else 0 end) as sof_usd

	,sum(CASE
	     WHEN 
	     	booking_state_simple = 'COMPLETED'
	     THEN convenience_fee else 0 end) as convenience_fee_local

	,sum(CASE
	     WHEN 
	     	booking_state_simple = 'COMPLETED'
	     THEN convenience_fee/fx_one_usd else 0 end) as convenience_fee_usd

	,sum(CASE
	     WHEN 
	     	booking_state_simple = 'COMPLETED'
	     THEN pax_platform_fee else 0 end) as pax_platform_fee_local

	,sum(CASE
	     WHEN 
	     	booking_state_simple = 'COMPLETED'
	     THEN pax_platform_fee/fx_one_usd else 0 end) as pax_platform_fee_usd

    /*============takeaway orders============*/
    ,sum(case 
        when 
            is_takeaway = 1
        THEN 
            1 
        ELSE 0 
        END) AS total_takeaway_orders

    ,sum(case 
        when 
            is_takeaway = 1 and booking_state_simple = 'COMPLETED'
        THEN 
            1 
        ELSE 0 
        END) AS total_takeaway_completed_orders

    ,sum(case 
        when 
            is_takeaway = 1 and booking_state_simple = 'COMPLETED'
        THEN 
            gross_merchandise_value
        ELSE 0
        END) AS takeaway_gmv_local  

    ,sum(case 
        when 
            is_takeaway = 1 and booking_state_simple = 'COMPLETED'
        THEN 
            gross_merchandise_value / fx_one_usd
        ELSE 0
        END) AS takeaway_gmv_usd 

    ,sum(case 
        when 
            is_takeaway = 1 and booking_state_simple = 'COMPLETED'
        THEN 
            bookings.commission_from_merchant
        ELSE 0
        END) AS takeaway_mex_commission_local  

    ,sum(case 
        when 
            is_takeaway = 1 and booking_state_simple = 'COMPLETED'
        THEN 
			bookings.commission_from_merchant / fx_one_usd
        ELSE 0
        END) AS takeaway_mex_commission_usd 

    ,sum(CASE 
        WHEN 
            is_takeaway = 1 and bookings.booking_state_simple = 'COMPLETED' 
        THEN 
            cast(coalesce(fo.gf_base,0) as double)
			-- cast(bookings.food_sub_total - coalesce(fo.total_order_mfc_discount_affecting_comms,0) as double)
        ELSE 0.0
        END) AS takeaway_base_for_mex_commission_local

    ,sum(CASE 
        WHEN 
            is_takeaway = 1 and bookings.booking_state_simple = 'COMPLETED' 
        THEN 
            cast(coalesce(fo.gf_base,0) as double) / fx_one_usd
			-- cast(bookings.food_sub_total - coalesce(fo.total_order_mfc_discount_affecting_comms,0) as double) / fx_one_usd
        ELSE 0.0
        END) AS takeaway_base_for_mex_commission

    ,sum(CASE 
	  	   WHEN 
	  	   		is_takeaway = 1 and bookings.booking_state_simple = 'COMPLETED' 
	       THEN 
	       		basket_size / fx_one_usd
	       ELSE 0.0
	       END) AS takeaway_basket_size_usd
	       
	  ,sum(CASE
	       WHEN 
	       		is_takeaway = 1 and bookings.booking_state_simple = 'COMPLETED'
	       THEN 
	       		basket_size       
	       ELSE 0.0
	       END) AS takeaway_basket_size_local
	       
	 ,sum(CASE
	  	  WHEN 
	  	  		is_takeaway = 1 and bookings.booking_state_simple = 'COMPLETED'
	  	  THEN	
	  	  		bookings.food_sub_total / fx_one_usd
		  ELSE 0.0
	      END) AS takeaway_sub_total_usd
	      
	 ,SUM(CASE
	  	  WHEN 
	  	  		is_takeaway = 1 and bookings.booking_state_simple = 'COMPLETED'
	  	  THEN
	      		bookings.food_sub_total
	  	  ELSE 0.0
	      END) AS takeaway_sub_total_local

    ,SUM(CASE 
	       WHEN bookings.booking_state_simple = 'COMPLETED' AND is_advance<>1 and is_takeaway = 1 THEN CAST(bookings.time_to_complete AS double)/60.0 
	       ELSE 0.0 
	       END) AS takeaway_time_from_order_create_to_completed

    ,SUM(CASE
            WHEN bookings.booking_state_simple = 'COMPLETED' THEN 
            bookings.tips
            ELSE 0.0
            END) as tips_local

    ,SUM(CASE
            WHEN bookings.booking_state_simple = 'COMPLETED' THEN 
            bookings.tips/fx_one_usd
            ELSE 0.0
            END) as tips_usd

    /*============scheduled orders============*/
	,sum(case 
        when is_advance = 1
        THEN 1 
        ELSE 0 
        END) AS total_scheduled_orders
	     
	,sum(case 
        when is_advance = 1 and booking_state_simple = 'COMPLETED'
        THEN 1 
        ELSE 0 
        END) AS total_scheduled_completed_orders

    ,sum(case 
        when is_advance = 1 and booking_state_simple = 'COMPLETED'
        THEN gross_merchandise_value
        ELSE 0
        END) AS scheduled_gmv_local  

    ,sum(case 
        when is_advance = 1 and booking_state_simple = 'COMPLETED'
        THEN 
            gross_merchandise_value / fx_one_usd
        ELSE 0
        END) AS scheduled_gmv_usd 

     ,sum(CASE
	      WHEN 
	  	  		is_advance = 1 and bookings.booking_state_simple = 'COMPLETED'
	      THEN
			bookings.commission_from_merchant / fx_one_usd
	     ELSE 0.0
	     END) AS scheduled_mex_commission
	       
	 ,sum(CASE
	      WHEN 
	      		is_advance = 1 and bookings.booking_state_simple = 'COMPLETED'
	      THEN
				bookings.commission_from_merchant
	     ELSE 0.0
	     END) AS scheduled_mex_commission_local

    ,sum(CASE 
        WHEN 
            is_advance = 1 and bookings.booking_state_simple = 'COMPLETED' 
        THEN 
            cast(coalesce(fo.gf_base,0) as double)
			-- cast(bookings.food_sub_total - coalesce(fo.total_order_mfc_discount_affecting_comms,0) as double)
        ELSE 0.0
        END) AS scheduled_base_for_mex_commission_local

    ,sum(CASE 
        WHEN 
            is_advance = 1 and bookings.booking_state_simple = 'COMPLETED' 
        THEN 
            cast(coalesce(fo.gf_base,0) as double) / fx_one_usd
			-- cast(bookings.food_sub_total - coalesce(fo.total_order_mfc_discount_affecting_comms,0) as double) / fx_one_usd
        ELSE 0.0
        END) AS scheduled_base_for_mex_commission

    ,sum(CASE 
	  	   WHEN 
	  	   		is_advance = 1 and bookings.booking_state_simple = 'COMPLETED' 
	       THEN 
	       		basket_size / fx_one_usd
	       ELSE 0.0
	       END) AS scheduled_basket_size_usd
	       
	  ,sum(CASE
	       WHEN 
	       		is_advance = 1 and bookings.booking_state_simple = 'COMPLETED'
	       THEN 
	       		basket_size       
	       ELSE 0.0
	       END) AS scheduled_basket_size_local
	       
	 ,sum(CASE
	  	  WHEN 
	  	  		is_advance = 1 and bookings.booking_state_simple = 'COMPLETED'
	  	  THEN	
	  	  		bookings.food_sub_total / fx_one_usd
		  ELSE 0.0
	      END) AS scheduled_sub_total_usd
	      
	 ,SUM(CASE
	  	  WHEN 
	  	  		is_advance = 1 and bookings.booking_state_simple = 'COMPLETED'
	  	  THEN
	      		bookings.food_sub_total
	  	  ELSE 0.0
	      END) AS scheduled_sub_total_local
	     
	 ,SUM(CASE
	  	  WHEN 
	  	  		is_advance = 1 and bookings.booking_state_simple = 'COMPLETED'
	  	  THEN
	      		date_diff('day',date(created_at_local),date(pick_up_time_local)) 
	  	  ELSE cast(0.0 as double)
	      END) AS scheduled_total_date_diff
	     
	     
	FROM datamart_grabfood.base_bookings bookings

    left join fo on bookings.order_id = fo.order_id
		
	WHERE date(bookings.date_local) >= date([[inc_start_date]])
	    AND date(bookings.date_local) <= DATE([[inc_end_date]])
	
	GROUP BY 1,2,3,4,5,6,7,8
)
,jobs_metrics as (
    SELECT 
        bk.date_local
        ,ct.city_name
        ,ct.country_name
        ,bk.restaurant_id
        ,CASE WHEN bk.partner=1 THEN 'partner' ELSE 'non-partner' END AS restaurant_partner_status
        ,CASE WHEN coalesce(json_extract_scalar(bk.grabfood_metadata, '$.orderType'), '0') = '1' THEN 'Integrated' ELSE 'Concierge' END AS business_model
        ,CASE WHEN LENGTH(bk.payment_type_id)>0 AND bk.payment_type_id IS NOT NULL THEN 'Cashless' ELSE 'Cash' END AS cashless_status
        ,CASE 
                WHEN gpc.booking_code IS NOT NULL AND LENGTH(bk.payment_type_id)>0 AND  bk.payment_type_id IS NOT NULL THEN 'GrabPay-GPC'
                WHEN gpc.booking_code IS NULL AND LENGTH(bk.payment_type_id)>0 AND bk.payment_type_id IS NOT NULL THEN 'GrabPay-Non GPC'
                WHEN LENGTH(bk.payment_type_id)=0 OR  bk.payment_type_id IS NULL THEN 'Cash'
        END AS payment_type
        ,SUM(CASE WHEN cd.state_simple = 'Bid' THEN 1 ELSE 0 END) AS jobs_accepted
        ,SUM(CASE WHEN cd.state_simple IN ('Bid','Declined','Ignored') THEN 1 ELSE 0 END) AS jobs_received
        ,SUM(CASE WHEN cd.state_simple = 'Unread' THEN 1 ELSE 0 END) AS jobs_unread
    FROM public.prejoin_candidate cd 
    INNER JOIN public.prejoin_grabfood bk 
        ON cd.booking_code = bk.booking_code
        
    LEFT JOIN 
        (
        SELECT * FROM slide.datamart_bb_gpc
        WHERE date(partition_date) >= date([[inc_start_date]]) - INTERVAL '10' DAY
        AND date(partition_date) <= date([[inc_end_date]]) + INTERVAL '10' DAY
        ) gpc  
        
        ON bk.booking_code = gpc.booking_code
        
    INNER JOIN datamart.dim_cities_countries ct 
        ON cd.city_id = ct.city_id
        
    WHERE bk.is_test_booking = FALSE
        AND date(bk.partition_date) >= date([[inc_start_date]]) - INTERVAL '1' day
        AND date(bk.partition_date) <= DATE([[inc_end_date]]) + interval '1' DAY
        AND date(cd.partition_date) >= date([[inc_start_date]]) - INTERVAL '1' day
        AND date(cd.partition_date) <= DATE([[inc_end_date]]) + interval '1' DAY
        
    GROUP BY 1,2,3,4,5,6,7,8
)
SELECT 
    bk_metrics.*,
coalesce(jobs_metrics.jobs_accepted,0) as jobs_accepted,
coalesce(jobs_metrics.jobs_received,0) as jobs_received,
coalesce(jobs_metrics.jobs_unread,0) as jobs_unread,
bk_metrics.date_local as partition_date_local

FROM bk_metrics


LEFT JOIN jobs_metrics
    ON bk_metrics.date_local = jobs_metrics.date_local
        AND bk_metrics.city_name = jobs_metrics.city_name
        AND bk_metrics.country_name = jobs_metrics.country_name
        AND bk_metrics.merchant_id = jobs_metrics.restaurant_id
        AND bk_metrics.restaurant_partner_status = jobs_metrics.restaurant_partner_status
        AND bk_metrics.business_model = jobs_metrics.business_model
        AND bk_metrics.cashless_status = jobs_metrics.cashless_status
        AND bk_metrics.payment_type = jobs_metrics.payment_type