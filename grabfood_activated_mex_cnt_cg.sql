/*
    Name: slide.grabfood_activated_mex_cnt_cg
    Refresh Time: Daily, 21:00 UTC
    Aggregation Mode: Incremental, lookback window 10 days
    Lighthouse Dependancy Tables: NIL
*/
WITH all_mex AS 
(

(
SELECT 
a.*,
coalesce(ct.city_name,'Unidentified') AS city_name,
countries.name AS country_name

FROM

(SELECT
gm.id AS merchant_id,
gm.country_id,
coalesce(area_map.city_id,gm.city_id) AS city_id,
date(partition_date) AS snapshot_date,
CASE WHEN json_extract_scalar(gm.contract, '$.partner') = '1' THEN 1 ELSE 0 END AS partner_status,
model_type

FROM
(SELECT *,
geohash(cast(latitude AS double), cast(longitude AS double), 6) AS geohash
from
grab_mall.grab_mall_seller_snapshot
WHERE partition_date <= '2019-06-26'
) gm

LEFT JOIN geohash.area_map ON area_map.geohash = gm.geohash
WHERE 
status = 1
GROUP BY 1,2,3,4,5,6) a 
LEFT JOIN datamart.dim_cities_countries ct
ON a.city_id = ct.city_id
INNER JOIN public.countries 
ON a.country_id = countries.id

)

UNION ALL

/*** after 2019-06-26, use food_data_service snapshot ***/

(	

	select det.merchant_id
	    ,det.country_id
	    ,det.city_id
	    ,date(det.partition_date) AS snapshot_date
	    ,CASE WHEN contracts.partner = TRUE THEN 1 ELSE 0 END AS partner_status
	    ,det.model_type       
		,det.city_name
	  	,det.country_name
	  	
	FROM 
	(
		select a.partition_date, ct.country_name,ct.city_name,b.* 
		from 
		(
		  -- taking only the last record of the day (in UTC time)
		    SELECT merchant_id, concat(year,'-',month,'-',day) AS partition_date, MAX(valid_until) AS final_record_time
		    FROM snapshots.food_data_service_merchants
		    WHERE status = 'ACTIVE'
		    AND concat(year,'-',month,'-',day) > '2019-06-26'
		    AND concat(year,'-',month,'-',day) >= [[inc_start_date]]
		    AND concat(year,'-',month,'-',day) <= [[inc_end_date]]
		    GROUP BY 1,2
	  	) a
	  	INNER JOIN 
	  	(
	    	SELECT * FROM snapshots.food_data_service_merchants
	    	WHERE status = 'ACTIVE'
	    	AND concat(year,'-',month,'-',day) > '2019-06-26'
	    	AND concat(year,'-',month,'-',day) >= [[inc_start_date]]
		    AND concat(year,'-',month,'-',day) <= [[inc_end_date]]
	  	) b 
	  	ON b.merchant_id = a.merchant_id 
	  	AND b.valid_until = a.final_record_time 
	  	AND concat(b.year,'-',b.month,'-',b.day) = a.partition_date
	  	
	  	INNER JOIN datamart.dim_cities_countries ct
	  	ON b.city_id = ct.city_id
	
	) det
	
	left join (
		select c.partition_date, d.* from (
		  -- taking only the last record of the day (in UTC time)
		    SELECT merchant_id, concat(year,'-',month,'-',day) AS partition_date, MAX(valid_until) AS final_record_time
		    FROM snapshots.food_data_service_merchant_contracts
		    WHERE concat(year,'-',month,'-',day) > '2019-06-26'
		    AND concat(year,'-',month,'-',day) >= [[inc_start_date]]
		    AND concat(year,'-',month,'-',day) <= [[inc_end_date]]
		    GROUP BY 1,2
	  	) c
	  	LEFT JOIN (
	    	SELECT * FROM snapshots.food_data_service_merchant_contracts
	    	WHERE concat(year,'-',month,'-',day) > '2019-06-26'
	    	AND concat(year,'-',month,'-',day) >= [[inc_start_date]]
		    AND concat(year,'-',month,'-',day) <= [[inc_end_date]]
	  	) d ON d.merchant_id = c.merchant_id AND d.valid_until = c.final_record_time AND concat(d.year,'-',d.month,'-',d.day) = c.partition_date
	  	where d.merchant_id is not null
	) contracts 
	
	ON contracts.merchant_id = det.merchant_id 
	and contracts.partition_date = det.partition_date

)	


)

/*** BY Region + By day ****/
(
SELECT 
'By Region' AS by_city_country,
'By Day' AS by_day_week_month,
snapshot_date AS time_period,
'All' as country_name,
'All' AS city_name,
COUNT(DISTINCT merchant_id) AS activated_merchants,
COUNT(DISTINCT CASE WHEN partner_status=1 THEN merchant_id ELSE NULL end) AS activated_partner_merchants,
COUNT(DISTINCT CASE WHEN model_type = 1 THEN merchant_id ELSE NULL END) AS activated_IM_merchants

FROM all_mex
WHERE snapshot_date>=date([[inc_start_date]])
AND snapshot_date<=date([[inc_end_date]])

GROUP BY 1,2,3,4,5 )

UNION ALL

/*** BY Country + By day ****/
(
SELECT 
'By Country' AS by_city_country,
'By Day' AS by_day_week_month,
snapshot_date AS time_period,
country_name,
'All' AS city_name,
COUNT(DISTINCT merchant_id) AS activated_merchants,
COUNT(DISTINCT CASE WHEN partner_status=1 THEN merchant_id ELSE NULL end) AS activated_partner_merchants,
COUNT(DISTINCT CASE WHEN model_type = 1 THEN merchant_id ELSE NULL END) AS activated_IM_merchants

FROM all_mex
WHERE snapshot_date>=date([[inc_start_date]])
AND snapshot_date<=date([[inc_end_date]])

GROUP BY 1,2,3,4,5 )

/*** BY City + By day ****/

UNION ALL

(
SELECT 
'By City' AS by_city_country,
'By Day' AS by_day_week_month,
snapshot_date AS time_period,
country_name,
city_name,
COUNT(DISTINCT merchant_id) AS activated_merchants,
COUNT(DISTINCT CASE WHEN partner_status=1 THEN merchant_id ELSE NULL end) AS activated_partner_merchants,
COUNT(DISTINCT CASE WHEN model_type = 1 THEN merchant_id ELSE NULL END) AS activated_IM_merchants

FROM all_mex
WHERE snapshot_date>=date([[inc_start_date]])
AND snapshot_date<=date([[inc_end_date]])

GROUP BY 1,2,3,4,5 )


/*** BY Region + By Week ***/
UNION ALL

(
SELECT 
'By Region' AS by_city_country,
'By Week' AS by_day_week_month,
date_trunc('week',snapshot_date) AS time_period,
'All' AS country_name,
'All' AS city_name,
COUNT(DISTINCT merchant_id) AS activated_merchants,
COUNT(DISTINCT CASE WHEN partner_status=1 THEN merchant_id ELSE NULL end) AS activated_partner_merchants,
COUNT(DISTINCT CASE WHEN model_type = 1 THEN merchant_id ELSE NULL END) AS activated_IM_merchants

FROM all_mex
WHERE dow(snapshot_date) = 7
AND snapshot_date>=date_trunc('week',date([[inc_start_date]]))
AND snapshot_date<=date([[inc_end_date]])
GROUP BY 1,2,3,4,5 )



/*** BY Country + By Week ****/
UNION ALL

(
SELECT 
'By Country' AS by_city_country,
'By Week' AS by_day_week_month,
date_trunc('week',snapshot_date) AS time_period,
country_name,
'All' AS city_name,
COUNT(DISTINCT merchant_id) AS activated_merchants,
COUNT(DISTINCT CASE WHEN partner_status=1 THEN merchant_id ELSE NULL end) AS activated_partner_merchants,
COUNT(DISTINCT CASE WHEN model_type = 1 THEN merchant_id ELSE NULL END) AS activated_IM_merchants

FROM all_mex
WHERE dow(snapshot_date) = 7
AND snapshot_date>=date_trunc('week',date([[inc_start_date]]))
AND snapshot_date<=date([[inc_end_date]])
GROUP BY 1,2,3,4,5 )

/*** BY City + By Week ****/
UNION ALL

(
SELECT 
'By City' AS by_city_country,
'By Week' AS by_day_week_month,
date_trunc('week',snapshot_date) AS time_period,
country_name,
city_name,
COUNT(DISTINCT merchant_id) AS activated_merchants,
COUNT(DISTINCT CASE WHEN partner_status=1 THEN merchant_id ELSE NULL end) AS activated_partner_merchants,
COUNT(DISTINCT CASE WHEN model_type = 1 THEN merchant_id ELSE NULL END) AS activated_IM_merchants

FROM all_mex
WHERE dow(snapshot_date) = 7
AND snapshot_date>=date_trunc('week',date([[inc_start_date]]))
AND snapshot_date<=date([[inc_end_date]])

GROUP BY 1,2,3,4,5 )

/*** BY Region + By Month ****/
UNION ALL

(
SELECT 
'By Region' AS by_city_country,
'By Month' AS by_day_week_month,
date_trunc('month',snapshot_date) AS time_period,
'All' AS country_name,
'All' AS city_name,
COUNT(DISTINCT merchant_id) AS activated_merchants,
COUNT(DISTINCT CASE WHEN partner_status=1 THEN merchant_id ELSE NULL end) AS activated_partner_merchants,
COUNT(DISTINCT CASE WHEN model_type = 1 THEN merchant_id ELSE NULL END) AS activated_IM_merchants

FROM all_mex
WHERE snapshot_date = date_trunc('month',snapshot_date + INTERVAL '1' MONTH )  - INTERVAL '1' day
AND snapshot_date>=date_trunc('month',date([[inc_start_date]]))
AND snapshot_date<=date([[inc_end_date]])

GROUP BY 1,2,3,4,5 )


/*** BY Country + By Month ****/
UNION ALL

(
SELECT 
'By Country' AS by_city_country,
'By Month' AS by_day_week_month,
date_trunc('month',snapshot_date) AS time_period,
country_name,
'All' AS city_name,
COUNT(DISTINCT merchant_id) AS activated_merchants,
COUNT(DISTINCT CASE WHEN partner_status=1 THEN merchant_id ELSE NULL end) AS activated_partner_merchants,
COUNT(DISTINCT CASE WHEN model_type = 1 THEN merchant_id ELSE NULL END) AS activated_IM_merchants

FROM all_mex
WHERE snapshot_date = date_trunc('month',snapshot_date + INTERVAL '1' MONTH )  - INTERVAL '1' day
AND snapshot_date>=date_trunc('month',date([[inc_start_date]]))
AND snapshot_date<=date([[inc_end_date]])

GROUP BY 1,2,3,4,5 )

/*** BY City + By Month ****/
UNION ALL

(
SELECT 
'By City' AS by_city_country,
'By Month' AS by_day_week_month,
date_trunc('month',snapshot_date) AS time_period,
country_name,
city_name,
COUNT(DISTINCT merchant_id) AS activated_merchants,
COUNT(DISTINCT CASE WHEN partner_status=1 THEN merchant_id ELSE NULL end) AS activated_partner_merchants,
COUNT(DISTINCT CASE WHEN model_type = 1 THEN merchant_id ELSE NULL END) AS activated_IM_merchants

FROM all_mex
WHERE snapshot_date = date_trunc('month',snapshot_date + INTERVAL '1' MONTH ) - INTERVAL '1' day
AND snapshot_date>=date_trunc('month',date([[inc_start_date]]))
AND snapshot_date<=date([[inc_end_date]])
GROUP BY 1,2,3,4,5 )


