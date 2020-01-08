/*
    Name: slide.gf_dash_mex_join_date
    Refresh Time: Daily, 21:00 UTC
    Aggregation Mode: Overwrite
	Increment window: 45 days
    Lighthouse Dependancy Tables: NIL
*/

SELECT 
merchant_id,
country_name,
city_name,
model_type,
min(join_date) AS join_date,
min(partner_join_date) AS partner_join_date
FROM
(
	(	
	select det.merchant_id
	  	,det.country_name
		,det.city_name
		,(CASE WHEN det.model_type = 1 THEN 'IM' ELSE 'CM' END) AS model_type
		,MIN(CASE WHEN det.status = 'ACTIVE' THEN TRY_CAST(det.partition_date as date) ELSE NULL END) AS join_date
	    ,MIN(CASE WHEN contracts.partner = true AND det.status = 'ACTIVE' THEN TRY_CAST(det.partition_date as DATE) ELSE NULL END) AS partner_join_date
	FROM 
	(
		select a.partition_date, ct.country_name,ct.city_name,b.* 
		from 
		(
		  -- taking only the last record of the day (in UTC time)
		    SELECT merchant_id, concat(year,'-',month,'-',day) AS partition_date, MAX(valid_until) AS final_record_time
		    FROM snapshots.food_data_service_merchants
		    GROUP BY 1,2
	  	) a
	  	INNER JOIN 
	  	(
	    	SELECT * FROM snapshots.food_data_service_merchants
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
		    GROUP BY 1,2
	  	) c
	  	LEFT JOIN (
	    	SELECT * FROM snapshots.food_data_service_merchant_contracts
	  	) d ON d.merchant_id = c.merchant_id AND d.valid_until = c.final_record_time AND concat(d.year,'-',d.month,'-',d.day) = c.partition_date
	  	where d.merchant_id is not null
	) contracts 
	ON contracts.merchant_id = det.merchant_id 
	and contracts.partition_date = det.partition_date
	group by 1,2,3,4	
	)
		
			
	/*** from old grab_mall_seller ****/	
	UNION ALL
	
	(
		SELECT
		DISTINCT
		a.id AS merchant_id,
		COALESCE(ct.country_name,countries.name) AS country_name,
		ct.city_name,
		model_type,
		date(join_date) AS join_date,
		date(partner_join_date) as partner_join_date
		
		from
		/*have to be changed because Azure doesnt have partition_date column in the table
			impact: new table doesn't have records after 2018-11-01, all the mex join date will be super high there*/
		/*(
			SELECT
			gm.id,
			gm.country_id,
		    coalesce(area_map.city_id,gm.city_id) as city_id,
		       CASE WHEN model_type = 1 then 'IM' ELSE 'CM' END AS model_type,
			MIN(CASE WHEN status = 1 THEN partition_date ELSE NULL END) AS join_date,
		    MIN(CASE WHEN json_extract_scalar(gm.contract,'$.partner') = '1' AND status = 1 THEN partition_date ELSE NULL END) AS partner_join_date
		
			FROM
				(
				SELECT *,
				geohash(cast(latitude AS double), cast(longitude AS double), 6) AS geohash
				from
				grab_mall.grab_mall_seller_snapshot
				) gm
			LEFT JOIN geohash.area_map 
			      ON area_map.geohash = gm.geohash
			GROUP BY 1,2,3,4
		) a */

		(
			SELECT
			gm.id,
			gm.country_id,
		    coalesce(area_map.city_id,gm.city_id) as city_id,
		    CASE WHEN model_type = 1 then 'IM' ELSE 'CM' END AS model_type,
			MIN(CASE WHEN status = 1 THEN date(year||'-'||month||'-'||day) ELSE NULL END) AS join_date,
		    MIN(CASE WHEN json_extract_scalar(gm.contract,'$.partner') = '1' AND status = 1 THEN date(year||'-'||month||'-'||day) ELSE NULL END) AS partner_join_date
		
			FROM
				(
				SELECT *,
				geohash(cast(latitude AS double), cast(longitude AS double), 6) AS geohash
				from snapshots.grab_mall_grab_mall_seller
                where year||'-'||month||'-'||day < '2019-05-29'
				) gm
			LEFT JOIN geohash.area_map 
			      ON area_map.geohash = gm.geohash
			GROUP BY 1,2,3,4
		) a 

		LEFT JOIN datamart.dim_cities_countries ct
		ON a.city_id = ct.city_id
		INNER JOIN public.countries
		ON a.country_id = countries.id
		
		WHERE a.join_date IS NOT NULL 
	)
	
	/****** From Delta App ******/
	UNION ALL
	
	 ( SELECT
	    CASE
	  WHEN ct.country_name='Singapore' then concat('SGDD',lpad(cast(dd.merchant_id AS varchar),5,'0')) 
	  WHEN ct.country_name='Malaysia' then concat('MYDD',lpad(cast(dd.merchant_id AS varchar),5,'0')) 
	 END AS merchant_id
	    ,ct.country_name
	    ,ct.city_name
	   ,'IM' AS model_type
	    ,MIN(dd.date_local) AS join_date
	    ,MIN(dd.date_local) AS partner_join_date
	  FROM slide.grabfood_delta_orders dd
	  LEFT JOIN datamart.dim_cities_countries ct ON dd.city_id = ct.city_id
	  WHERE
	  dd.date_local IS NOT NULL
	  AND dd.is_completed = 1
	  AND dd.is_unique_booking = 1
	  GROUP BY 1,2,3,4)

)

GROUP BY 1,2,3,4