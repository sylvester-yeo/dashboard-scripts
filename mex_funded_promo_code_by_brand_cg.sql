/*
    Name: slide.mex_funded_promo_code_by_brand_cg
    Aggregation Mode: Overwrite
    Refresh Time: Inactive, 22:00 
*/

SELECT 
base.country AS country_gsheet,
base.city AS city_gsheet,
base.brand_name AS business_name_gsheet,
ct.country_name AS country,
ct.city_name AS city,
base.brand_name AS business_name,
base.date_local,
base.mex_funding_amount_perday AS mex_funding_amount_perday_local,
cast(base.mex_funding_amount_perday AS double)/forex.exchange_one_usd AS mex_funding_amount_perday_usd

FROM 

(SELECT 
 mfp.country,
 mfp.city,
 mfp.brand_name,
 date_dim.date_local,
 sum(mex_funding_amount_perday) AS mex_funding_amount_perday
 FROM
	(
	SELECT
	combine.country,
	combine.city,
	combine.brand_name,
	--combine.brand_id,
	date(combine.start_date) AS start_date,
	date(combine.end_date) AS end_date,
	combine.promo_code,
	cast(combine.mex_funding_amount AS double)/CAST((date_diff('day',date(combine.start_date),date(combine.end_date))+1) AS double) AS mex_funding_amount_perday
	from (  
	  select * from temptables.mex_funded_promo_code_vn_old WHERE city IS NOT NULL AND brand_name IS NOT NULL AND mex_funding_amount IS NOT NULL AND trim(start_date)<>'' AND trim(end_date)<>''  
	  union all
	  --select * from temptables.mex_funded_promo_code_id WHERE city IS NOT NULL AND brand_name IS NOT NULL AND mex_funding_amount IS NOT NULL AND trim(start_date)<>'' AND trim(end_date)<>''
	  --union all
	  select * from temptables.mex_funded_promo_code_th_old WHERE city IS NOT NULL AND brand_name IS NOT NULL AND mex_funding_amount IS NOT NULL AND trim(start_date)<>'' AND trim(end_date)<>''
	  union all
	  select * from temptables.mex_funded_promo_code_ph_old WHERE city IS NOT NULL AND brand_name IS NOT NULL AND mex_funding_amount IS NOT NULL AND trim(start_date)<>'' AND trim(end_date)<>''
	  union all
	  select * from temptables.mex_funded_promo_code_sg_old WHERE city IS NOT NULL AND brand_name IS NOT NULL AND mex_funding_amount IS NOT NULL AND trim(start_date)<>'' AND trim(end_date)<>''
	  union all
	  select * from temptables.mex_funded_promo_code_my_old WHERE city IS NOT NULL AND brand_name IS NOT NULL AND mex_funding_amount IS NOT NULL AND trim(start_date)<>'' AND trim(end_date)<>''
	  ) combine
	left join temptables.mex_funded_promo_code_dup dup 
	  on dup.promo_code=combine.promo_code
	  --and dup.brand_id=combine.brand_id
	  and dup.brand_name=combine.brand_name
	  AND dup.start_date = combine.start_date
	  AND dup.end_date = combine.end_date
	 where dup.promo_code IS NULL
	   AND dup.start_date IS NULL
	   AND dup.end_date IS NULL
	  ) mfp
  
	CROSS JOIN 

	(SELECT DISTINCT date(l_date) AS date_local FROM public.date_dim 
	 WHERE l_date>= '2018-10-01'
	 AND date(l_date) <= current_date) date_dim

	WHERE date_dim.date_local >= mfp.start_date
	AND date_dim.date_local <= mfp.end_date
	
	GROUP BY 1,2,3,4
	) base
	
	LEFT JOIN datamart.dim_cities_countries ct
	ON lower(trim(base.city)) = lower(trim(ct.city_name))
	
	
	LEFT JOIN datamart.ref_exchange_rates forex 
	ON ct.country_id = forex.country_id 
	AND DATE_TRUNC('month',base.date_local) = DATE_TRUNC('month',forex.start_date)