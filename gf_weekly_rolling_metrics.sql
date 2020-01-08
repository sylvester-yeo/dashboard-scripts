/*

    Name: slide.gf_weekly_rolling_metrics
    Refresh Time: Daily, 21:00 UTC
    Lighthouse dependency Tables:
        slide.gf_dash_dax_first_last_bid 21:00
        slide.gf_dash_dax_metrics_aggregated 21:00
        slide.gf_dash_pax_first_booking_order 21:00
        slide.gf_dash_pax_metrics_agg 21:00

*/

SELECT * from slide.gf_dash_weekly_rolling_metrics_region
UNION ALL
SELECT * FROM slide.gf_dash_weekly_rolling_metrics_country
UNION ALL
SELECT * FROM slide.gf_dash_weekly_rolling_metrics_city