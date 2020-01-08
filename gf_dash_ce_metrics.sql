/*
    Name: slide.gf_dash_ce_metrics
    Refresh Time: Daily, 21:00 UTC
    Aggregation Mode: Incremental, lookback window 10 days
    Lighthouse Dependancy Tables: NIL
*/

SELECT
total_contact.country,
total_contact.date_local,
csat_sum,
csat_count,
inbound_handled_call,
inbound_abandoned_call,
outbound_handled_call,
zendesk_ticket_count,
zendesk_ticket_count_w_voice,
(inbound_handled_call + inbound_abandoned_call + outbound_handled_call + zendesk_ticket_count) AS total_contact
FROM
(
SELECT
COALESCE(xcally.date_local,zendesk.date_local) AS date_local,
COALESCE(xcally.country,zendesk.country) AS country,
COALESCE(inbound_handled_call,0) AS inbound_handled_call,
COALESCE(inbound_abandoned_call,0) AS inbound_abandoned_call,
COALESCE(outbound_handled_call,0) AS outbound_handled_call,
COALESCE(ticket_count,0) AS zendesk_ticket_count,
COALESCE(ticket_count_w_voice,0) AS zendesk_ticket_count_w_voice
FROM
(
SELECT
countries.name AS country,
date(call_log_v1.origcalldate) AS date_local,
sum(
   CASE
       WHEN call_log_v1.outboundcall = 0 AND call_log_v1.connect = 1  AND team_group IS NOT NULL THEN 1
       ELSE 0
   END) AS inbound_handled_call,
sum(
   CASE
       WHEN call_log_v1.outboundcall = 0 AND call_log_v1.connect = 0 AND team_group IS NOT NULL THEN 1
       ELSE 0
   END) AS inbound_abandoned_call,
sum(
   CASE
       WHEN call_log_v1.outboundcall = 1  THEN 1
       ELSE 0
   END) AS outbound_handled_call

 FROM xcally.call_log_v1
 left join  public.countries ON call_log_v1.countryid = cast(countries.id as integer)
 WHERE date(call_log_v1.origcalldate) >= date([[inc_start_date]])
 and date(call_log_v1.origcalldate) <= date([[inc_end_date]])
  --grabfood only
 and (call_log_v1.team_group LIKE '%gf%' OR tag='gf')
 GROUP BY 1,2) xcally

 FULL OUTER JOIN
 (
 select
(c.name) as country,
date(from_utc_timestamp(tickets_metrics.created_at, c.country_timezone)) as date_local,
count(tickets.id) AS ticket_count_w_voice,
count(CASE WHEN lower(ticket_source) <> 'voice' THEN tickets.id ELSE NULL END) AS ticket_count
  from
  zendesk.tickets tickets
  left join zendesk.tickets_metrics on tickets.id = tickets_metrics.ticket_id
  left join public.countries c on tickets.country = lower(c.code)
  WHERE
  tickets.status != 'deleted'
  AND date(from_utc_timestamp(tickets_metrics.created_at, c.country_timezone)) >= date([[inc_start_date]])
  AND date(from_utc_timestamp(tickets_metrics.created_at, c.country_timezone)) <= date([[inc_end_date]])
  ---add partition column
  and date(concat(substr(tickets_metrics.created_date,1,4),'-',substr(tickets_metrics.created_date,5,2),'-',substr(tickets_metrics.created_date,7,2)))>= date([[inc_start_date]]) - INTERVAL '1' day
  and date(concat(substr(tickets.partition_date,1,4),'-',substr(tickets.partition_date,5,2),'-',substr(tickets.partItion_date,7,2)))>= date([[inc_start_date]]) - INTERVAL '1' day

  AND c.name IS NOT NULL
  AND tickets_metrics.created_at IS NOT NULL
  AND UPPER(tickets.service) = 'GRABFOOD'
  GROUP BY 1,2
 ) zendesk
 ON xcally.country = zendesk.country
 AND xcally.date_local = zendesk.date_local 
 ) total_contact
LEFT JOIN
(
select
c.name as country,
date(from_utc_timestamp(tickets_metrics.solved_at, c.country_timezone)) AS date_local,
sum(CASE WHEN csat_score IN ('',' ') THEN 0 ELSE cast(csat_score AS double) end) as csat_sum,
count(CASE WHEN csat_score IN ('',' ') THEN null ELSE cast(csat_score AS double) end) as csat_count
from zendesk.tickets
left join zendesk.tickets_metrics on tickets.id = tickets_metrics.ticket_id
left join public.countries c on tickets.country = lower(c.code)
where tickets.status in ('solved', 'closed')
and lower(tickets.tags) not like lower('%ticket_migration%')
and date(from_utc_timestamp(tickets_metrics.solved_at, c.country_timezone)) >= date([[inc_start_date]])
AND date(from_utc_timestamp(tickets_metrics.solved_at, c.country_timezone)) <= date([[inc_end_date]])
---add partition column
and date(concat(substr(tickets_metrics.created_date,1,4),'-',substr(tickets_metrics.created_date,5,2),'-',substr(tickets_metrics.created_date,7,2)))>= date([[inc_start_date]]) - INTERVAL '1' day
and date(concat(substr(tickets.partition_date,1,4),'-',substr(tickets.partition_date,5,2),'-',substr(tickets.partition_date,7,2)))>= date([[inc_start_date]]) - INTERVAL '1' day
AND UPPER(tickets.service) = 'GRABFOOD'
AND c.name IS NOT NULL
group by 1,2
) tk_rating
ON total_contact.country = tk_rating.country
AND total_contact.date_local = tk_rating.date_local

