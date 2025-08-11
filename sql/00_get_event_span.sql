/* Event propensity model - Step 2 - Get date range of target event */

select
  min(event_dt) as start_dt,
  max(event_dt) as end_dt
from (
  select
    event_dt,
    {1},
    sum(1-{1}) over (
      order by event_dt
      rows between unbounded preceding and current row
    ) as grp
  from analytics_user_vws.liveramp_events{0}
)
where {1} = 1
group by grp
having extract(year from max(event_dt)) = {2};
