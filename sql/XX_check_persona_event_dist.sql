/* Check the distribution of customer personas shopping in each event */
/* Determine if making separate models underneath persona is necessary */


drop table if exists event_dates;
drop table if exists base_population;


create temp table event_dates (
  event varchar(32),
  yr integer,
  start_dt date,
  end_dt date
)
  diststyle key
  distkey (event)
  sortkey (yr);

insert into event_dates (
  select
    event,
    yr,
    start_dt,
    end_dt
  from (
    {3} -- insert select statements in ../json_and_txt/event_dates_select.txt for each
        --   event in ../json_and_txt/mktg_events.json, joining the strings with 'union'
  )
  where yr between {1} and {2}
    and extract(year from start_dt) between {1} and {2}
);

analyze event_dates;


create temp table base_population (
  cust_key numeric(20) not null,
  persona numeric(10)
)
  diststyle key
  distkey (persona)
  sortkey (cust_key);

insert into base_population (
  select
    cust.cust_key,
    persona.persona
  from analytics_user_vws.liveramp_customers{0} as cust
  inner join analytics_user_vws.liveramp_model_personas_clusters{0} as persona
    on cust.cust_key = persona.cust_key
  inner join analytics_user_vws.liveramp_trans{0} as trans
    on cust.cust_key = trans.cust_key
  inner join analytics_user_vws.liveramp_funnel{0} as funnel
    on trans.cust_key = funnel.cust_key and trans.sale_dt >= funnel.acq_dt
  where cust.current_employee_ind = 0
  group by cust.cust_key, persona.persona
  having max(trans.spend_discount_employee_ind) = 0
    and max(
      case
        when extract(year from trans.sale_dt) between {1} and {2}
          and trans.intent_channel in ('FLS','N.COM')
          then 1
        else 0
      end
    ) = 1
);

analyze base_population;


select
  base.persona,
  edates.event,
  edates.yr as year,
  --edates.start_dt,
  --edates.end_dt,
  count(distinct trans.cust_key) as n_shopped,
  count(distinct base.cust_key) as n_existing
from base_population as base
inner join analytics_user_vws.liveramp_funnel{0} as funnel
  on base.cust_key = funnel.cust_key
inner join event_dates as edates
  on funnel.acq_dt <= edates.end_dt
left join (
  select
    cust_key,
    sale_dt
  from analytics_user_vws.liveramp_trans{0}
  where extract(year from sale_dt) between {1} and {2}
    and intent_channel in ('FLS','N.COM')
) as trans
  on base.cust_key = trans.cust_key
    and trans.sale_dt between edates.start_dt and edates.end_dt
group by
  base.persona,
  edates.event,
  edates.yr,
  edates.start_dt,
  edates.end_dt
order by
  edates.yr,
  edates.start_dt,
  edates.end_dt,
  base.persona;
