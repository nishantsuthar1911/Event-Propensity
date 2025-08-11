/* Event propensity model - Step 3 - Create the base dataset tables for customers and transactions */
/*                                   Also create event date span table */
/* Event propensity model - Step 4 - Create the event-specific variables */
/* Event propensity model - Step 5 - Unload target variable and possible features */


drop table if exists base_population;
drop table if exists base_trans;
drop table if exists event_dates;

drop table if exists cust_pct_spend_in_event;
-- drop table if exists cust_pct_spend_in_event_ly;
-- drop table if exists cust_pct_trips_in_event;
drop table if exists cust_pct_events_shopped;
drop table if exists cust_event_shopped_ly;

drop table if exists cust_stats;


-- base population consists of non-employees who have made at least 1
--  merchandise purchase at FLS or N.COM in the prior 2 years
create temp table base_population (
  cust_key numeric(20) not null
)
  diststyle key
  distkey (cust_key)
  sortkey (cust_key);

insert into base_population (
  select
    cust.cust_key
  from analytics_user_vws.liveramp_customers{0} as cust
  inner join analytics_user_vws.liveramp_trans{0} as trans
    on cust.cust_key = trans.cust_key
  inner join analytics_user_vws.liveramp_funnel{0} as funnel
    on trans.cust_key = funnel.cust_key and trans.sale_dt >= funnel.acq_dt
  where cust.current_employee_ind = 0
  group by cust.cust_key
  having max(trans.spend_discount_employee_ind) = 0
    and max(
      case
        when trans.sale_dt between trunc(dateadd(year, -2, '{3}')) and '{3}'-1
          and trans.merch_type = 'Merch'
          and trans.intent_channel in ('FLS','N.COM')
          and trans.channel <> 'RESTAURANT'
          then 1
        else 0
      end
    ) = 1
);

analyze base_population;


create temp table base_trans (
  cust_key numeric(20) encode zstd,
  tran_key numeric(20) encode zstd,
  tran_line_key numeric(10) encode zstd,
  corp_trip_key varchar(32) encode zstd,
  sale_dt date encode zstd,
  intent_channel varchar(20) encode zstd,
  channel varchar(20) encode zstd,
  merch_type varchar(10) encode zstd,
  prod_div varchar(40) encode zstd,
  prod_merchlevel2 varchar(255) encode zstd,
  prod_gender char(2) encode zstd,
  --prod_age_group varchar(20) encode zstd,
  full_line_sale_ind integer encode zstd,
  spend_gross numeric(12,2) encode zstd,
  tender_giftcard integer encode zstd
)
  diststyle key
  distkey (cust_key)
  sortkey (sale_dt);

insert into base_trans (
  select
    base.cust_key,
    trans.tran_key,
    trans.tran_line_key,
    trans.corp_trip_key,
    trans.sale_dt,
    trans.intent_channel,
    trans.channel,
    trans.merch_type,
    trans.prod_div,
    trans.prod_merchlevel2,
    trans.prod_gender,
    --trans.prod_age_group,
    trans.full_line_sale_ind,
    trans.spend_gross,
    trans.tender_giftcard
  from base_population as base
  inner join analytics_user_vws.liveramp_trans{0} as trans
    on base.cust_key = trans.cust_key
  inner join analytics_user_vws.liveramp_funnel{0} as funnel
    on trans.cust_key = funnel.cust_key and trans.sale_dt >= funnel.acq_dt
  where trans.sale_dt between trunc(dateadd(year, -4, '{3}')) and '{3}'-1
    --and trans.merch_type = 'Merch'
    --and trans.intent_channel in ('FLS','N.COM')
    --and trans.channel not in ('RESTAURANT','UNKNOWN')
);

analyze base_trans;


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
    {4} -- insert select statements in ../json_and_txt/event_dates_select.txt for each
        --   event in ../json_and_txt/mktg_events.json, joining the strings with 'union'
  )
  where end_dt between trunc(dateadd(year, -4, '{3}')) and '{3}'-1
);

analyze event_dates;

--------------------------

create temp table cust_pct_spend_in_event (
  cust_key numeric(20) encode zstd,
  anniversary_early_pct_spend numeric(20,10) encode zstd,
  anniversary_public_pct_spend numeric(20,10) encode zstd,
  december_halfyear_pct_spend numeric(20,10) encode zstd,
  may_halfyear_pct_spend numeric(20,10) encode zstd,
  wellness_pct_spend numeric(20,10) encode zstd,
  spring_fashion_pct_spend numeric(20,10) encode zstd,
  fall_fashion_pct_spend numeric(20,10) encode zstd,
  holiday_light_pct_spend numeric(20,10) encode zstd,
  holiday_full_pct_spend numeric(20,10) encode zstd,
  backtoschool_pct_spend numeric(20,10) encode zstd,
  holiday_dressing_pct_spend numeric(20,10) encode zstd,
  valentines_day_pct_spend numeric(20,10) encode zstd,
  mothers_day_pct_spend numeric(20,10) encode zstd,
  fathers_day_pct_spend numeric(20,10) encode zstd
)
  diststyle key
  distkey (cust_key)
  sortkey (cust_key);

insert into cust_pct_spend_in_event (
  select
    trans.cust_key,
    sum(
      case when events.anniversary_early_access = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as anniversary_early_pct_spend,
    sum(
      case when events.anniversary_public_event = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as anniversary_public_pct_spend,
    sum(
      case when events.december_half_yearly = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as december_halfyear_pct_spend,
    sum(
      case when events.may_half_yearly = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as may_halfyear_pct_spend,
    sum(
      case when events.mktg_wellness = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as wellness_pct_spend,
    sum(
      case when events.mktg_spring_fashion = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as spring_fashion_pct_spend,
    sum(
      case when events.mktg_fall_fashion = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as fall_fashion_pct_spend,
    sum(
      case when events.mktg_holiday_light = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as holiday_light_pct_spend,
    sum(
      case when events.mktg_holiday_full = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as holiday_full_pct_spend,
    sum(
      case when events.mktg_backtoschool = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as backtoschool_pct_spend,
    sum(
      case when events.mktg_holiday_dressing = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as holiday_dressing_pct_spend,
    sum(
      case when events.mktg_valentines_day = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as valentines_day_pct_spend,
    sum(
      case when events.mktg_mothers_day = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as mothers_day_pct_spend,
    sum(
      case when events.mktg_fathers_day = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as fathers_day_pct_spend
  from base_trans as trans
  inner join analytics_user_vws.liveramp_events{0} as events
    on trans.sale_dt = events.event_dt
  where trans.intent_channel in ('FLS','N.COM')
  group by trans.cust_key
);

analyze cust_pct_spend_in_event;

/*
create temp table cust_pct_spend_in_event_ly (
  cust_key numeric(20) encode zstd,
  anniversary_early_pct_spend numeric(20,10) encode zstd,
  anniversary_public_pct_spend numeric(20,10) encode zstd,
  december_halfyear_pct_spend numeric(20,10) encode zstd,
  may_halfyear_pct_spend numeric(20,10) encode zstd,
  wellness_pct_spend numeric(20,10) encode zstd,
  spring_fashion_pct_spend numeric(20,10) encode zstd,
  fall_fashion_pct_spend numeric(20,10) encode zstd,
  holiday_light_pct_spend numeric(20,10) encode zstd,
  holiday_full_pct_spend numeric(20,10) encode zstd,
  backtoschool_pct_spend numeric(20,10) encode zstd,
  holiday_dressing_pct_spend numeric(20,10) encode zstd,
  valentines_day_pct_spend numeric(20,10) encode zstd,
  mothers_day_pct_spend numeric(20,10) encode zstd,
  fathers_day_pct_spend numeric(20,10) encode zstd
)
  diststyle key
  distkey (cust_key)
  sortkey (cust_key);

insert into cust_pct_spend_in_event_ly (
  select
    trans.cust_key,
    sum(
      case when events.anniversary_early_access = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as anniversary_early_pct_spend,
    sum(
      case when events.anniversary_public_event = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as anniversary_public_pct_spend,
    sum(
      case when events.december_half_yearly = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as december_halfyear_pct_spend,
    sum(
      case when events.may_half_yearly = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as may_halfyear_pct_spend,
    sum(
      case when events.mktg_wellness = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as wellness_pct_spend,
    sum(
      case when events.mktg_spring_fashion = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as spring_fashion_pct_spend,
    sum(
      case when events.mktg_fall_fashion = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as fall_fashion_pct_spend,
    sum(
      case when events.mktg_holiday_light = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as holiday_light_pct_spend,
    sum(
      case when events.mktg_holiday_full = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as holiday_full_pct_spend,
    sum(
      case when events.mktg_backtoschool = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as backtoschool_pct_spend,
    sum(
      case when events.mktg_holiday_dressing = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as holiday_dressing_pct_spend,
    sum(
      case when events.mktg_valentines_day = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as valentines_day_pct_spend,
    sum(
      case when events.mktg_mothers_day = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as mothers_day_pct_spend,
    sum(
      case when events.mktg_fathers_day = 1 then trans.spend_gross else 0 end
    )/sum(trans.spend_gross)::float as fathers_day_pct_spend
  from base_trans as trans
  inner join analytics_user_vws.liveramp_events{0} as events
    on trans.sale_dt = events.event_dt
  where trans.sale_dt between trunc(dateadd(year, -1, '{3}')) and '{3}'-1
    and trans.intent_channel in ('FLS','N.COM')
  group by trans.cust_key
);

analyze cust_pct_spend_in_event_ly;
*/
/*
create temp table cust_pct_trips_in_event (
  cust_key numeric(20) encode zstd,
  anniversary_early_pct_trips numeric(20,10) encode zstd,
  anniversary_public_pct_trips numeric(20,10) encode zstd,
  december_halfyear_pct_trips numeric(20,10) encode zstd,
  may_halfyear_pct_trips numeric(20,10) encode zstd,
  wellness_pct_trips numeric(20,10) encode zstd,
  spring_fashion_pct_trips numeric(20,10) encode zstd,
  fall_fashion_pct_trips numeric(20,10) encode zstd,
  holiday_light_pct_trips numeric(20,10) encode zstd,
  holiday_full_pct_trips numeric(20,10) encode zstd,
  backtoschool_pct_trips numeric(20,10) encode zstd,
  holiday_dressing_pct_trips numeric(20,10) encode zstd,
  valentines_day_pct_trips numeric(20,10) encode zstd,
  mothers_day_pct_trips numeric(20,10) encode zstd,
  fathers_day_pct_trips numeric(20,10) encode zstd
)
  diststyle key
  distkey (cust_key)
  sortkey (cust_key);

insert into cust_pct_trips_in_event (
  select
    trans.cust_key,
    count(distinct
      case when events.anniversary_early_access = 1 then trans.corp_trip_key else null end
    )/count(distinct trans.corp_trip_key)::float as anniversary_early_pct_trips,
    count(distinct
      case when events.anniversary_public_event = 1 then trans.corp_trip_key else null end
    )/count(distinct trans.corp_trip_key)::float as anniversary_public_pct_trips,
    count(distinct
      case when events.december_half_yearly = 1 then trans.corp_trip_key else null end
    )/count(distinct trans.corp_trip_key)::float as december_halfyear_pct_trips,
    count(distinct
      case when events.may_half_yearly = 1 then trans.corp_trip_key else null end
    )/count(distinct trans.corp_trip_key)::float as may_halfyear_pct_trips,
    count(distinct
      case when events.mktg_wellness = 1 then trans.corp_trip_key else null end
    )/count(distinct trans.corp_trip_key)::float as wellness_pct_trips,
    count(distinct
      case when events.mktg_spring_fashion = 1 then trans.corp_trip_key else null end
    )/count(distinct trans.corp_trip_key)::float as spring_fashion_pct_trips,
    count(distinct
      case when events.mktg_fall_fashion = 1 then trans.corp_trip_key else null end
    )/count(distinct trans.corp_trip_key)::float as fall_fashion_pct_trips,
    count(distinct
      case when events.mktg_holiday_light = 1 then trans.corp_trip_key else null end
    )/count(distinct trans.corp_trip_key)::float as holiday_light_pct_trips,
    count(distinct
      case when events.mktg_holiday_full = 1 then trans.corp_trip_key else null end
    )/count(distinct trans.corp_trip_key)::float as holiday_full_pct_trips,
    count(distinct
      case when events.mktg_backtoschool = 1 then trans.corp_trip_key else null end
    )/count(distinct trans.corp_trip_key)::float as backtoschool_pct_trips,
    count(distinct
      case when events.mktg_holiday_dressing = 1 then trans.corp_trip_key else null end
    )/count(distinct trans.corp_trip_key)::float as holiday_dressing_pct_trips,
    count(distinct
      case when events.mktg_valentines_day = 1 then trans.corp_trip_key else null end
    )/count(distinct trans.corp_trip_key)::float as valentines_day_pct_trips,
    count(distinct
      case when events.mktg_mothers_day = 1 then trans.corp_trip_key else null end
    )/count(distinct trans.corp_trip_key)::float as mothers_day_pct_trips,
    count(distinct
      case when events.mktg_fathers_day = 1 then trans.corp_trip_key else null end
    )/count(distinct trans.corp_trip_key)::float as fathers_day_pct_trips
  from base_trans as trans
  inner join analytics_user_vws.liveramp_events{0} as events
    on trans.sale_dt = events.event_dt
  where trans.intent_channel in ('FLS','N.COM')
  group by trans.cust_key
);

analyze cust_pct_trips_in_event;
*/

create temp table cust_pct_events_shopped (
  cust_key numeric(20) encode zstd,
  anniversary_early_pct_shopped numeric(20,10) encode zstd,
  anniversary_public_pct_shopped numeric(20,10) encode zstd,
  december_halfyear_pct_shopped numeric(20,10) encode zstd,
  may_halfyear_pct_shopped numeric(20,10) encode zstd,
  wellness_pct_shopped numeric(20,10) encode zstd,
  spring_fashion_pct_shopped numeric(20,10) encode zstd,
  fall_fashion_pct_shopped numeric(20,10) encode zstd,
  holiday_light_pct_shopped numeric(20,10) encode zstd,
  holiday_full_pct_shopped numeric(20,10) encode zstd,
  backtoschool_pct_shopped numeric(20,10) encode zstd,
  holiday_dressing_pct_shopped numeric(20,10) encode zstd,
  valentines_day_pct_shopped numeric(20,10) encode zstd,
  mothers_day_pct_shopped numeric(20,10) encode zstd,
  fathers_day_pct_shopped numeric(20,10) encode zstd
)
  diststyle key
  distkey (cust_key)
  sortkey (cust_key);

insert into cust_pct_events_shopped (
  select
    base.cust_key,
    case
    	when count(distinct case when edates.event = 'anniversary_early_access' then edates.yr else null end) = 0
    		then 0 --- Do I make this `then null` instead?
    	else count(distinct
    		case when edates.event = 'anniversary_early_access' and trans.corp_trip_key is not null then edates.yr else null end
    		)/count(distinct case when edates.event = 'anniversary_early_access' then edates.yr else null end)::float
    end as anniversary_early_pct_shopped,
    case
    	when count(distinct case when edates.event = 'anniversary_public_event' then edates.yr else null end) = 0
    		then 0
    	else count(distinct
    		case when edates.event = 'anniversary_public_event' and trans.corp_trip_key is not null then edates.yr else null end
    		)/count(distinct case when edates.event = 'anniversary_public_event' then edates.yr else null end)::float
    end as anniversary_public_pct_shopped,
    case
    	when count(distinct case when edates.event = 'december_half_yearly' then edates.yr else null end) = 0
    		then 0
    	else count(distinct
    		case when edates.event = 'december_half_yearly' and trans.corp_trip_key is not null then edates.yr else null end
    		)/count(distinct case when edates.event = 'december_half_yearly' then edates.yr else null end)::float
    end as december_halfyear_pct_shopped,
    case
    	when count(distinct case when edates.event = 'may_half_yearly' then edates.yr else null end) = 0
    		then 0
    	else count(distinct
    		case when edates.event = 'may_half_yearly' and trans.corp_trip_key is not null then edates.yr else null end
    		)/count(distinct case when edates.event = 'may_half_yearly' then edates.yr else null end)::float
    end as may_halfyear_pct_shopped,
    case
    	when count(distinct case when edates.event = 'mktg_wellness' then edates.yr else null end) = 0
    		then 0
    	else count(distinct
    		case when edates.event = 'mktg_wellness' and trans.corp_trip_key is not null then edates.yr else null end
    		)/count(distinct case when edates.event = 'mktg_wellness' then edates.yr else null end)::float
    end as wellness_pct_shopped,
    case
    	when count(distinct case when edates.event = 'mktg_spring_fashion' then edates.yr else null end) = 0
    		then 0
    	else count(distinct
    		case when edates.event = 'mktg_spring_fashion' and trans.corp_trip_key is not null then edates.yr else null end
    		)/count(distinct case when edates.event = 'mktg_spring_fashion' then edates.yr else null end)::float
    end as spring_fashion_pct_shopped,
    case
    	when count(distinct case when edates.event = 'mktg_fall_fashion' then edates.yr else null end) = 0
    		then 0
    	else count(distinct
    		case when edates.event = 'mktg_fall_fashion' and trans.corp_trip_key is not null then edates.yr else null end
    		)/count(distinct case when edates.event = 'mktg_fall_fashion' then edates.yr else null end)::float
    end as fall_fashion_pct_shopped,
    case
    	when count(distinct case when edates.event = 'mktg_holiday_light' then edates.yr else null end) = 0
    		then 0
    	else count(distinct
    		case when edates.event = 'mktg_holiday_light' and trans.corp_trip_key is not null then edates.yr else null end
    		)/count(distinct case when edates.event = 'mktg_holiday_light' then edates.yr else null end)::float
    end as holiday_light_pct_shopped,
    case
    	when count(distinct case when edates.event = 'mktg_holiday_full' then edates.yr else null end) = 0
    		then 0
    	else count(distinct
    		case when edates.event = 'mktg_holiday_full' and trans.corp_trip_key is not null then edates.yr else null end
    		)/count(distinct case when edates.event = 'mktg_holiday_full' then edates.yr else null end)::float
    end as holiday_full_pct_shopped,
    case
    	when count(distinct case when edates.event = 'mktg_backtoschool' then edates.yr else null end) = 0
    		then 0
    	else count(distinct
    		case when edates.event = 'mktg_backtoschool' and trans.corp_trip_key is not null then edates.yr else null end
    		)/count(distinct case when edates.event = 'mktg_backtoschool' then edates.yr else null end)::float
    end as backtoschool_pct_shopped,
    case
    	when count(distinct case when edates.event = 'mktg_holiday_dressing' then edates.yr else null end) = 0
    		then 0
    	else count(distinct
    		case when edates.event = 'mktg_holiday_dressing' and trans.corp_trip_key is not null then edates.yr else null end
    		)/count(distinct case when edates.event = 'mktg_holiday_dressing' then edates.yr else null end)::float
    end as holiday_dressing_pct_shopped,
    case
    	when count(distinct case when edates.event = 'mktg_valentines_day' then edates.yr else null end) = 0
    		then 0
    	else count(distinct
    		case when edates.event = 'mktg_valentines_day' and trans.corp_trip_key is not null then edates.yr else null end
    		)/count(distinct case when edates.event = 'mktg_valentines_day' then edates.yr else null end)::float
    end as valentines_day_pct_shopped,
    case
    	when count(distinct case when edates.event = 'mktg_mothers_day' then edates.yr else null end) = 0
    		then 0
    	else count(distinct
    		case when edates.event = 'mktg_mothers_day' and trans.corp_trip_key is not null then edates.yr else null end
    		)/count(distinct case when edates.event = 'mktg_mothers_day' then edates.yr else null end)::float
    end as mothers_day_pct_shopped,
    case
    	when count(distinct case when edates.event = 'mktg_fathers_day' then edates.yr else null end) = 0
    		then 0
    	else count(distinct
    		case when edates.event = 'mktg_fathers_day' and trans.corp_trip_key is not null then edates.yr else null end
    		)/count(distinct case when edates.event = 'mktg_fathers_day' then edates.yr else null end)::float
    end as fathers_day_pct_shopped

  from base_population as base
  inner join analytics_user_vws.liveramp_funnel{0} as funnel
    on base.cust_key = funnel.cust_key
  inner join event_dates as edates
    on funnel.acq_dt <= edates.end_dt
  left join (
    select
      cust_key,
      corp_trip_key,
      sale_dt
    from base_trans
    where intent_channel in ('FLS','N.COM')
  ) as trans
    on base.cust_key = trans.cust_key
      and trans.sale_dt between edates.start_dt and edates.end_dt
  group by base.cust_key
);

analyze cust_pct_events_shopped;

create temp table cust_event_shopped_ly (
  cust_key numeric(20) encode zstd,
  anniversary_early_shopped_ind integer encode zstd,
  anniversary_public_shopped_ind integer encode zstd,
  december_halfyear_shopped_ind integer encode zstd,
  may_halfyear_shopped_ind integer encode zstd,
  wellness_shopped_ind integer encode zstd,
  spring_fashion_shopped_ind integer encode zstd,
  fall_fashion_shopped_ind integer encode zstd,
  holiday_light_shopped_ind integer encode zstd,
  holiday_full_shopped_ind integer encode zstd,
  backtoschool_shopped_ind integer encode zstd,
  holiday_dressing_shopped_ind integer encode zstd,
  valentines_day_shopped_ind integer encode zstd,
  mothers_day_shopped_ind integer encode zstd,
  fathers_day_shopped_ind integer encode zstd
)
  diststyle key
  distkey (cust_key)
  sortkey (cust_key);

insert into cust_event_shopped_ly (
  select
    trans.cust_key,
    max(events.anniversary_early_access) as anniversary_early_shopped_ind,
    max(events.anniversary_public_event) as anniversary_public_shopped_ind,
    max(events.december_half_yearly) as december_halfyear_shopped_ind,
    max(events.may_half_yearly) as may_halfyear_shopped_ind,
    max(events.mktg_wellness) as wellness_shopped_ind,
    max(events.mktg_spring_fashion) as spring_fashion_shopped_ind,
    max(events.mktg_fall_fashion) as fall_fashion_shopped_ind,
    max(events.mktg_holiday_light) as holiday_light_shopped_ind,
    max(events.mktg_holiday_full) as holiday_full_shopped_ind,
    max(events.mktg_backtoschool) as backtoschool_shopped_ind,
    max(events.mktg_holiday_dressing) as holiday_dressing_shopped_ind,
    max(events.mktg_valentines_day) as valentines_day_shopped_ind,
    max(events.mktg_mothers_day) as mothers_day_shopped_ind,
    max(events.mktg_fathers_day) as fathers_day_shopped_ind
  from base_trans as trans
  inner join analytics_user_vws.liveramp_events{0} as events
    on trans.sale_dt = events.event_dt
  where trans.sale_dt between trunc(dateadd(year, -1, '{3}')) and '{3}'-1
    and trans.intent_channel in ('FLS','N.COM')
  group by trans.cust_key
);

analyze cust_event_shopped_ly;

-----------------------------------

create temp table cust_stats (
  cust_key numeric(20) encode zstd,
  fl_items bigint encode zstd,
  sale_items bigint encode zstd,
  fl_spend numeric(20,2) encode zstd,
  merch_spend numeric(20,2) encode zstd,
  womens_prod_spend numeric(20,2) encode zstd,
  mens_prod_spend numeric(20,2) encode zstd,
  accessories_spend numeric(20,2) encode zstd,
  apparel_spend numeric(20,2) encode zstd,
  babypet_spend numeric(20,2) encode zstd,
  bag_spend numeric(20,2) encode zstd,
  beauty_spend numeric(20,2) encode zstd,
  entertainment_spend numeric(20,2) encode zstd,
  food_spend numeric(20,2) encode zstd,
  home_spend numeric(20,2) encode zstd,
  jewelry_spend numeric(20,2) encode zstd,
  other_spend numeric(20,2) encode zstd,
  shoe_spend numeric(20,2) encode zstd,
  giftcard_spend numeric(20,2) encode zstd,
  web_spend numeric(20,2) encode zstd,
  rack_spend numeric(20,2) encode zstd,
  jwn_spend numeric(20,2) encode zstd,
  fl_trips bigint encode zstd,
  fl_divs integer encode zstd,
  jwn_channels integer encode zstd,
  fl_last_sale_dt date encode zstd,
  fl_shopped_ly_ind integer encode zstd,
  fl_spend_ly numeric(20,2) encode zstd,
  fl_trips_ly bigint encode zstd
)
  diststyle key
  distkey (cust_key)
  sortkey (cust_key);

insert into cust_stats (
  select
    cust_key,
    sum(
      case
        when intent_channel in ('FLS','N.COM')
          then 1
        else 0
      end
    ) as fl_items,
    sum(
      case
        when intent_channel in ('FLS','N.COM')
          then full_line_sale_ind
        else 0
      end
    ) as sale_items,
    sum(
      case
        when intent_channel in ('FLS','N.COM')
          then spend_gross
        else 0
      end
    ) as fl_spend,
    sum(
      case
        when intent_channel in ('FLS','N.COM') and merch_type = 'Merch' and channel <> 'RESTAURANT'
          then spend_gross
        else 0
      end
    ) as merch_spend,
    sum(
      case
        when intent_channel in ('FLS','N.COM') and prod_gender = 'F'
          then spend_gross
        else 0
      end
    ) as womens_prod_spend, -- Make clear these are gender-specific PRODUCTS, not customer-related
    sum(
      case
        when intent_channel in ('FLS','N.COM') and prod_gender = 'M'
          then spend_gross
        else 0
      end
    ) as mens_prod_spend, -- Make clear these are gender-specific PRODUCTS, not customer-related
    sum(
      case
        when intent_channel in ('FLS','N.COM')
          and prod_merchlevel2 in ('Umbrellas','Small Leather Goods','Belts & Braces',
                                   'Apparel care','Eyewear','Neckwear','Gloves/Mittens',
                                   'Headwear','Scarves/Wraps/Ponchos')
          then spend_gross
        else 0
      end
    ) as accessories_spend,
    sum(
      case
        when intent_channel in ('FLS','N.COM')
          and prod_merchlevel2 in ('Suits/Sets/Wardrobers','Bottoms','Sleepwear',
                                   'Dresses','Hosiery','Swimwear','Tops',
                                   'Jumpsuits/Coveralls','Jacket/Sportcoat',
                                   'Outerwear','Underwear/Lingerie')
          then spend_gross
        else 0
      end
    ) as apparel_spend,
    sum(
      case
        when intent_channel in ('FLS','N.COM')
          and prod_merchlevel2 in ('Baby Accessories','Pet Accessories')
          then spend_gross
        else 0
      end
    ) as babypet_spend,
    sum(
      case
        when intent_channel in ('FLS','N.COM') and prod_merchlevel2 = 'Bags'
          then spend_gross
        else 0
      end
    ) as bag_spend,
    sum(
      case
        when intent_channel in ('FLS','N.COM')
          and prod_merchlevel2 in ('Fragrance','Personal Care Accessories',
                                   'Hair Accessories','Hair Care','Makeup',
                                   'Skin/Body Treatment')
          then spend_gross
        else 0
      end
    ) as beauty_spend,
    sum(
      case
        when intent_channel in ('FLS','N.COM')
          and prod_merchlevel2 in ('Toys/Games', 'Recreation/Entertainment')
          then spend_gross
        else 0
      end
    ) as entertainment_spend,
    sum(
      case
        when intent_channel in ('FLS','N.COM')
          and (prod_merchlevel2 = 'Food' or merch_type = 'Restaurant')
          then spend_gross
        else 0
      end
    ) as food_spend,
    sum(
      case
        when intent_channel in ('FLS','N.COM')
          and prod_merchlevel2 in ('Home','Memorabilia & Collectibles',
                                   'Stationery/Giftwrap')
          then spend_gross
        else 0
      end
    ) as home_spend,
    sum(
      case
        when intent_channel in ('FLS','N.COM')
          and prod_merchlevel2 in ('Jewelry', 'Jewelry Care')
          then spend_gross
        else 0
      end
    ) as jewelry_spend,
    sum(
      case
        when intent_channel in ('FLS','N.COM')
          and prod_merchlevel2 in ('Gift/Operational','DNU I','NOT USED I')
          then spend_gross
        else 0
      end
    ) as other_spend,
    sum(
      case
        when intent_channel in ('FLS','N.COM')
          and prod_merchlevel2 in ('Shoes', 'Shoe care')
          then spend_gross
        else 0
      end
    ) as shoe_spend,
    sum(
      case
        when intent_channel in ('FLS','N.COM') and tender_giftcard = 1
          then spend_gross
        else 0
      end
    ) as giftcard_spend,
    sum(
      case
        when intent_channel = 'N.COM'
          then spend_gross
        else 0
      end
    ) as web_spend,
    sum(
      case
        when intent_channel = 'RACK'
          then spend_gross
        else 0
      end
    ) as rack_spend,
    sum(spend_gross) as jwn_spend,
    count(distinct
      case
        when intent_channel in ('FLS','N.COM')
          then corp_trip_key
        else null
      end
    ) as fl_trips,
    count(distinct
      case
        when intent_channel in ('FLS','N.COM')
          then prod_div
        else null
      end
    ) as fl_divs,
    count(distinct intent_channel) as jwn_channels,
    max(
      case
        when intent_channel in ('FLS','N.COM')
          then sale_dt
        else null
      end
    ) as fl_last_sale_dt,
    max(
      case
        when intent_channel in ('FLS','N.COM')
          and sale_dt between trunc(dateadd(year, -1, '{3}')) and '{3}'-1
          then 1
        else 0
      end
    ) as fl_shopped_ly_ind,
    sum(
      case
        when intent_channel in ('FLS','N.COM')
          and sale_dt between trunc(dateadd(year, -1, '{3}')) and '{3}'-1
          then spend_gross
        else 0
      end
    ) as fl_spend_ly,
    count(distinct
      case
        when intent_channel in ('FLS','N.COM')
          and sale_dt between trunc(dateadd(year, -1, '{3}')) and '{3}'-1
          then corp_trip_key
        else null
      end
    ) as fl_trips_ly
  from base_trans
  group by cust_key
);

analyze cust_stats;

unload('
select
  \'cust_key\',
  \'persona\',
  \'target_shopped_ind\',

  \'tenure_total_months\',
  \'loyalty_tender_ind\',
  \'loyalty_nontender_ind\',

  \'fl_total_spend\',
  \'fl_total_trips\',
  \'fl_avg_spend_per_trip\',
  \'sale_pct_items\',
  \'merch_pct_spend\',
  \'womens_prod_pct_spend\',
  \'mens_prod_pct_spend\',
  \'accessories_pct_spend\',
  \'apparel_pct_spend\',
  \'babypet_pct_spend\',
  \'bag_pct_spend\',
  \'beauty_pct_spend\',
  \'entertainment_pct_spend\',
  \'food_pct_spend\',
  \'home_pct_spend\',
  \'jewelry_pct_spend\',
  \'other_pct_spend\',
  \'shoe_pct_spend\',
  \'giftcard_pct_spend\',
  \'web_pct_spend\',
  \'rack_pct_spend\',
  \'fl_total_divs\',
  \'total_channels\',
  \'months_since_last_sale\',

  \'fl_shopped_ly_ind\',
  \'fl_total_spend_ly\',

  \'anniversary_early_pct_spend\',
  \'anniversary_public_pct_spend\',
  \'december_halfyear_pct_spend\',
  \'may_halfyear_pct_spend\',
  \'wellness_pct_spend\',
  \'spring_fashion_pct_spend\',
  \'fall_fashion_pct_spend\',
  \'holiday_light_pct_spend\',
  \'holiday_full_pct_spend\',
  \'backtoschool_pct_spend\',
  \'holiday_dressing_pct_spend\',
  \'valentines_day_pct_spend\',
  \'mothers_day_pct_spend\',
  \'fathers_day_pct_spend\',

  \'anniversary_early_pct_shopped\',
  \'anniversary_public_pct_shopped\',
  \'december_halfyear_pct_shopped\',
  \'may_halfyear_pct_shopped\',
  \'wellness_pct_shopped\',
  \'spring_fashion_pct_shopped\',
  \'fall_fashion_pct_shopped\',
  \'holiday_light_pct_shopped\',
  \'holiday_full_pct_shopped\',
  \'backtoschool_pct_shopped\',
  \'holiday_dressing_pct_shopped\',
  \'valentines_day_pct_shopped\',
  \'mothers_day_pct_shopped\',
  \'fathers_day_pct_shopped\',

  \'anniversary_early_shop_ly_ind\',
  \'anniversary_public_shop_ly_ind\',
  \'december_halfyear_shop_ly_ind\',
  \'may_halfyear_shop_ly_ind\',
  \'wellness_shop_ly_ind\',
  \'spring_fashion_shop_ly_ind\',
  \'fall_fashion_shop_ly_ind\',
  \'holiday_light_shop_ly_ind\',
  \'holiday_full_shop_ly_ind\',
  \'backtoschool_shop_ly_ind\',
  \'holiday_dressing_shop_ly_ind\',
  \'valentines_day_shop_ly_ind\',
  \'mothers_day_shop_ly_ind\',
  \'fathers_day_shop_ly_ind\'

union

select
  cast(base.cust_key as varchar(32)),
  cast(persona.persona as varchar(32)),
  cast(coalesce(target.target_shopped_ind,0) as varchar(32)),

  cast(months_between(\'{3}\', funnel.acq_dt) as varchar(32)),
  cast(coalesce(loyal.loyalty_tender_ind,0) as varchar(32)),
  cast(coalesce(loyal.loyalty_nontender_ind,0) as varchar(32)),

  cast(coalesce(stats.fl_spend,0) as varchar(32)),
  cast(coalesce(stats.fl_trips,0) as varchar(32)),
  cast(case
      when coalesce(stats.fl_trips,0) = 0 then 0
      else stats.fl_spend/stats.fl_trips::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.fl_items,0) = 0 then 0
      else 100*stats.sale_items/stats.fl_items::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.fl_spend,0) = 0 then 0
      else 100*stats.merch_spend/stats.fl_spend::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.fl_spend,0) = 0 then 0
      else 100*stats.womens_prod_spend/stats.fl_spend::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.fl_spend,0) = 0 then 0
      else 100*stats.mens_prod_spend/stats.fl_spend::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.fl_spend,0) = 0 then 0
      else 100*stats.accessories_spend/stats.fl_spend::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.fl_spend,0) = 0 then 0
      else 100*stats.apparel_spend/stats.fl_spend::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.fl_spend,0) = 0 then 0
      else 100*stats.babypet_spend/stats.fl_spend::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.fl_spend,0) = 0 then 0
      else 100*stats.bag_spend/stats.fl_spend::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.fl_spend,0) = 0 then 0
      else 100*stats.beauty_spend/stats.fl_spend::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.fl_spend,0) = 0 then 0
      else 100*stats.entertainment_spend/stats.fl_spend::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.fl_spend,0) = 0 then 0
      else 100*stats.food_spend/stats.fl_spend::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.fl_spend,0) = 0 then 0
      else 100*stats.home_spend/stats.fl_spend::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.fl_spend,0) = 0 then 0
      else 100*stats.jewelry_spend/stats.fl_spend::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.fl_spend,0) = 0 then 0
      else 100*stats.other_spend/stats.fl_spend::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.fl_spend,0) = 0 then 0
      else 100*stats.shoe_spend/stats.fl_spend::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.fl_spend,0) = 0 then 0
      else 100*stats.giftcard_spend/stats.fl_spend::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.fl_spend,0) = 0 then 0
      else 100*stats.web_spend/stats.fl_spend::float
    end as varchar(32)),
  cast(case
      when coalesce(stats.jwn_spend,0) = 0 then 0
      else 100*coalesce(stats.rack_spend,0)/stats.jwn_spend::float
    end as varchar(32)),
  cast(coalesce(stats.fl_divs,0) as varchar(32)),
  cast(coalesce(stats.jwn_channels,0) as varchar(32)),
  cast(months_between(\'{1}\', stats.fl_last_sale_dt) as varchar(32)),

  cast(coalesce(stats.fl_shopped_ly_ind,0) as varchar(32)),
  cast(coalesce(stats.fl_spend_ly,0) as varchar(32)),

  cast(100*coalesce(espend.anniversary_early_pct_spend,0) as varchar(32)),
  cast(100*coalesce(espend.anniversary_public_pct_spend,0) as varchar(32)),
  cast(100*coalesce(espend.december_halfyear_pct_spend,0) as varchar(32)),
  cast(100*coalesce(espend.may_halfyear_pct_spend,0) as varchar(32)),
  cast(100*coalesce(espend.wellness_pct_spend,0) as varchar(32)),
  cast(100*coalesce(espend.spring_fashion_pct_spend,0) as varchar(32)),
  cast(100*coalesce(espend.fall_fashion_pct_spend,0) as varchar(32)),
  cast(100*coalesce(espend.holiday_light_pct_spend,0) as varchar(32)),
  cast(100*coalesce(espend.holiday_full_pct_spend,0) as varchar(32)),
  cast(100*coalesce(espend.backtoschool_pct_spend,0) as varchar(32)),
  cast(100*coalesce(espend.holiday_dressing_pct_spend,0) as varchar(32)),
  cast(100*coalesce(espend.valentines_day_pct_spend,0) as varchar(32)),
  cast(100*coalesce(espend.mothers_day_pct_spend,0) as varchar(32)),
  cast(100*coalesce(espend.fathers_day_pct_spend,0) as varchar(32)),

  cast(100*coalesce(shopped.anniversary_early_pct_shopped,0) as varchar(32)),
  cast(100*coalesce(shopped.anniversary_public_pct_shopped,0) as varchar(32)),
  cast(100*coalesce(shopped.december_halfyear_pct_shopped,0) as varchar(32)),
  cast(100*coalesce(shopped.may_halfyear_pct_shopped,0) as varchar(32)),
  cast(100*coalesce(shopped.wellness_pct_shopped,0) as varchar(32)),
  cast(100*coalesce(shopped.spring_fashion_pct_shopped,0) as varchar(32)),
  cast(100*coalesce(shopped.fall_fashion_pct_shopped,0) as varchar(32)),
  cast(100*coalesce(shopped.holiday_light_pct_shopped,0) as varchar(32)),
  cast(100*coalesce(shopped.holiday_full_pct_shopped,0) as varchar(32)),
  cast(100*coalesce(shopped.backtoschool_pct_shopped,0) as varchar(32)),
  cast(100*coalesce(shopped.holiday_dressing_pct_shopped,0) as varchar(32)),
  cast(100*coalesce(shopped.valentines_day_pct_shopped,0) as varchar(32)),
  cast(100*coalesce(shopped.mothers_day_pct_shopped,0) as varchar(32)),
  cast(100*coalesce(shopped.fathers_day_pct_shopped,0) as varchar(32)),

  cast(coalesce(shoppedly.anniversary_early_shopped_ind,0) as varchar(32)),
  cast(coalesce(shoppedly.anniversary_public_shopped_ind,0) as varchar(32)),
  cast(coalesce(shoppedly.december_halfyear_shopped_ind,0) as varchar(32)),
  cast(coalesce(shoppedly.may_halfyear_shopped_ind,0) as varchar(32)),
  cast(coalesce(shoppedly.wellness_shopped_ind,0) as varchar(32)),
  cast(coalesce(shoppedly.spring_fashion_shopped_ind,0) as varchar(32)),
  cast(coalesce(shoppedly.fall_fashion_shopped_ind,0) as varchar(32)),
  cast(coalesce(shoppedly.holiday_light_shopped_ind,0) as varchar(32)),
  cast(coalesce(shoppedly.holiday_full_shopped_ind,0) as varchar(32)),
  cast(coalesce(shoppedly.backtoschool_shopped_ind,0) as varchar(32)),
  cast(coalesce(shoppedly.holiday_dressing_shopped_ind,0) as varchar(32)),
  cast(coalesce(shoppedly.valentines_day_shopped_ind,0) as varchar(32)),
  cast(coalesce(shoppedly.mothers_day_shopped_ind,0) as varchar(32)),
  cast(coalesce(shoppedly.fathers_day_shopped_ind,0) as varchar(32))

from base_population as base
inner join analytics_user_vws.liveramp_model_personas_clusters{0} as persona
  on base.cust_key = persona.cust_key
inner join analytics_user_vws.liveramp_funnel{0} as funnel
  on base.cust_key = funnel.cust_key
left join analytics_user_vws.liveramp_loyalty{0} as loyal
  on base.cust_key = loyal.cust_key
left join cust_stats as stats
  on base.cust_key = stats.cust_key
left join cust_pct_spend_in_event as espend
  on base.cust_key = espend.cust_key
left join cust_pct_events_shopped as shopped
  on base.cust_key = shopped.cust_key
left join cust_event_shopped_ly as shoppedly
  on base.cust_key = shoppedly.cust_key
left join (
  select
    trans.cust_key,
    1 as target_shopped_ind
  from analytics_user_vws.liveramp_trans{0} as trans
  inner join analytics_user_vws.liveramp_funnel{0} as funnel
    on trans.cust_key = funnel.cust_key and trans.sale_dt >= funnel.acq_dt
  where trans.sale_dt between \'{1}\' and \'{2}\'
    -- and trans.merch_type = \'Merch\'
    and trans.intent_channel in (\'FLS\',\'N.COM\')
    -- and trans.channel <> \'RESTAURANT\'
  group by trans.cust_key
) as target
  on base.cust_key = target.cust_key

order by 2 desc, 3 desc;
')
to 's3://{5}/{6}{7}'
credentials '{8}'
parallel off gzip allowoverwrite;
