
/*This is based on Corp Calendar available on Nordnet as of 11/30/2017
Some events don't have a regular cadence due to realignment caused by 53 weeks in certain years which has been addressed
Some events like easter and past years events have been hardcoded as events shifted a lot due to chaging times and inclusion of more corporate events
Fiscal calendar has been used to automate most of the dates
Please note that events/campaign date might be mutually exclusive
*/

drop table if exists cust_usr.c01f_event_lookup;
drop table if exists yearly_holidays;
drop table if exists temp_event_lookup;

create temp table yearly_holidays (
  yr_cal integer,
  new_years_day date,
  valentines_day date,
  easter date,
  mothers_day date,
  memorial_day date,
  fathers_day date,
  labor_day date,
  thanksgiving date,
  christmas date
)
  distkey (yr_cal);

insert into yearly_holidays (
  select
    years.yr_cal,
    (years.yr_cal||'0101')::date as new_years_day,
    (years.yr_cal||'0214')::date as valentines_day,
    easters.day_dt as easter,
    next_day((years.yr_cal||'0430')::date,'Sunday')+7 as mothers_day,
    next_day((years.yr_cal||'0531')::date,'Monday')-7 as memorial_day,
    next_day((years.yr_cal||'0531')::date,'Sunday')+14 as fathers_day,
    next_day((years.yr_cal||'0831')::date,'Monday') as labor_day,
    next_day((years.yr_cal||'1031')::date,'Thursday')+21 as thanksgiving,
    (years.yr_cal||'1225')::date as christmas
  from (
    select
      extract(year from day_dt) as yr_cal
    from cust_vws.time_day_lkp_vw
    where day_dt between '2013-01-01' and sysdate-1
    group by yr_cal
  ) as years
  left join (
    select
      day_dt,
      extract(year from day_dt) as yr_cal
    from cust_vws.time_day_lkp_vw
    where day_dt in ('2013-03-31', '2014-04-20', '2015-04-05',
      '2016-03-27', '2017-04-16', '2018-04-01', '2019-04-21',
      '2020-04-12', '2021-04-04', '2022-04-17', '2023-04-09',
      '2024-03-21', '2025-04-20', '2026-04-05', '2027-03-28')
  ) as easters
    on years.yr_cal = easters.yr_cal
);

analyze yearly_holidays;


create temp table temp_event_lookup (
  day_dt date,
  anniversary_early_access integer,
  anniversary_l4_preshop integer,
  anniversary_public_event integer,
  black_friday_cyber_monday integer,
  christmas integer,
  december_half_yearly integer,
  easter integer,
  fathers_day integer,
  mothers_day integer,
  thanksgiving integer,
  valentines_day integer,
  labor_day_clearance integer,
  march_triple_rewards integer,
  may_triple_rewards integer,
  september_triple_rewards integer,
  fall_clearance integer,
  june_summer_clearance integer,
  may_half_yearly integer,
  winter_clearance integer,
  mktg_wellness integer,
  mktg_spring_fashion integer,
  mktg_fall_fashion integer,
  mktg_holiday_light integer,
  mktg_holiday_full integer,
  mktg_backtoschool integer,
  mktg_holiday_dressing integer,
  mktg_valentines_day integer,
  mktg_mothers_day integer,
  mktg_fathers_day integer
)
  distkey (day_dt);

insert into temp_event_lookup (
  select
    cal.day_dt,
    case
      when cal.yr_cal <= 2015 and cal.mth_454 = 6
        and ((cal.wk_454 = 1 and cal.day_454 >= 4) or (cal.wk_454 = 2 and cal.day_454 <= 5))
        then 1
      when cal.yr_cal > 2015 and cal.mth_454 = 6
        and ((cal.wk_454+xtra.wk = 2 and cal.day_454 >= 5) or (cal.wk_454+xtra.wk = 3 and cal.day_454 <= 5))
        then 1
      else 0
    end as anniversary_early_access,
    case
      when cal.day_dt = '2015-07-08'
        then 1
      when cal.yr_cal > 2015 and cal.mth_454 = 6 and cal.wk_454+xtra.wk = 2 and cal.day_454 = 4
        then 1
      else 0
    end as anniversary_l4_preshop,
    case
      when cal.yr_cal <= 2015
        and (
          (cal.mth_454 = 6 and cal.wk_454 = 2 and cal.day_454 >= 6)
            or (cal.mth_454 = 6 and cal.wk_454 in (3,4))
            or (cal.mth_454 = 7 and cal.wk_454 = 1 and cal.day_454 = 1)
        )
        then 1
      when cal.yr_cal > 2015
        and (
          (cal.mth_454 = 6 and cal.wk_454+xtra.wk = 3 and cal.day_454 >= 6)
            or (cal.mth_454 = 6 and cal.wk_454+xtra.wk in (4,5))
            or (cal.mth_454 = 7 and cal.wk_454+xtra.wk = 1)
            or (cal.mth_454 = 7 and cal.wk_454+xtra.wk = 2 and cal.day_454 = 1)
        )
        then 1
      else 0
    end as anniversary_public_event,
    case
      when cal.day_dt between holi.thanksgiving + 1 and holi.thanksgiving + 4
        then 1
      else 0
    end as black_friday_cyber_monday,        --above is a calculation to get to the black friday day to cyber monday for each year
    case
      when cal.day_dt = holi.christmas
        then 1
      else 0
    end as christmas,
    case
      when cal.day_dt = '2013-01-06'     --hardcoding as per Nordnet events calendar
        then 1
      when cal.day_dt > holi.christmas
        then 1
      when cal.yr_cal <= 2016
        and cal.day_dt <= next_day(holi.new_years_day,'Sunday')
        then 1 ---aligning with events calendar
      when cal.yr_cal > 2016
        and cal.day_dt <= holi.new_years_day + 1
        then 1  --aligning with events calendar
      else 0
    end as december_half_yearly,
    case
      when cal.day_dt = holi.easter
        then 1
      else 0
    end as easter,
    case
      when cal.day_dt = holi.fathers_day
        then 1
      else 0
    end as fathers_day,
    case
      when cal.day_dt = holi.mothers_day
        then 1
      else 0
    end as mothers_day,
    case
      when cal.day_dt = holi.thanksgiving
        then 1
      else 0
    end as thanksgiving,
    case
      when cal.day_dt = holi.valentines_day
        then 1
      else 0
    end as valentines_day,
    case
      when cal.day_dt between '2013-08-28' and '2013-09-15'
        then 1
      when cal.yr_cal in (2014, 2015)
        and cal.day_dt between holi.labor_day-5 and holi.labor_day+6
        then 1
      when cal.yr_cal > 2015
        and cal.day_dt between holi.labor_day-3 and holi.labor_day+6
        then 1
      else 0
    end as labor_day_clearance,
    case
      when cal.yr_cal <= 2015 and cal.mth_454 = 2
        and ((cal.wk_454 = 3 and cal.day_454 >= 4) or (cal.wk_454 = 4 and cal.day_454 = 1))
        then 1
      when cal.yr_cal > 2015 and cal.mth_454 = 2
        and ((cal.wk_454+xtra.wk = 4 and cal.day_454 >= 4) or (cal.wk_454+xtra.wk = 5 and cal.day_454 = 1))
        then 1
      else 0
    end as march_triple_rewards,
    case
      when cal.yr_cal >= 2015 and cal.mth_454 = 4
        and ((cal.wk_454 = 1 and cal.day_454 >= 4) or (cal.wk_454 = 2 and cal.day_454 = 1))
        then 1	 			--may triple started from 2015
      else 0
    end as may_triple_rewards,
    case
      when cal.yr_cal <= 2015 and cal.mth_454 = 8
        and ((cal.wk_454 = 3 and cal.day_454 >= 4) or (cal.wk_454 = 4 and cal.day_454 = 1))
        then 1
      when cal.yr_cal > 2015 and cal.mth_454 = 8
        and ((cal.wk_454+xtra.wk = 4 and cal.day_454 >= 4) or (cal.wk_454+xtra.wk = 5 and cal.day_454 = 1))
        then 1
      else 0
    end as september_triple_rewards,
    case
      when cal.yr_cal < 2015 and cal.mth_454 = 10
        and ((cal.wk_454 = 1 and cal.day_454 >= 4) or cal.wk_454 = 2 or (cal.wk_454 = 3 and cal.day_454 = 1))
        then 1
      when cal.yr_cal >= 2015
        and (
          (cal.mth_454 = 9 and cal.wk_454+xtra.wk = 5 and cal.day_454 >= 6)
            or (cal.mth_454 = 10 and cal.wk_454+xtra.wk = 1 and cal.day_454 >= 5)
            or (cal.mth_454 = 10 and cal.wk_454+xtra.wk = 2)
            or (cal.mth_454 = 10 and cal.wk_454+xtra.wk = 3 and cal.day_454 = 1)
        )
        then 1
      else 0
    end as fall_clearance,
    case
      when cal.yr_cal < 2015 and cal.mth_454 = 5
        and ((cal.wk_454 = 2 and cal.day_454 >= 6) or cal.wk_454 = 3 or (cal.wk_454 = 4 and cal.day_454 = 1))
        then 1
      when cal.yr_cal = 2015 and cal.mth_454 = 5
        and ((cal.wk_454 = 3 and cal.day_454 >= 6) or cal.wk_454 = 4 or (cal.wk_454 = 5 and cal.day_454 = 1))
        then 1
      else 0
    end as june_summer_clearance,	--looks like we don't have these events after 2015
    case
      when cal.yr_cal <= 2015
        and (
          (cal.mth_454 = 4 and cal.wk_454 = 3 and cal.day_454 >= 4)
            or (cal.mth_454 = 4 and cal.wk_454 = 4)
            or (cal.mth_454 = 5 and cal.wk_454 = 1 and cal.day_454 = 1)
        )
        then 1
      when cal.yr_cal > 2015
        and (
          (cal.mth_454 = 4 and cal.wk_454+xtra.wk = 4 and cal.day_454 >= 4)
            or (cal.mth_454 = 4 and cal.wk_454+xtra.wk = 5)
            or (cal.mth_454 = 5 and cal.wk_454+xtra.wk = 1)
            or (cal.mth_454 = 5 and cal.wk_454+xtra.wk = 2 and cal.day_454 = 1)
        )
        then 1
      else 0
    end as may_half_yearly,
    case
      when cal.yr_cal < 2015
        and (
          (cal.mth_454 = 1 and cal.wk_454 = 2 and cal.day_454 >= 4)
            or (cal.mth_454 = 1 and cal.wk_454 in (3,4))
            or (cal.mth_454 = 2 and cal.wk_454 = 1 and cal.day_454 = 1)
        )
        then 1
      when cal.yr_cal = 2015 and cal.mth_454 = 1
        and ((cal.wk_454 = 2 and cal.day_454 >= 4) or cal.wk_454 = 3 or (cal.wk_454 = 4 and cal.day_454 = 1))
        then 1
      when cal.yr_cal = 2016 and cal.mth_454 = 1
        and ((cal.wk_454 = 2 and cal.day_454 >= 6) or cal.wk_454 = 3 or (cal.wk_454 = 4 and cal.day_454 = 1))
        then 1
      when cal.yr_cal >= 2017
        and (
          (cal.mth_454 = 1 and cal.wk_454+xtra.wk = 3 and cal.day_454 >= 6)
            or (cal.mth_454 = 1 and cal.wk_454+xtra.wk = 4)
            or (cal.mth_454 = 1 and cal.wk_454+xtra.wk = 5 and cal.day_454 = 1)
            or (cal.mth_454 = 2 and cal.wk_454+xtra.wk = 1 and cal.day_454 = 1)
        )
        then 1
      else 0
    end as winter_clearance,
    case
      when cal.day_dt > holi.christmas
        then 1
      when cal.mth_cal in (1,2)
        then 1
      else 0
    end as mktg_wellness,
    case
      when cal.mth_cal in (2,3)
        then 1
      else 0
    end as mktg_spring_fashion,
    case
    when (cal.mth_454 = 7 and cal.wk_454 in (3,4)) or (cal.mth_454 = 8 and cal.wk_454 in (1,2))
      then 1
    when cal.mth_cal = 9
      then 1
      else 0
    end as mktg_fall_fashion,
    case
      when cal.day_dt = '2015-10-26'
        then 0 --hardcoding as 2015 is exception
      when (cal.mth_454 = 9 and cal.wk_454 =4 and cal.day_454 >= 2) or (cal.mth_454 = 10 and cal.wk_454 in (1,2))
        then 1
      when cal.day_dt between (cal.yr_cal||'1101')::date and holi.thanksgiving
        then 1
      else 0
    end as mktg_holiday_light,
    case
      when day_dt between holi.thanksgiving + 1 and holi.christmas - 1
        then 1
      else 0
    end as mktg_holiday_full,
    case
      when cal.mth_454 = 8 and cal.wk_454 in (1,2,3)
        then 1
      when cal.mth_cal = 8
        then 1
      else 0
    end as mktg_backtoschool,
    case
      when (cal.mth_454 = 9 and cal.wk_454 in (3,4)) or cal.mth_454 = 10 or (cal.mth_454 = 11 and cal.wk_454 = 1)
        then 1
      when cal.mth_cal = 12
        then 1
      else 0
    end as mktg_holiday_dressing,
    case
      when cal.day_dt between holi.new_years_day and holi.valentines_day
        then 1
      else 0
    end as mktg_valentines_day,        --this focuses on the full marketing timeframe
    case
      when (cal.mth_454 = 3 and cal.wk_454 in (2,3,4,5))
        or (cal.mth_454 = 4 and cal.wk_454 = 1)
        or (cal.mth_454 = 4 and cal.wk_454 = 2 and cal.day_454 = 1)
        then 1
      when cal.day_dt between (cal.yr_cal||'0501')::date and holi.mothers_day
        then 1
      else 0
    end as mktg_mothers_day,           --this focuses on the full marketing timeframe
    case
      when day_dt between holi.mothers_day + 1 and holi.fathers_day
        then 1
      else 0
    end as mktg_fathers_day           --this focuses on the full marketing timeframe

  from (
    select
      day_dt,
      mth_454,
      wk_454,
      day_454,
      extract(year from day_dt) as yr_cal,
      extract(month from day_dt) as mth_cal
    from cust_vws.time_day_lkp_vw
    where day_dt between '2013-01-01' and sysdate-1
  )	as cal

  inner join yearly_holidays as holi
    on cal.yr_cal = holi.yr_cal

  inner join (
    select
      yr_454+1 as yr_cal,
      case
        when max(wk_454) = 5
          then 1
        else 0
      end as wk
    from cust_vws.time_day_lkp_vw
    where day_dt between '2012-01-01' and dateadd(year,2,sysdate)
      and mth_454 = 12
    group by yr_454
  ) as xtra
    on cal.yr_cal = xtra.yr_cal  --this part takes care of the years which have 53 weeks, forcing the events team to realign in the rest of the year
);

analyze temp_event_lookup;


create table cust_usr.c01f_event_lookup (
  day_dt date,
  anniversary_early_access integer,
  anniversary_l4_preshop integer,
  anniversary_public_event integer,
  black_friday_cyber_monday integer,
  christmas integer,
  december_half_yearly integer,
  easter integer,
  fathers_day integer,
  mothers_day integer,
  thanksgiving integer,
  valentines_day integer,
  labor_day_clearance integer,
  march_triple_rewards integer,
  may_triple_rewards integer,
  september_triple_rewards integer,
  fall_clearance integer,
  june_summer_clearance integer,
  may_half_yearly integer,
  winter_clearance integer,
  mktg_wellness integer,
  mktg_spring_fashion integer,
  mktg_fall_fashion integer,
  mktg_holiday_light integer,
  mktg_holiday_full integer,
  mktg_backtoschool integer,
  mktg_holiday_dressing integer,
  mktg_valentines_day integer,
  mktg_mothers_day integer,
  mktg_fathers_day integer,
  holiday integer,
  loyalty integer,
  sale integer,
  holiday_loyalty integer,
  holiday_sale integer,
  loyalty_sale integer,
  holiday_loyalty_sale integer,
  data_last_updated timestamp encode zstd
)
  distkey (day_dt);

insert into cust_usr.c01f_event_lookup (
  select
    day_dt,
    anniversary_early_access,
    anniversary_l4_preshop,
    anniversary_public_event,
    black_friday_cyber_monday,
    christmas,
    december_half_yearly,
    easter,
    fathers_day,
    mothers_day,
    thanksgiving,
    valentines_day,
    labor_day_clearance,
    march_triple_rewards,
    may_triple_rewards,
    september_triple_rewards,
    fall_clearance,
    june_summer_clearance,
    may_half_yearly,
    winter_clearance,
    mktg_wellness,
    mktg_spring_fashion,
    mktg_fall_fashion,
    mktg_holiday_light,
    mktg_holiday_full,
    mktg_backtoschool,
    mktg_holiday_dressing,
    mktg_valentines_day,
    mktg_mothers_day,
    mktg_fathers_day,
    case
      when easter = 1 or christmas = 1 or thanksgiving = 1
        or mothers_day = 1 or fathers_day = 1 or valentines_day = 1
        then 1
      else 0
    end as holiday,
    case
      when anniversary_early_access = 1 or anniversary_l4_preshop = 1
        or march_triple_rewards = 1 or may_triple_rewards = 1 or september_triple_rewards = 1
        then 1
      else 0
    end as loyalty,
    case
      when anniversary_early_access = 1 or anniversary_l4_preshop = 1
        or anniversary_public_event = 1 or black_friday_cyber_monday = 1
        or december_half_yearly = 1 or fall_clearance = 1 or june_summer_clearance = 1
        or labor_day_clearance = 1 or may_half_yearly = 1 or winter_clearance = 1
        then 1
      else 0
    end as sale,
    case
      when (
        easter = 1 or christmas = 1 or thanksgiving = 1
          or mothers_day = 1 or fathers_day = 1 or valentines_day = 1
      ) and (
        anniversary_early_access = 1 or anniversary_l4_preshop = 1
          or march_triple_rewards = 1 or may_triple_rewards = 1 or september_triple_rewards = 1
      )
        then 1
      else 0
    end as holiday_loyalty,
    case
      when (
        easter = 1 or christmas = 1 or thanksgiving = 1
          or mothers_day = 1 or fathers_day = 1 or valentines_day = 1
      ) and (
        anniversary_early_access = 1 or anniversary_l4_preshop = 1
          or anniversary_public_event = 1 or black_friday_cyber_monday = 1
          or december_half_yearly = 1 or fall_clearance = 1 or june_summer_clearance = 1
          or labor_day_clearance = 1 or may_half_yearly = 1 or winter_clearance = 1
      )
        then 1
      else 0
    end as holiday_sale,
    case
      when (
        anniversary_early_access = 1 or anniversary_l4_preshop = 1
          or march_triple_rewards = 1 or may_triple_rewards = 1 or september_triple_rewards = 1
      ) and (
        anniversary_early_access = 1 or anniversary_l4_preshop = 1
          or anniversary_public_event = 1 or black_friday_cyber_monday = 1
          or december_half_yearly = 1 or fall_clearance = 1 or june_summer_clearance = 1
          or labor_day_clearance = 1 or may_half_yearly = 1 or winter_clearance = 1
      )
        then 1
      else 0
    end as loyalty_sale,
    case
      when (
        easter = 1 or christmas = 1 or thanksgiving = 1
          or mothers_day = 1 or fathers_day = 1 or valentines_day = 1
      ) and (
        anniversary_early_access = 1 or anniversary_l4_preshop = 1
          or march_triple_rewards = 1 or may_triple_rewards = 1 or september_triple_rewards = 1
      ) and (
        anniversary_early_access = 1 or anniversary_l4_preshop = 1
          or anniversary_public_event = 1 or black_friday_cyber_monday = 1
          or december_half_yearly = 1 or fall_clearance = 1 or june_summer_clearance = 1
          or labor_day_clearance = 1 or may_half_yearly = 1 or winter_clearance = 1
      )
        then 1
      else 0
    end as holiday_loyalty_sale,
    sysdate as data_last_updated

  from temp_event_lookup
);

analyze cust_usr.c01f_event_lookup;
