# Question # 1
# get avg streak days and avg streak pct change
with rate_increased as (
	select 
		currency_symbol,
		rate_date,
		exchange_rate,
		lag(exchange_rate, 1) over (partition by currency_symbol order by rate_date) as previous_rate,
		exchange_rate > lag(exchange_rate, 1) over (partition by currency_symbol order by rate_date) as increased
	from exchange_rates 
	order by currency_symbol, rate_date desc
),
is_streak_day as (
	select 
		currency_symbol,
		rate_date,
		exchange_rate,
		case
			when increased and ((lag(increased, 1) over (partition by currency_symbol order by rate_date) is true
				and lag(increased, 2) over (partition by currency_symbol order by rate_date) is true)
				or (lead(increased, 1) over (partition by currency_symbol order by rate_date) is true
				and lead(increased, 2) over (partition by currency_symbol order by rate_date) is true)
				or (lag(increased, 1) over (partition by currency_symbol order by rate_date) is true
				and lead(increased, 1) over (partition by currency_symbol order by rate_date) is true))
			then true
			else false 
		end as streak_part,
		increased and not lag(increased, 1) over (partition by currency_symbol order by rate_date) is true as start_increase
	from rate_increased 
	group by currency_symbol, exchange_rate, rate_date, increased 
	order by currency_symbol, rate_date desc
),
streak_groups AS (
    select
        currency_symbol,
        rate_date,
        exchange_rate,
        streak_part,
        start_increase,
        SUM(case when streak_part and not start_increase then 0 else 1 end) over (partition by currency_symbol order by rate_date asc) as streak_group_id
    from
        is_streak_day
    group by currency_symbol, exchange_rate, rate_date, streak_part, start_increase 
    order by currency_symbol, rate_date
),
streak_counts as (
	select 
		currency_symbol, 
		streak_group_id, 
		count(*) as streak_count
	from streak_groups 
	group by currency_symbol, streak_group_id 
	having count(*) > 1 
	order by currency_symbol
),
streak_pct_change as (
	select 
		currency_symbol, 
		((max(exchange_rate) - min(exchange_rate))/min(exchange_rate)) * 100 as pct_chg, 
		streak_group_id 
	from streak_groups 
	group by currency_symbol, streak_group_id 
	having count(*) > 1 
	order by currency_symbol, streak_group_id
),
avg_pct_chg as (
	select currency_symbol, round(sum(pct_chg)/count(*),4) as avg_cons_perc_change 
	from streak_pct_change 
	group by currency_symbol
),
avg_streak_days as (
	select currency_symbol, round(sum(streak_count)/count(*),2) as avg_cons_pos_days 
	from streak_counts 
	group by currency_symbol
)
(select a.currency_symbol, 
	b.avg_cons_pos_days, 
	a.avg_cons_perc_change, 
	rank() over (order by b.avg_cons_pos_days desc) as avg_cons_pos_days_rank, 
	rank() over (order by a.avg_cons_perc_change desc) as avg_cons_perc_change_rank 
from avg_pct_chg a
left join avg_streak_days b on a.currency_symbol = b.currency_symbol
order by avg_cons_pos_days_rank asc limit 5)
union all
(select a.currency_symbol, 
	b.avg_cons_pos_days, 
	a.avg_cons_perc_change, 
	rank() over (order by b.avg_cons_pos_days desc) as avg_cons_pos_days_rank, 
	rank() over (order by a.avg_cons_perc_change desc) as avg_cons_perc_change_rank 
from avg_pct_chg a
left join avg_streak_days b on a.currency_symbol = b.currency_symbol
order by avg_cons_perc_change_rank asc limit 5)


# Question # 2
# Custom metrics - max_monthly_variance and high_months_rank
# get the months/currency with the highest single month variance,
# and get the currencies with the most number of months with > 10% variance
with variance_by_month as (
	select 
		currency_symbol, 
		((max(exchange_rate) - min(exchange_rate))/min(exchange_rate)) * 100 as pct_variance, 
		to_char(rate_date, 'YYYY-MM') as rate_month
	from exchange_rates 
	group by currency_symbol, rate_month order by currency_symbol
),
high_variance_months as (
	select 
		currency_symbol, 
		count(*) month_count 
	from variance_by_month 
	where pct_variance > 10 
	group by 1
),
highest_monthly_variance_rank as (
	select currency_symbol, max(pct_variance) as max_monthly_variance, rank() over (order by max(pct_variance) desc) from variance_by_month group by currency_symbol
),
high_variance_month_rank as (
	select currency_symbol, count(*) high_variance_months, rank() over (order by count(*) desc) from variance_by_month where pct_variance > 10 group by 1
)
(select a.currency_symbol, a.max_monthly_variance, a.rank as max_variance_rank, b.high_variance_months, b.rank as high_months_rank
from highest_monthly_variance_rank a
left join high_variance_month_rank b
on a.currency_symbol = b.currency_symbol
order by a.rank asc limit 5)
union all
(select a.currency_symbol, a.max_monthly_variance, a.rank as max_variance_rank, b.high_variance_months, b.rank as high_months_rank
from highest_monthly_variance_rank a
left join high_variance_month_rank b
on a.currency_symbol = b.currency_symbol
order by b.rank asc limit 5)


## Bonus question sql
with rate_increased as (
	select 
		currency_symbol,
		rate_date,
		exchange_rate,
		lag(exchange_rate, 1) over (partition by currency_symbol order by rate_date) as previous_rate,
		exchange_rate > lag(exchange_rate, 1) over (partition by currency_symbol order by rate_date) as increased
	from exchange_rates 
	order by currency_symbol, rate_date desc
),
is_streak_day as (
	select 
		currency_symbol,
		rate_date,
		exchange_rate,
		case
			when increased and ((lag(increased, 1) over (partition by currency_symbol order by rate_date) is true
				and lag(increased, 2) over (partition by currency_symbol order by rate_date) is true)
				or (lead(increased, 1) over (partition by currency_symbol order by rate_date) is true
				and lead(increased, 2) over (partition by currency_symbol order by rate_date) is true)
				or (lag(increased, 1) over (partition by currency_symbol order by rate_date) is true
				and lead(increased, 1) over (partition by currency_symbol order by rate_date) is true))
			then true
			else false 
		end as streak_part,
		increased and not lag(increased, 1) over (partition by currency_symbol order by rate_date) is true as start_increase
	from rate_increased 
	group by currency_symbol, exchange_rate, rate_date, increased 
	order by currency_symbol, rate_date desc
),
streak_groups AS (
    select
        currency_symbol,
        rate_date,
        exchange_rate,
        streak_part,
        start_increase,
        SUM(case when streak_part and not start_increase then 0 else 1 end) over (partition by currency_symbol order by rate_date asc) as streak_group_id
    from
        is_streak_day
    group by currency_symbol, exchange_rate, rate_date, streak_part, start_increase 
    order by currency_symbol, rate_date
),
streak_groups_yesterday AS (
    select
        currency_symbol,
        rate_date,
        exchange_rate,
        streak_part,
        start_increase,
        SUM(case when streak_part and not start_increase then 0 else 1 end) over (partition by currency_symbol order by rate_date asc) as streak_group_id
    from
        is_streak_day
    where rate_date < CURRENT_DATE
    group by currency_symbol, exchange_rate, rate_date, streak_part, start_increase 
    order by currency_symbol, rate_date
),
streak_pct_change as (
	select 
		currency_symbol, 
		((max(exchange_rate) - min(exchange_rate))/min(exchange_rate)) * 100 as pct_chg, 
		streak_group_id 
	from streak_groups 
	group by currency_symbol, streak_group_id 
	having count(*) > 1 
	order by currency_symbol, streak_group_id
),
streak_pct_change_yesterday as (
	select 
		currency_symbol, 
		((max(exchange_rate) - min(exchange_rate))/min(exchange_rate)) * 100 as pct_chg, 
		streak_group_id 
	from streak_groups_yesterday 
	group by currency_symbol, streak_group_id 
	having count(*) > 1 
	order by currency_symbol, streak_group_id
),
avg_pct_chg as (
	select currency_symbol, round(sum(pct_chg)/count(*),4) as avg_cons_perc_change 
	from streak_pct_change 
	group by currency_symbol
),
avg_pct_chg_yesterday as (
	select currency_symbol, round(sum(pct_chg)/count(*),4) as avg_cons_perc_change 
	from streak_pct_change_yesterday 
	group by currency_symbol
)
select a.currency_symbol,  
	a.avg_cons_perc_change, 
	rank() over (order by a.avg_cons_perc_change desc) as avg_cons_perc_change_rank,
	rank() over (order by b.avg_cons_perc_change desc) as avg_cons_perc_change_rank_yesterday
from avg_pct_chg a
left join avg_pct_chg_yesterday b
on a.currency_symbol = b.currency_symbol
order by avg_cons_perc_change_rank asc limit 10