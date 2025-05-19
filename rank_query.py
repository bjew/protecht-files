rank_query = '''
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
	order by avg_cons_perc_change_rank asc limit 10'''