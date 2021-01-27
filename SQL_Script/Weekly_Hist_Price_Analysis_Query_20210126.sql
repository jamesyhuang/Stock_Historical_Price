-- **********************************************************************************************
-- Parameters:
--		tickers (temp table) store all tickers to display in the query. 1 or many
--			sample ticker: tsm, tsla, aapl, amzn, nflx, nvda, fb, baba, dji, nasdaq, nio, qqq, spy, arkk, eem, gld, snow, sq
-- 		run_ticker: pick 1 (not used anymore) 
--		begDate: beginning range of trade date
--		endDate: ending range of trade date (default current_date)	
--***********************************************************************************************
DROP TABLE IF EXISTS tickers;
CREATE TEMP TABLE tickers(ticker varchar(10));
insert into tickers values ('tsm') , ('tsla'), ('aapl'), ('amzn'), ('nflx'), ('nvda'), ('fb'), ('baba'), ('dji'), ('nasdaq'), ('nio'), ('qqq'), ('spy'),
							('arkk'), ('eem'), ('gld'), ('snow'), ('sq'), ('^dji'), ('^ixic'), ('crm'),('amd'), ('msft'), ('uber'), ('zm'), ('slv'), ('arkg'),
						   ('xle'), ('xlf'), ('xlv'), ('xlk'), ('lmnd'), ('mrna'), ('bynd'), ('pltr'),('bidu'),('v'),('abnb'),('li'), ('ba'), ('iwm'), ('dash');
--select * from tickers;

--- constants or variables
WITH myconstants (run_ticker, begDate, endDate) as (values ('ticker not used here', '2020-06-01', CURRENT_DATE)) 	 --'2017-01-01', CURRENT_DATE)) 	
,
--- stock price table download data fields
MainStockPrice as
(
select ticker, tradedate, openprice::numeric(9,2) , high::numeric(9,2), low::numeric(9,2), 
       closeprice::numeric(9,2) , adjclose::numeric(9,2), volume, trim(to_char(tradedate, 'Day')) as Dayofweek,
       DATE_PART('week',tradedate) AS  week_number
  from stockprice, myconstants
 where (trim(to_char(tradedate, 'Day')) = 'Friday' or 
 			tradedate in (select (holiday - 1) as trade_date 
                            from market_holiday 
                           where trim(to_char(holiday, 'Day')) = 'Friday'
                         )
        )
   and ticker in (select * from tickers) -- ('aapl', 'tsm') -- run_ticker 
   and tradedate between begDate::date and endDate::date
)  

--select * from MainStockPrice
,
-------------------------------------------------------------------
--- beginning of week get open price
BegWeekStockPrice as
(
select ticker, tradedate, openprice::numeric(9,2) , high::numeric(9,2), low::numeric(9,2), 
       closeprice::numeric(9,2) , adjclose::numeric(9,2), volume, trim(to_char(tradedate, 'Day')) as Dayofweek,
       DATE_PART('week',tradedate) AS  week_number
  from stockprice, myconstants
 where (trim(to_char(tradedate, 'Day')) = 'Monday' or 
 			tradedate in (select (holiday + 1) as trade_date 
                            from market_holiday 
                           where trim(to_char(holiday, 'Day')) = 'Monday'
                         )
        )
   and ticker in (select * from tickers) --('aapl', 'tsm') --run_ticker --'aapl' 
   and tradedate between begDate::date and endDate::date
) 
,
-----------------------------------------------------------------
--- Compare with prior week close price, get delta, pct change, set up (+1) or down (-1) mark
steak_stock_hist as 
(
select 	main.ticker, 
		main.tradedate, 
		beginweek.openprice as beg_openprice, 
		main.openprice, 
		main.closeprice, 
		main.week_number::text ||'-'|| main.dayofweek as week_num ,
		lag(main.closeprice,1) OVER (partition by main.ticker order by main.ticker, main.tradedate) as PriorPrice,
		main.closeprice - lag(main.closeprice,1) OVER (partition by main.ticker order by main.ticker, main.tradedate) as Delta,
		((main.closeprice - lag(main.closeprice,1) OVER (partition by main.ticker order by main.ticker, main.tradedate)) / (lag(main.closeprice,1) OVER (partition by main.ticker order by main.ticker, main.tradedate))*100)::numeric(10,2)::text ||'%' as Pct_change,
		case when lag(main.closeprice,1) OVER (partition by main.ticker order by main.ticker, main.tradedate) is null then 0
		     when ((main.closeprice - lag(main.closeprice,1) OVER (partition by main.ticker order by main.ticker, main.tradedate)) / (lag(main.closeprice,1) OVER (partition by main.ticker order by main.ticker, main.tradedate))) < -0.01 then -1
		     else 1
		end as mark
		
  from MainStockPrice as main
  		left join BegWeekStockPrice as beginWeek
 				on 	main.ticker = beginWeek.ticker and 
 					main.week_number = beginWeek.week_number and 
 					date_part('year', main.tradedate) = date_part('year', beginWeek.tradedate)
)				
,
steak_stock_Mark as 
-- Find win and loss streak
(
select 	ticker,
		tradedate,
		week_num,
		beg_openprice,
		openprice as last_openprice,
		closeprice,
		priorprice,
		delta,
		pct_change,
		mark,
		lag (mark, 1) over (partition by ticker, date_part('year', tradedate) order by ticker, tradedate) as prior_mark,
		sum(mark) over (partition by ticker, date_part('year', tradedate) order by ticker, tradedate rows between unbounded preceding and current row) as Win_Total --streak
		--dense_rank() over (partition by ticker, date_part('year', tradedate), mark order by ticker, tradedate, mark) as in_streak
		--sum(mark) over (partition by ticker, date_part('year', tradedate), mark order by ticker, tradedate rows between unbounded preceding and current row) as streak
		from steak_stock_hist
	)
,
-- streak_stock_NewGroup
streak_stock_NewGroup as	
(
	select 	*,
			case	when prior_mark <> mark or prior_mark is null then 1
                	else 0
            end as is_new_group
	from steak_stock_Mark
)
,
-- streak_stock_NewGroup
streak_stock_steak as
(
select 	*,
       	sum(is_new_group) over (partition by ticker, date_part('year', tradedate) order by ticker, tradedate rows between unbounded preceding and current row) as streak_group --streak
from streak_stock_NewGroup
)

select 	--*,
		ticker,
		tradedate,
		week_num,
		beg_openprice,
		last_openprice,
		closeprice,
		priorprice,
		delta,
		pct_change,
		mark,
		Win_Total,
		--row_number() over (partition by player,streak_group order by dt asc, streak_group asc) - 1 as expected_unsigned
		row_number() over (partition by ticker, date_part('year', tradedate), streak_group order by ticker, tradedate ) *
			(case when mark >= 0 then 1 else -1 end)
		as streak --expected_unsigned
  from streak_stock_steak
--- Final query with display fields and calc running total of win and loss per each year