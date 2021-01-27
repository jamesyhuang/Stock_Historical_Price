--------------------------------
-- Holiday Table
-- public.market_holiday definition

-- Drop table

-- DROP TABLE public.market_holiday;

CREATE TABLE IF NOT EXISTS public.market_holiday (
	holiday date NOT NULL,
	"name" varchar(40) NULL
);

--------------------------------
-- Historical Stock price table
-- public.stockprice definition

-- Drop table

-- DROP TABLE public.stockprice;

CREATE TABLE IF NOT EXISTS public.stockprice (
	ticker varchar(10) NOT NULL,
	tradedate date NOT NULL,
	openprice float8 NULL,
	high float8 NULL,
	low float8 NULL,
	closeprice float8 NULL,
	adjclose float8 NULL,
	volume int8 NULL,
	CONSTRAINT pk_stockprice PRIMARY KEY (ticker, tradedate)
);