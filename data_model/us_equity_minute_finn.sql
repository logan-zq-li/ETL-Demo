DROP TABLE IF EXISTS us_equity_minute_finn;

CREATE TABLE IF NOT EXISTS us_equity_minute_finn(
	id serial NOT NULL PRIMARY KEY,
    close_price     numeric(9,3) NULL,
    high_price      numeric(9,3) NULL,
    low_price       numeric(9,3) NULL,
    open_price      numeric(9,3) NULL,
    time_stamp_unix bigint NOT NULL,
    volume          integer NULL,
    dt_nyc          timestamp NOT NULL,
    date_nyc        date NOT NULL,
    date_utc        date NOT NULL,
    symbol          varchar(7) NOT NULL,
    created_at      timestamp NULL DEFAULT NOW(),
    CONSTRAINT unique_minute_data UNIQUE (symbol, dt_nyc)
);
