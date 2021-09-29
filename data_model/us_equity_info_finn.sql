DROP TABLE IF EXISTS us_equity_info_finn;

CREATE TABLE IF NOT EXISTS us_equity_info_finn(
	id serial NOT NULL,
	currency varchar(4) NOT NULL,
	description varchar NOT NULL,
	displaySymbol varchar(10) NOT NULL,
	figi varchar(12) NOT NULL,
	mic varchar(4) NOT NULL,
	symbol varchar(7) NOT NULL PRIMARY KEY,
	type varchar NOT NULL
);
	
