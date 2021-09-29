# ETL demo for CPP

## Introduction
The goal is to fetch minute prices for about 1000 US stocks through Finnhub API
and load data into online cloud database AWS RDS. The ETL job is
scheduled to run automatically on AWS EC2 through Ubuntu crontab. There are dual channels for ETL
job notifications or alerts: A message will be sent to my phone via twilio and an email will be sent
to my gmail mailbox. An error log will also record all notifications and errors.



## Steps
### 1. Set up SQL tables

Folder data_model includes sql commands to create tables.
Run **tables.sql** in psql to create the below two tables:
+ us_equity_info_finn -> basic info of selected 1000 stocks
+ us_equity_minute_finn -> stock minute prices

### 2. Configuration

**config.ini** includes connection info for RDS PostgreSQL database. Credentials for the
database, Finnhub API, Twilio, and email are saved in a pickle file (**Algo.pkl** was not uploaded
to Github). Database connection parameters and credentials are imported from **config.py** in
folder utils

The error log, DS_Algo_ETL.log, is imported from **my_logging.py** in folder utils

### 3. Main functions and stock data

The symbols of the 1000 US stocks are saved in us_equity_info_finn.csv
in folder data_input.

The main function **etl_minute.py** will check database records, 
get the latest date of loaded prices, calculate the next day after the latest date, fetch 
stock prices from finnhub from the next day to today, 
and load data into AWS PostgreSQL database. Finnhub free tier has a limit of 60 API calls/minute,
so the function makes sure that the interval between 2 API calls is at least a second.

### 4. Scheduling

The shell script **ETL.sh** is written into crontab to set up a cron job on EC2: Run etl_minute.py 
at 9pm on every Wednesday.

## Reference
+ Finnhub API documentation: https://finnhub.io/docs/api/stock-candles
+ AWS RDS: https://aws.amazon.com/rds/
+ AWS EC2: https://aws.amazon.com/ec2/
+ Twilio: https://www.twilio.com/

