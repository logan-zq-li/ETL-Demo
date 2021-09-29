import datetime as dt
import time
import pandas as pd
from io import StringIO
from contextlib import contextmanager
from email.message import EmailMessage

import finnhub
import psycopg2 as pg
import smtplib
from twilio.rest import Client

from utils.config import db_config
from utils.config import finnhub_config
from utils.config import twilio_config
from utils.config import email_config
from utils.my_logging import Mylogger

# Get twilio parameters
account_sid = twilio_config['sid']
auth_token = twilio_config['token']
twi_phone = twilio_config['twilio_phone']
my_phone = twilio_config['my_phone']
twi_client = Client(account_sid, auth_token)

# Get email parameters
to_email = email_config['to']
from_email = email_config['from']
password = email_config['pwd']


def send_email(subject, body):
    """ Define function to send email alert and notifications with smtp gmail server """
    msg = EmailMessage()
    msg.set_content(body)
    msg['subject'] = subject
    msg['to'] = to_email
    msg['from'] = from_email

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(from_email, password)
    server.send_message(msg)
    server.quit()


def get_finnhub_minute_prices(symbol: str, start_dt: int, end_dt: int):
    """ Get stock minute prices for a specific stock with specific time window, return DataFrame """
    try:
        finnhub_client = finnhub.Client(api_key=finnhub_config['api'])
        request = finnhub_client.stock_candles(symbol, '1', start_dt, end_dt)

        if request['s'] != 'ok':
            Mylogger.error(f"{symbol}'s minute prices failed to fetch from finnhub, data status: {request['s']}")
            # Send email alert from gmail
            send_email('DS Algo ETL Alert',
                       f"{symbol}'s minute prices failed to fetch from finnhub, data status: {request['s']}")
            # Send SMS alert from twilio phone number
            twi_client.messages.create(
                body=f"{symbol}'s minute prices failed to fetch from finnhub, data status: {request['s']}",
                from_=twi_phone,
                to=my_phone)
            # Function stop. Empty dataframe will be passed to the next function
            return pd.DataFrame(None)

        candles = pd.DataFrame(request)
        # Drop column status
        candles.drop(columns=['s'], inplace=True)
        # Round float numbers in volume column to int
        candles['v'] = candles['v'].round(0).astype(int)
        # Add column dt_nyc to convert unix datetime to readable utc datetime
        candles['dt_nyc'] = [dt.datetime.fromtimestamp(x) for x in candles['t']]
        # date_nyc makes sure any dt from 4am to 8pm falls on the same date. date_utc is the date used by finnhub API
        candles['date_nyc'] = [dt.date.fromtimestamp(x) for x in candles['t']]
        candles['date_utc'] = [dt.datetime.utcfromtimestamp(x).date() for x in candles['t']]
        # Rename columns
        candles.columns=['close_price', 'high_price','low_price','open_price','time_stamp_unix','volume',
                         'dt_nyc', 'date_nyc', 'date_utc']
        candles['symbol'] = symbol
        candles['created_at'] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return candles

    except finnhub.exceptions.FinnhubAPIException as error:
        Mylogger.error(f"{symbol}'s minute prices failed to fetch from finnhub because {error}")
        # Send email alert from gmail
        send_email('DS Algo ETL Alert',
                   f"{symbol}'s minute prices failed to fetch from finnhub because {error}")
        # Send SMS alert from twilio phone number
        twi_client.messages.create(
            body=f"{symbol}'s minute prices failed to fetch from finnhub because {error}",
            from_=twi_phone,
            to=my_phone)
        time.sleep(2)
        # Function stops. Empty dataframe will be passed to the next function
        return pd.DataFrame(None)


@contextmanager
def connection(host, port, database, user, password):
    """ Connect to RDS PostgreSQL database server """
    try:
        conn = pg.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database)
        yield conn

    except Exception as error:
        conn.rollback()
        Mylogger.error(f"RDS PostgreSQL database connection failed because {error}")
        # Send email alert from gmail
        send_email('DS Algo ETL Alert',
                   f"RDS PostgreSQL database connection failed because {error}")
        # Send SMS alert from twilio phone number
        twi_client.messages.create(
            body=f"RDS PostgreSQL database connection failed because {error}",
            from_=twi_phone,
            to=my_phone)
    else:
        conn.commit()
    finally:
        conn.close()


def get_latest_date(symbol: str, table: str):
    """Query the latest date for loaded prices"""
    with connection(**db_config) as conn:
        curr = conn.cursor()
        try:
            curr.execute(f"SELECT max(date_nyc) from {table} where symbol = '{symbol}'")
            latest_date = curr.fetchone()[0]
        except Exception as error:
            conn.rollback()
            Mylogger.error(f"Query the latest date for {symbol} from {table} failed because {error}")
            # Send email alert from gmail
            send_email('DS Algo ETL Alert',
                       f"Query the latest date for {symbol} from {table} failed because {error}")
            # Send SMS alert from twilio phone number
            twi_client.messages.create(
                body=f"Query the latest date for {symbol} from {table} failed because {error}",
                from_=twi_phone,
                to=my_phone)
            latest_date = None
        return latest_date


def get_next_day(latest_date):
    if latest_date is None:
        next_day = None
        return next_day
    else:
        try:
            # Get the next day's midnight time regardless latest_date ends at 20:00 or 16:30
            next_day = latest_date + dt.timedelta(days=1)
            # Add minutes to date so date can be converted into datetime
            next_day = dt.datetime.combine(next_day, dt.time())
            # Convert datetime to timestamp (unix time)
            next_day = int(next_day.timestamp())
        except Exception as error:
            Mylogger.error(f"Get next day failed because {error}")
            # Send email alert from gmail
            send_email('DS Algo ETL Alert',
                       f"Get next day failed because {error}")
            # Send SMS alert from twilio phone number
            twi_client.messages.create(
                body=f"Get next day failed because {error}",
                from_=twi_phone,
                to=my_phone)
            next_day = None
        return next_day


def load_into_min_tbl(df, table):
    if df is not None:
        temp_file = StringIO()
        # save dataframe to an in-memory buffer
        df.to_csv(temp_file, header=False, index=False)
        # connect to RDS and load data from temp file for each stock. Connection is closed after loading.
        with connection(**db_config) as conn:
            temp_file.seek(0)
            curr = conn.cursor()
            try:
                curr.copy_from(temp_file, table, sep=",",
                               columns=['close_price',
                                        'high_price',
                                        'low_price',
                                        'open_price',
                                        'time_stamp_unix',
                                        'volume',
                                        'dt_nyc',
                                        'date_nyc',
                                        'date_utc',
                                        'symbol',
                                        'created_at']
                              )
            # SQL CONSTRAINT in db will avoid loading duplicate prices
            except pg.errors.UniqueViolation as error:
                conn.rollback()
                Mylogger.error(f"Minute prices failed to load into DB because {error}")
                # Send email alert from gmail
                send_email('DS Algo ETL Alert',
                           f"Minute prices failed to load into DB because {error}")
                # Send SMS alert from twilio phone number
                twi_client.messages.create(
                    body=f"Minute prices failed to load into DB because {error}",
                    from_=twi_phone,
                    to=my_phone)

        temp_file.close()


def etl_minute():
    # Get symbols for selected stocks
    column = ['symbol']
    df_info = pd.read_csv('data_input/us_equity_info_finn.csv', header=0, usecols=column)
    symbols = df_info['symbol'].tolist()

    api_ts = time.time()
    rows_for_min = 0

    for symbol in symbols:
        latest_date = get_latest_date(symbol, 'us_equity_minute_finn')
        dt0 = get_next_day(latest_date)
        dt1 = int(dt.datetime.now().timestamp())
        if dt0 is not None:
            time_diff = time.time() - api_ts
            # Finnhub free tier has a limit of 60 API calls/minute
            if time_diff < 1:
                time.sleep(1 - time_diff)

            data_min = get_finnhub_minute_prices(symbol, dt0, dt1)
            api_ts = time.time()
            load_into_min_tbl(data_min, 'us_equity_minute_finn')
            rows_for_min += len(data_min)

    Mylogger.info(f"ETL job done. {rows_for_min} rows loaded into minute prices table.")
    # Send email notification from gmail
    send_email('DS Algo ETL Notification',
               f"ETL job done. {rows_for_min} rows loaded into minute prices table.")
    # Send SMS notification from twilio phone number
    twi_client.messages.create(
        body=f"ETL job done. {rows_for_min} rows loaded into minute prices table.",
        from_=twi_phone,
        to=my_phone)


if __name__ == "__main__":
    etl_minute()
