import pickle
from configparser import ConfigParser
import os

# Syntax in .py
curr_path = os.path.dirname(os.path.realpath(__file__))
# Syntax in Jupyter Notebook
#curr_path = os.getcwd()

# Read config.ini
ini_file = 'config.ini'
parser = ConfigParser()
parser.read(os.path.join(curr_path, ini_file))

credential_file = os.path.join(curr_path, parser['Algo']['credential'])

# Read credential from pickle; r for read only, b for binary mode
with open(credential_file, 'rb') as f:
    credentials = pickle.load(f)

# Create db_config
db_config = {
    'host': parser['Algo']['host'],
    'port': parser['Algo']['port'],
    'database': parser['Algo']['database'],
    'user': credentials['db']['user'],
    'password': credentials['db']['pwd']}

# Create finnhub_config
finnhub_config = credentials['finnhub']

# Create twilio_config
twilio_config = credentials['twilio']

# Create email_config
email_config = credentials['email']