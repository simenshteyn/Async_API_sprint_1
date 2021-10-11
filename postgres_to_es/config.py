import os
import logging

from dotenv import load_dotenv

load_dotenv()

# Logging settings
logging.basicConfig(filename='etl.log', level=os.getenv('LOGGING_LEVEL'))
logger = logging.getLogger()
logger.setLevel(level=os.getenv('LOGGING_LEVEL'))

dsl = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.environ.get('POSTGRES_HOST'),
    'port': os.environ.get('POSTGRES_PORT'),
}

es_conf = [{
    'host': os.getenv('ELASTIC_HOST'),
    'port': os.getenv('ELASTIC_PORT'),
}]
