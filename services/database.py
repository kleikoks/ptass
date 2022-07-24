
import requests
import time

from sqlalchemy import create_engine
from sqlalchemy_utils import (
    database_exists, create_database
)
from decouple import config


from services.tables import meta
from services.logg import get_logger

db_file = 'db.sqlite3'
url = 'https://api.novaposhta.ua/v2.0/json/'
engine = create_engine('sqlite:///db.sqlite3', echo=True, future=True)
api_key = config('NP_API_KEY', None)
logger = get_logger()

def handle_db():
    if not database_exists(engine.url):
        create_database(engine.url)
    # ? meta.drop_all(engine) неробоче гівно
        meta.create_all(engine)


def handle_execution(stmt):
    with engine.begin() as connection:
        try:
            connection.execute(stmt)
        except Exception as e:
            logger.info(e)


def get_full_response(model:str, method: str, properties:dict = None):
    result = {'data': []}
    if not properties:
        properties = {}
    data = {
        "apiKey": api_key,
        "modelName": model,
        "calledMethod": method,
        'methodProperties': properties
    }
    data['methodProperties']['Page'] = 1
    while True:
        response = requests.post(url, json=data).json()
        for obj in response['data']:
            result['data'].append(obj)
        if not response['data']:
            break
        data['methodProperties']['Page'] += 1
    return result


def get_response(model:str, method: str, properties:dict = {}, url: str = url):
    data = {
        "apiKey": api_key,
        "modelName": model,
        "calledMethod": method,
        'methodProperties': properties
    }
    response = requests.post(url, json=data).json()
    return response


def time_decorator(func):
    def wrapper():
        logger.info(f'{func.__name__:<25} started')
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        logger.info(f'{func.__name__:<25} : {round(end - start, 2)}s')
    return wrapper
