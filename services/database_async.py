
from services.logg import get_logger
import requests
import aiohttp
import time
import asyncpg

from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy_utils import (
    database_exists, create_database
)
from decouple import config

from services.tables import meta
from services.logg import logger

url = 'https://api.novaposhta.ua/v2.0/json/'
engine = create_async_engine('postgresql+asyncpg://kleikoks:kleikoks@localhost:5432/test', echo=True, future=True)
api_key = config('NP_API_KEY', None)
loger = get_logger()


def handle_db():
    engine = create_engine('postgresql://kleikoks:kleikoks@localhost:5432/test', echo=True, future=True)
    meta.drop_all(engine)
    meta.create_all(engine)


async def handle_execution(stmt):
    async with engine.begin() as connection:
        try:
            await connection.execute(stmt)
        except Exception as e:
            logger.info(e)


async def get_full_response(model:str, method: str, properties:dict = None):
    result = {'data': []}
    if not properties:
        properties = {"Limit": 100000}
    data = {
        "apiKey": api_key,
        "modelName": model,
        "calledMethod": method,
        'methodProperties': properties
    }
    data['methodProperties']['Page'] = 1
    while True:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=data) as response:
                response = await response.json()
                for obj in response['data']:
                    result['data'].append(obj)
                if not response['data']:
                    break
                data['methodProperties']['Page'] += 1
    return result


async def get_response(model:str, method:str, properties:dict = {}, url:str = url):
    data = {
        "apiKey": api_key,
        "modelName": model,
        "calledMethod": method,
        'methodProperties': properties
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=data) as response:
            return await response.json()


def time_decorator(func):
    async def wrapper():
        loger.info(f'{func.__name__:<25} started')
        start = time.perf_counter()
        await func()
        end = time.perf_counter()
        loger.info(f'{func.__name__:<25} : {round(end - start, 2)}s')
    return wrapper
