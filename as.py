import asyncio

from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from services.logg import get_logger
from services.database_async import (
    get_response, get_full_response,
    handle_db, handle_execution, time_decorator
)
from services.tables import (
    settlement_type, warehouse_type,
    area,  warehouse, address, settlement
)

logger = get_logger()


async def create_settlement_type(objects):
    stmt = settlement_type.insert().values(
        [
        {
            'ref': obj.get('Ref'),
            'title': obj.get('Description'),
            'short_desc': obj.get('Code')
        } for obj in objects
        ]
    )
    await handle_execution(stmt)


@time_decorator
async def handle_settlement_type():
    response = await get_response('Address', 'getSettlementTypes')
    await create_settlement_type(response['data'])


async def create_warehouse_type(objects):
    stmt = warehouse_type.insert().values(
        [
        {
            'ref': obj.get('Ref'),
            'title': obj.get('Description')
        } for obj in objects
        ]
    )
    await handle_execution(stmt)


@time_decorator
async def handle_warehouse_type():
    response = await get_response('Address', 'getWarehouseTypes')
    await create_warehouse_type(response['data'])


async def create_area(objects):
    stmt = area.insert().values(
        [
        {
            'ref': obj.get('Ref'),
            'title': obj.get('Description')
        } for obj in objects
        ]
    )
    await handle_execution(stmt)


@time_decorator
async def handle_area():
    response = await get_response('Address', 'getAreas')
    await create_area(response['data'])


async def create_settlement(objects):
    stmt = settlement.insert().values(
        [
        {
            'ref': obj.get('Ref'),
            'title': obj.get('Description'),
            'type': obj.get('SettlementType'),
            'area': obj.get('Area')
        } for obj in objects
        ]
    )
    await handle_execution(stmt)


@time_decorator
async def handle_settlement():
    response = await get_full_response('Address', 'getCities')
    await create_settlement(response['data'])


async def get_settlement_ids():
        stmt = select(settlement.c.ref)
        settlement_ids = []
        async with engine.connect() as connection:
            try:
                result = await connection.execute(stmt)
                settlement_ids = [obj[0] for obj in result]
            except Exception as e:
                logger.info(e)
        return settlement_ids


async def create_warehouse(settlement_id):
    properties = {
        'CityRef': settlement_id
    }
    response = await get_response('Address', 'getWarehouses', properties)
    data = response['data']
    if data:
        stmt = warehouse.insert().values(
            [
            {
                'ref': obj.get('Ref'),
                'title': obj.get('Description'),
                'short_address': obj['ShortAddress'],
                'type': obj.get('TypeOfWarehouse'),
                'settlement': settlement_id
            } for obj in data if data
            ]
        )
        await handle_execution(stmt)


@time_decorator
async def handle_warehouse():
    settlement_ids = await get_settlement_ids()
    coros = [
        create_warehouse(id) for id in settlement_ids
    ]
    await asyncio.gather(*coros)


async def create_address(settlement_id):
    properties = {
        'CityRef': settlement_id
    }
    response = await get_response('Address', 'getStreet', properties)
    data = response['data']
    if data:
        stmt = address.insert().values(
            [
            {
                'ref': obj.get('Ref'),
                'title': obj.get('Description'),
                'settlement': settlement_id
            } for obj in data 
            ]
        )
        await handle_execution(stmt)


@time_decorator
async def handle_address():
    settlement_ids = await get_settlement_ids()
    coros = [
        create_address(id) for id in settlement_ids
    ]
    await asyncio.gather(*coros)


@time_decorator
async def main():
    logger.info('async started')
    handle_db()
    await asyncio.gather(
        handle_settlement_type(),
        handle_warehouse_type(),
        handle_area(),
        handle_settlement()
    )
    # await asyncio.gather(
    #     handle_warehouse(),
    #     handle_address()
    # )
    logger.info('async ended')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
