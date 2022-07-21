from sqlalchemy import select

from services.logg import get_logger
from services.database import (
    get_response, get_full_response, time_decorator, 
    handle_execution, handle_db, engine
)
from services.tables import (
    settlement_type, warehouse_type, area, settlement, warehouse, address
)

logger = get_logger()


def create_settlement_type(objects):
    stmt = settlement_type.insert().values(
        [
        {
            'ref': obj.get('Ref'),
            'title': obj.get('Description'),
            'short_desc': obj.get('Code')
        } for obj in objects
        ]
    )
    handle_execution(stmt)


@time_decorator
def handle_settlement_type():
    response = get_response('Address', 'getSettlementTypes')
    create_settlement_type(response['data'])


def create_warehouse_type(objects):
    stmt = warehouse_type.insert().values(
        [
        {
            'ref': obj.get('Ref'),
            'title': obj.get('Description')
        } for obj in objects
        ]
    )
    handle_execution(stmt)


@time_decorator
def handle_warehouse_type():
    response = get_response('Address', 'getWarehouseTypes')
    create_warehouse_type(response['data'])


def create_area(objects):
    stmt = area.insert().values(
        [
        {
            'ref': obj.get('Ref'),
            'title': obj.get('Description')
        } for obj in objects
        ]
    )
    handle_execution(stmt)


@time_decorator
def handle_area():
    response = get_response('Address', 'getAreas')
    create_area(response['data'])


def create_settlement(objects):
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
    handle_execution(stmt)


@time_decorator
def handle_settlement():
    response = get_full_response('Address', 'getCities')
    create_settlement(response['data'])


def get_settlement_ids():
        stmt = select(settlement.c.ref)
        settlement_ids = []
        with engine.connect() as connection:
            try:
                result = connection.execute(stmt)
                settlement_ids = [obj[0] for obj in result]
            except Exception as e:
                logger.info(e)
        return settlement_ids


def create_warehouse(settlement_id):
    properties = {
        'CityRef': settlement_id
    }
    data = get_response('Address', 'getWarehouses', properties)['data']
    if data:
        stmt = warehouse.insert().values(
            [
            {
                'ref': obj.get('Ref'),
                'title': obj.get('Description'),
                'short_address': obj['ShortAddress'],
                'type': obj.get('TypeOfWarehouse'),
                'settlement': settlement_id
            } for obj in data
            ]
        )
        handle_execution(stmt)


@time_decorator
def handle_warehouse():
    settlement_ids = get_settlement_ids()
    [
        create_warehouse(settlement_id) for settlement_id in settlement_ids
    ]


def create_address(settlement_id):
    properties = {
        'CityRef': settlement_id
    }
    data = get_response('Address', 'getStreet', properties)['data']
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
        handle_execution(stmt)


@time_decorator
def handle_address():
    settlement_ids = get_settlement_ids()
    [
        create_address(settlement_id) for settlement_id in settlement_ids
    ]   


@time_decorator
def main():
    get_logger().info('ThreadPoolExecutor started')
    handle_db()
    handle_settlement_type()
    handle_warehouse_type()
    handle_area()
    handle_settlement()
    handle_warehouse()
    handle_address()


if __name__ == '__main__':
    main()
