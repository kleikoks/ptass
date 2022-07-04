import asyncio

from sqlalchemy import select

from services.logg import get_logger
from services.database import (
    a_get_response, get_full_response, engine,
    handle_db, handle_execution, time_decorator
)
from services.tables import (
    settlement_type, warehouse_type,
    area,  warehouse, address, settlement
)

logger = get_logger()


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


if __name__ == '__main__':
    handle_settlement()
