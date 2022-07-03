from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey


meta = MetaData()


settlement_type = Table(
    'settlement_type', meta,
    Column('ref', String, primary_key = True),
    Column('title', String),
    Column('short_desc', String)
)

warehouse_type = Table(
    'warehouse_type', meta,
    Column('title', String),
    Column('ref', String, primary_key = True)
)

area = Table(
    'area', meta,
    Column('ref', String, primary_key = True),
    Column('title', String)
)

settlement = Table(
    'settlement', meta,
    Column('ref', String, primary_key = True),
    Column('title', String),
    Column('type', String, ForeignKey('settlement_type.ref')),
    Column('area', String, ForeignKey('area.ref'))
)

warehouse = Table(
    'warehouse', meta,
    Column('ref', String, primary_key = True),
    Column('title', String),
    Column('type', String, ForeignKey('warehouse_type.ref')),
    Column('settlement', String, ForeignKey('settlement.ref')),
    Column('short_address', String)
)

address = Table(
    'address', meta,
    Column('ref', String, primary_key = True),
    Column('title', String),
    Column('settlement', String, ForeignKey('settlement.ref')),
)