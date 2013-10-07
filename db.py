from sqlalchemy.dialects.mysql import VARCHAR, TINYINT, DATE, BIGINT
from sqlalchemy import Column, Integer
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime


Base = declarative_base()


def client_init(self, **kwargs):
    for k in kwargs:
        if isinstance(getattr(self.__class__, k).type, DATE):
            try:
                kwargs[k] = datetime.strptime(kwargs[k], "%d.%m.%Y")
            except ValueError:
                kwargs[k] = None
        setattr(self, k, kwargs[k])


def create_client_class(engine, tablename):
    if tablename in Base.metadata.tables:
        return Base.metadata.tables[tablename]

    Client = type('Client', (Base, ), {
        'patient_id': Column(Integer),
        'lastname': Column(VARCHAR(30)),
        'firstName': Column(VARCHAR(30)),
        'midname': Column(VARCHAR(30)),
        'sex': Column(TINYINT(4)),
        'birthdate': Column(DATE),
        'doc_series': Column(VARCHAR(8)),
        'doc_number': Column(VARCHAR(16)),
        'doc_code': Column(Integer),
        'policy_series': Column(VARCHAR(16)),
        'policy_number': Column(VARCHAR(16)),
        'policy_doctype': Column(Integer),
        'insurance_orgcode': Column(VARCHAR(12)),
        'LPU': Column(VARCHAR(12)),
        'reg_date': Column(DATE),
        'UPN': Column(BIGINT, primary_key=True),
        '__tablename__': tablename,
        '__init__': client_init,
    })
    Base.metadata.create_all(engine, checkfirst=True)
    return Client
