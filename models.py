# -*- coding: utf-8 -*-
from datetime import datetime, date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, BigInteger, Unicode, SmallInteger, Date


Base = declarative_base()


class Clients(Base):
    __tablename__ = 'clients'
    __table_args__ = {'mysql_engine': 'InnoDB'}

    UPN = Column(BigInteger, primary_key=True)
    patient_id = Column(Integer)
    lastname = Column(Unicode(30))
    firstName = Column(Unicode(30))
    midname = Column(Unicode(30))
    sex = Column(SmallInteger(4))
    birthdate = Column(Date)
    doc_series = Column(Unicode(8))
    doc_number = Column(Unicode(16))
    doc_code = Column(Integer)
    policy_series = Column(Unicode(16))
    policy_number = Column(Unicode(16))
    policy_doctype = Column(Integer)
    insurance_orgcode = Column(Unicode(12))
    LPU = Column(Unicode(12))
    reg_date = Column(Date)

    def __init__(self, **kwargs):
        for k in kwargs:
            if isinstance(getattr(self.__class__, k).type, Date) and not isinstance(kwargs[k], date):
                try:
                    kwargs[k] = datetime.strptime(kwargs[k], "%d.%m.%Y")
                except ValueError:
                    kwargs[k] = None
            setattr(self, k, kwargs[k])