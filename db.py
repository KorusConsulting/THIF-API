# -*- coding: utf-8 -*-
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from models import Base
try:
    import json
except ImportError:
    import simplejson as json

config_file = os.path.join(os.path.dirname(__file__), 'config.json')
with open(config_file) as f:
    config = json.loads(f.read())
    connect = "mysql://%s:%s@localhost/%s?charset=utf8" % (config['username'],
                                                           config['password'],
                                                           config['db_name'])

engine = create_engine(connect, convert_unicode=True, pool_recycle=600)
db_session = scoped_session(sessionmaker(bind=engine))
import_session = scoped_session(sessionmaker(bind=engine))

#Base.query = db_session.query_property()


def init_db():
    # import all modules here that might define models so that
    # they will be registered properly on the metadata.  Otherwise
    # you will have to import them first before calling init_db()
    from models import Clients
    # Session.begin()
    Base.metadata.create_all(bind=engine)
    db_session.commit()


def remove_session():
    db_session.remove()
    import_session.remove()
