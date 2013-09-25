from argparse import ArgumentParser
from sqlalchemy import create_engine, Table, MetaData, Column, Integer
from sqlalchemy.dialects.mysql import VARCHAR, TINYINT, DATETIME
from sqlalchemy.orm import sessionmaker, mapper
import json
import glob
import os
import csv


CSV_FIELDS = ('id', 'enp', 'fam', 'IM', 'ot', 'w', 'dr',
              'docs', 'docn', 'DOCTP', 'spol', 'npol', 'opdoc',
              'q', 'LPU', 'lpudt')
CSV_DB_MAPPER = {
    'id': 'patient_id',
    'fam': 'lastname',
    'IM': 'firstName',
    'ot': 'midname',
    'w': 'sex',
    'dr': 'birthdate',
    'docs': 'doc_series',
    'docn': 'doc_number',
    'DOCTP': 'doc_code',
    'spol': 'policy_series',
    'npol': 'policy_number',
    'opdoc': 'policy_doctype',
    'q': 'insurance_orgcode',
    'LPU': 'LPU',
    'lpudt': 'reg_date',
    'enp': 'UPN',
}


class Client(object):
    """
    Desired table class to work with.
    Will be mapped later from the loaded table
    """
    def __init__(self,  **kwargs):
        for k in kwargs:
            print type(getattr(self, k))
        exit()


def create_client(row_lst):
    kwargs = dict((CSV_DB_MAPPER[CSV_FIELDS[idx]],
                   el) for idx, el in enumerate(row_lst))
    return Client(**kwargs)


def rreplace(s, old, new, occurrence):
    lst = s.rsplit(old, occurrence)
    return new.join(lst)


def bind(mf, mv, *args, **kwargs):
    """
    Service function. If there is any error - returns value and error
    Otherwise return the result of application mf function to value
    """
    value = mv[0]
    error = mv[1]

    if not error:
        try:
            return mf(value, *args, **kwargs)
        except Exception as e:
            return make_mv(e="%s: %s" % (mf.__name__, str(e)))
    return mv


def make_mv(v=None, e=None):
    """
    Creates monadic value from some value
    """
    return (v, e)


def read_config(path):
    with open(path) as f:
        config = json.loads(f.read())
        return make_mv(config)


def db_connect(config):
    connection = "mysql://%s:%s@localhost/%s" % (config['username'],
                                                 config['password'],
                                                 config['db_name'])
    engine = create_engine(connection)
    config['session'] = sessionmaker(bind=engine)()
    config['metadata'] = MetaData(engine)
    t = create_table(config)
    config['table'] = t
    return make_mv(config)


def find_files(context):
    all_csv_files = glob.glob(os.path.join(context['csv_path'],
                                           '*.csv'))
    context['csv_files'] = (c for c in all_csv_files
                            if not c.endswith('_loaded.csv'))
    return make_mv(context)


def load_files(context):
    session = context['session']
    for csv_path in context['csv_files']:
        with open(csv_path) as csv_file:
            reader = csv.reader(csv_file,
                                delimiter=str(context['delimiter']))
            fields = tuple(reader.next())
            if fields != CSV_FIELDS:
                raise ValueError("%s: wrong fields" % csv_path)

            for row in reader:
                c = create_client([x for x in row if x])
                session.add(c)
                session.commit()
        #os.rename(csv_path, rreplace(csv_path, '.csv', '_loaded.csv', 1))
    return make_mv(context)


def load_csv():
    config = bind(read_config, make_mv('config.json'))
    context = bind(db_connect, config)
    context = bind(find_files, context)
    context = bind(load_files, context)
    return context


def create_table(context):
    t = Table(context['table_name'], context['metadata'],
              Column('patient_id', Integer),
              Column('lastname', VARCHAR(30)),
              Column('firstName', VARCHAR(30)),
              Column('midname', VARCHAR(30)),
              Column('sex', TINYINT(4)),
              Column('birthdate', DATETIME),
              Column('doc_series', VARCHAR(8)),
              Column('doc_number', VARCHAR(16)),
              Column('doc_code', Integer),
              Column('policy_series', VARCHAR(16)),
              Column('policy_number', VARCHAR(16)),
              Column('policy_doctype', Integer),
              Column('insurance_orgcode', VARCHAR(12)),
              Column('LPU', VARCHAR(12)),
              Column('reg_date', DATETIME),
              Column('UPN', Integer, primary_key=True))
    t.create(checkfirst=True)
    mapper(Client, t)
    return t


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('mode', choices=('load',))
    opt = parser.parse_args()
    if opt.mode == 'load':
        print load_csv()


