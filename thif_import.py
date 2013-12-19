# -*- coding: utf-8 -*-

from argparse import ArgumentParser
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from crontab import CronTab
from db import init_db, import_session, remove_session
from models import Clients
try:
    import json
except ImportError:
    import simplejson as json
import glob
import os
import csv
import logging
import sys
import traceback



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


def client_params(context, row):
    kwargs = dict((CSV_DB_MAPPER[CSV_FIELDS[idx]],
                   el) for idx, el in enumerate(row))
    if kwargs['policy_number'].endswith(',00'):
        kwargs['policy_number'] = rreplace(kwargs['policy_number'],
                                           ',00', '', 1)
    return kwargs


def create_client(context, row):
    """
    creates class instance for row of table
    """
    return Clients(**client_params(context, row))


def rreplace(s, old, new, occurrence):
    """
    replace from the end
    """
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
            return make_mv(mf(value, *args, **kwargs))
        except Exception as e:
            exc_lnum = traceback.extract_tb(sys.exc_info()[2])[-1][1]
            exc_type = type(e).__name__
            exc_string = "[%s] %s (%s): %s" % (mf.__name__, exc_lnum,
                                               exc_type, str(e))
            return make_mv(e=exc_string)
    return mv


def make_mv(v=None, e=None):
    """
    Creates monadic value from some value
    """
    return (v, e)


def read_config(path):
    """
    Reads config file and returns it as a dict
    """
    with open(path) as f:
        config = json.loads(f.read())
        return config


def set_logging(config):
    logging.basicConfig(format='[%(levelname)s]%(asctime)s: %(message)s',
                        filename=config['logfile'], level=logging.INFO)
    return config


def find_files(context):
    """
    Finds all files which fit the *.csv pattern
    """
    all_csv_files = glob.glob(os.path.join(context['csv_path'], '*.csv'))
    context['csv_files'] = (c for c in all_csv_files
                            if not c.endswith('_loaded.csv'))
    return context


def load_files(context):
    """
    Puts all rows of all files into database
    """
    for csv_path in context['csv_files']:
        with open(csv_path) as csv_file:
            reader = csv.reader(csv_file,
                                delimiter=str(context['delimiter']))
            fields = tuple(reader.next())
            if fields != CSV_FIELDS:
                raise ValueError("%s: wrong fields" % csv_path)
            for row in reader:
                row = [item.decode("cp1251") for item in row]
                c = create_client(context, row)
                try:
                    import_session.add(c)
                    import_session.commit()
                except IntegrityError:
                    import_session.rollback()
                    obj = import_session.query(Clients).filter_by(UPN=c.UPN).first()
                    obj.set_attrs(**client_params(context, row))
                    import_session.add(obj)
                    import_session.commit()
        os.rename(csv_path, rreplace(csv_path, '.csv', '_loaded.csv', 1))
    return context


def configure_cron(config):
    cron = CronTab(config['cron']['user'])
    command = "python %s %s" % (os.path.realpath(__file__), "csv")
    for job in cron.find_command(command):
        cron.remove(job)

    job = cron.new(command=command)
    job.minute.every(config['cron']['minute'])
    job.hour.every(config['cron']['hour'])
    job.dom.every(config['cron']['day'])
    job.month.every(config['cron']['month'])
    job.dow.every(config['cron']['weekday'])
    job.enable()
    cron.write()


def csv_main():
    """
    main function for loading data into database
    """
    config = bind(read_config, make_mv('config.json'))
    config = bind(set_logging, config)
    context = bind(find_files, config)
    context = bind(load_files, context)
    remove_session()
    return context[1] or 'csv imported'


def cron_main():
    """
    main function for configuring cron
    """
    config = bind(read_config, make_mv('config.json'))
    config = bind(configure_cron, config)
    remove_session()
    return config[1] or 'cron configured'

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('mode', choices=('csv', 'cron'))
    opt = parser.parse_args()
    if opt.mode == 'csv':
        init_db()
        logging.info(csv_main())
    elif opt.mode == 'cron':
        init_db()
        logging.info(cron_main())
