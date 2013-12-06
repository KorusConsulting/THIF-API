 # -*- coding: utf-8 -*-
import os
from flask import Flask, request
from flask.ext.login import (LoginManager, UserMixin, login_user,
                             login_required)
from sqlalchemy import and_
from models import Clients, Date
from db import db_session, remove_session
from datetime import date, datetime
import logging
import json
import inspect


app = Flask(__name__)
login_manager = LoginManager()
login_manager.init_app(app)

config_file = os.path.join(os.path.dirname(__file__), 'config.json')
with open(config_file) as f:
    config = json.loads(f.read())
    app.config['SECRET_KEY'] = config['secret_key']
    app.config['DB_NAME'] = config['db_name']
    app.config['TABLE_NAME'] = config['table_name']
    app.config['LOG_FILE'] = config['logfile']
    app.config['USER'] = {'LOGIN': config['api_login'],
                          'PASSWORD': config['api_password']}


class APIEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return datetime.strftime(obj, "%d.%m.%Y")
        else:
            return super(APIEncoder, self).default(obj)


class APIDecoder(json.JSONDecoder):
    def decode(self, obj):
        obj = super(APIDecoder, self).decode(obj)
        for k in obj:
            if isinstance(getattr(Clients, k).type, Date):
                try:
                    obj[k] = datetime.strptime(obj[k], "%d.%m.%Y").date()
                except ValueError:
                    obj[k] = None
        return obj


class User(UserMixin):
    def __init__(self, login, password):
        self.login = login
        self.password = password

    def get_id(self):
        return self.login

    def is_authenticated(self):
        if self.login == app.config['USER']["LOGIN"] and self.password == app.config['USER']["PASSWORD"]:
            return True
        return False


@login_manager.user_loader
def load_user(userid):
    if userid == app.config["USER"]["LOGIN"]:
        return User(userid, app.config["USER"]["PASSWORD"])
    return None


@app.route("/login", methods=["POST"])
def login():
    try:
        data = json.loads(request.data)
        u = User(data['login'], data['password'])
        if u.is_authenticated():
            login_user(u)
            return 'logged in'
        else:
            return 'login or password incorrect', 401
    except Exception as e:
        logging.exception(e)
        return 'login or password incorrect', 401


@app.route("/search", methods=["POST"])
@login_required
def search():
    try:
        data = json.loads(request.data, cls=APIDecoder)
        q = [getattr(Clients, k) == v for k, v in data.iteritems()]
        res = list(db_session.query(Clients).filter(and_(*q)).all())
        attrs = [x for x in dir(Clients)
                 if not x.startswith('_') and x != "metadata"]
        res = [dict((attr, getattr(x, attr)) for attr in attrs
                    if not callable(getattr(x, attr)))
               for x in res]
        return json.dumps(res, cls=APIEncoder)
    except ValueError as e:
        logging.exception(e)
        res = dict(code=400, message=e.message, exc_name=e.__class__.__name__)
        return json.dumps(res, cls=APIEncoder), 400
    except Exception as e:
        logging.exception(e)
        res = dict(code=500, message=e.message, exc_name=e.__class__.__name__)
        return json.dumps(res, cls=APIEncoder), 500


@app.route("/check", methods=["POST"])
@login_required
def check():
    try:
        data = json.loads(request.data, cls=APIDecoder)
        q = [getattr(Clients, k) == v for k, v in data.iteritems()]
        count = db_session.query(Clients).filter(and_(*q)).count()
        if count > 0:
            return "true"
        return "false"
    except ValueError as e:
        logging.exception(e)
        res = dict(code=400, message=e.message, exc_name=e.__class__.__name__)
        return json.dumps(res, cls=APIEncoder), 400
    except Exception as e:
        logging.exception(e)
        res = dict(code=500, message=e.message, exc_name=e.__class__.__name__)
        return json.dumps(res, cls=APIEncoder), 500


@app.after_request
def shutdown_session(exception=None):
    remove_session()


if __name__ == "__main__":
    logging.basicConfig(format='[%(levelname)s]%(asctime)s: %(message)s',
                        filename=app.config['LOG_FILE'],
                        level=logging.INFO)
    app.run(debug=True)
