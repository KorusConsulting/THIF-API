from flask import Flask, request
from flask.ext.login import (LoginManager, UserMixin, login_user,
                             login_required, current_user)
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy import and_
from db import create_client_class
from datetime import date, datetime
import logging
import os
import binascii
import json


app = Flask(__name__)
login_manager = LoginManager()
login_manager.init_app(app)
app.config['SECRET_KEY'] = binascii.b2a_hex(os.urandom(15))
with open('config.json') as f:
    config = json.loads(f.read())
    app.config['DB_NAME'] = config['db_name']
    app.config['TABLE_NAME'] = config['table_name']
    app.config['LOG_FILE'] = config['logfile']
    connect = "mysql://%s:%s@localhost/%s" % (config['username'],
                                              config['password'],
                                              config['db_name'])
    Client = create_client_class(create_engine(connect), config['table_name'])
    del config
    del connect


class APIEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, date):
            return datetime.strftime(obj, "%d.%m.%Y")
        else:
            return super(APIEncoder, self).default(obj)


class User(UserMixin):
    created_users = {}

    def __init__(self, login, password):
        self.login = login
        self.password = password
        connection = 'mysql://%s:%s@localhost/%s' % (login, password,
                                                     app.config['DB_NAME'])
        self.engine = create_engine(connection)
        self.created_users[login] = self

    def __del__(self):
        if self.login in self.created_users:
            del self.created_users[self.login]

    def get_id(self):
        return self.login

    def is_authenticated(self):
        try:
            self.engine.connect()
            return True
        except OperationalError:
            return False


@login_manager.user_loader
def load_user(userid):
    return User.created_users[userid]


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
        data = json.loads(request.data)
        q = []
        for k, v in data.iteritems():
            q.append(getattr(Client, k) == v)
        session = sessionmaker(bind=current_user.engine)()
        res = list(session.query(Client).filter(and_(*q)).all())
        attrs = [x for x in dir(Client)
                 if not x.startswith('_') and x != "metadata"]
        res = [dict((attr, getattr(x, attr)) for attr in attrs) for x in res]
        return json.dumps(res,
                          cls=APIEncoder)
    except ValueError as e:
        logging.exception(e)
        return '', 400
    except Exception as e:
        logging.exception(e)
        return '', 500


@app.route("/check", methods=["POST"])
@login_required
def check():
    try:
        data = json.loads(request.data)
        q = []
        for k, v in data.iteritems():
            q.append(getattr(Client, k) == v)
        session = sessionmaker(bind=current_user.engine)()
        count = session.query(Client).filter(and_(*q)).count()
        if count > 0:
            return "true"
        return "false"
    except ValueError as e:
        logging.exception(e)
        return '', 400
    except Exception as e:
        logging.exception(e)
        return '', 500


if __name__ == "__main__":
    logging.basicConfig(format='[%(levelname)s]%(asctime)s: %(message)s',
                        filename=app.config['LOG_FILE'],
                        level=logging.INFO)
    app.run(debug=True)
