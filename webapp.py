 # -*- coding: utf-8 -*-
import os
from flask import Flask
from flask.ext.login import LoginManager
import logging
import json


app = Flask(__name__)

config_file = os.path.join(os.path.dirname(__file__), 'config.json')
with open(config_file) as f:
    config = json.loads(f.read())
    app.config['SECRET_KEY'] = config['secret_key']
    app.config['DEBUG'] = True
    app.config['DB_NAME'] = config['db_name']
    app.config['TABLE_NAME'] = config['table_name']
    app.config['LOG_FILE'] = config['logfile']
    app.config['USER'] = {'LOGIN': config['api_login'],
                          'PASSWORD': config['api_password']}

login_manager = LoginManager()
login_manager.init_app(app)


import views


if __name__ == "__main__":
    logging.basicConfig(format='[%(levelname)s]%(asctime)s: %(message)s',
                        filename=app.config['LOG_FILE'],
                        level=logging.INFO)
    app.run(debug=True)
