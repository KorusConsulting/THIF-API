# -*- coding: utf-8 -*-
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))
from webapp import app as application

if __name__ == '__main__':
    application.run()
