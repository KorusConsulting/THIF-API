from webapp import app as application
import os

p = os.path.dirname(os.path.abspath(__file__))
activate_this = "ve/bin/activate_this.py"
execfile(activate_this, dict(__file__=activate_this))