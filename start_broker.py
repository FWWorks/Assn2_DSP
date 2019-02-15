from application.broker import Broker
from configobj import ConfigObj
import sys

config_path, item = None, ''
if len(sys.argv) == 1:
    config_path = 'config/broker.ini'
if len(sys.argv) >= 2:
    config_path = sys.argv[1]
if len(sys.argv) >= 3:
    item = sys.argv[2]

config = ConfigObj(config_path)
if item == '':
    item = list(config.keys())[0]

broker = Broker(config=config[item])

broker.start()
