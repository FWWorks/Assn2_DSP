from application.pub import Publisher
from configobj import ConfigObj
import sys


config_path, item = None, ''
if len(sys.argv) == 1:
    config_path = 'config/publisher.ini'
if len(sys.argv) >= 2:
    config_path = sys.argv[1]
if len(sys.argv) >= 3:
    item = sys.argv[2]

config = ConfigObj(config_path)
if item == '':
    item = list(config.keys())[0]

config = config[item]

p = Publisher(mode=int(config['mode']), ip_address=config['pub_addr'],
              broker_address=config['broker_addr'], strength=config['strength'], logfile=config['logfile'])

p.register(config['topic'])

while 1:
    x = input('>')
    topic = config['topic'][0] if isinstance(config['topic'], list) else config['topic']
    p.publish(topic, x)

