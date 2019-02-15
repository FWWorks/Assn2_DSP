from middleware.broker import BrokerType1, BrokerType2
from logger import get_logger
import threading
import time

flag = True

class Broker:

    def __init__(self, config):
        self.config = config
        self.logger = get_logger(config['logfile'])

    def start(self):

        if int(self.config['mode']) == 1:
            broker = BrokerType1(self.config)
        else:
            broker = BrokerType2(self.config)
        self.logger.info('broker started. mode=%s, port=%s'%(self.config['mode'], self.config['port']))
        check_thread = threading.Thread(target=broker.check_dead_entity)
        check_thread.start()

        while flag:
            broker.handle_req()
