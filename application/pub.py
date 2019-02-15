from middleware.pub import *
import threading,time,socket
from logger import get_logger

class Publisher:
    def __init__(self, mode, ip_address=None, broker_address=None, strength=0, logfile='log/pub.log'):
        self.ip_address = ip_address
        self.broker_address = broker_address
        self.strength = strength
        self.heartthread = threading.Thread(target=self.send_heart_beat)
        if mode == 1:
            self.pub_mw = PublisherDirectly(self.ip_address, broker_address)
        elif mode == 2:
            self.pub_mw = PublisherViaBroker(self.ip_address, broker_address)
        else:
            print("mode error, please choose approach")
        self.exited = False
        self.logger = get_logger(logfile)

    def publish(self, topic, value):
        self.logger.info('publishing a msg. topic=%s, value=%s'%(topic, value))
        self.pub_mw.publish(topic, value)
        return 0

    def register(self, topic):
        self.pub_mw.register(topic)
        if self.heartthread.is_alive() == False:
            self.heartthread.start()
        self.logger.info('pub register to bloker on %s. ip=%s, topic=%s'%(self.broker_address, self.ip_address, topic))
        return 0

    '''
    publisher cancels a topic
    '''
    def unregister_topic(self, topic):
        self.pub_mw.unregister(topic)
        return 0

    '''
    publisher wants to exit the system
    '''
    def drop_system(self):
        self.exited = True
        self.pub_mw.drop_system()
        self.heartthread.join()
        return 0

    '''
    send heart beat
    '''
    def send_heart_beat(self):
        while True:
            if self.exited:
                break
            time.sleep(1)
            self.pub_mw.socket_heartbeat.send_json((json.dumps({'type': 'pub_heartbeat', 'ip': self.ip_address, 'mess': "1"})))
            res = self.pub_mw.socket_heartbeat.recv_json()
        return 0


    def get_ip_address(self):
        return self.ip_address

    def get_strength(self):
        return self.strength

    def set_ip_address(self, ip_address):
        self.ip_address = ip_address
        return 0

    def set_strength(self, strength):
        self.strength = strength
        return 0