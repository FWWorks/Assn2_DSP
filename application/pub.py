from middleware.pub import *
import threading,time,socket
from logger import get_logger
from kazoo import client as kz_client
from kazoo.recipe.watchers import DataWatch

class Publisher:
    def __init__(self, mode, ip_address=None, zk_address=None, strength=0, logfile='log/pub.log', pub_name = None):
        self.ip_address = ip_address
        self.zk_address = zk_address
        self.strength = strength
        self.heartthread = threading.Thread(target=self.send_heart_beat)
        self.my_client = kz_client.KazooClient(hosts=zk_address)
        self.my_client.start()
        leader_watcher = DataWatch(self.my_client, '/Leader', self.update_broker_ip_socket)
        self.broker_address = self.get_broker_address()
        if mode == 1:
            self.pub_mw = PublisherDirectly(self.ip_address, self.broker_address)
        elif mode == 2:
            self.pub_mw = PublisherViaBroker(self.ip_address, self.broker_address)
        else:
            print("mode error, please choose approach")
        self.exited = False
        self.logger = get_logger(logfile)
        self.pub_name = pub_name

    def publish(self, topic, value):
        self.logger.info('publishing a msg. topic=%s, value=%s'%(topic, value))
        self.pub_mw.publish(topic, value)
        return 0

    def register(self, topic):
        #tmp_watch =  self.my_client.get("/Leader", watch=self.update_broker_ip_socket)
        self.broker_address = self.my_client.get("/Leader")[0].decode()
        self.pub_mw.register(topic)
        #if self.heartthread.is_alive() == False:
        #    self.heartthread.start()
        self.logger.info('pub register to bloker on %s. ip=%s, topic=%s'%(self.broker_address, self.ip_address, topic))
        node_url = "/Pub/" + self.pub_name
        node_data = self.ip_address + "," + topic
        self.my_client.create(node_url, node_data.encode(), ephemeral=True)


        return 0

    '''
    if a leader broker dies, watcher should use this function to update connection with the new leader broker
    '''
    def update_broker_ip_socket(self, data, status, version):
        self.broker_address = data.decode()
        self.pub_mw.update_broker_bind(self.broker_address)
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

    def get_broker_address(self):
        return self.my_client.get("/Leader")

    def get_broker_address_now(self):
        return self.broker_address

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
