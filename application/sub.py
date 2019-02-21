from kazoo.client import KazooClient
from kazoo.recipe.watchers import DataWatch
from middleware.sub import *
from logger import get_logger

sub_direct = 1
sub_broker = 2


class Subscriber:

    def __init__(self, ip_self, ip_zookeeper, comm_type, logfile='log/sub.log', name='', zk_root=''):
        self.name = name
        self.zk_root = zk_root
        self.ip = ip_self
        self.ip_b = None
        self.comm_type = comm_type
        self.exited = False
        self.logger = get_logger(logfile)
        self.zk_client = KazooClient(hosts=ip_zookeeper)
        self.zk_client.start()
        self.sub_mid = None

    def create_mw(self):
        ip_b, _ = self.zk_client.get("%s/Leader" % self.zk_root)
        ip_b = ip_b.decode()
        self.ip_b = ip_b
        if self.comm_type == sub_direct:
            self.sub_mid = SubDirect(self.ip, self.ip_b)
        elif self.comm_type == sub_broker:
            self.sub_mid = SubBroker(self.ip, self.ip_b)
        else:
            print("Error in communication type: Only 1 and 2 are accepted.")
            exit(1)

    def update(self, data, stat, ver):
        self.ip_b = data.decode()
        self.sub_mid.update_broker_ip(self.ip_b)

    def __get_broker_ip(self):
        self.ip_b = self.zk_client.get("%s/Leader"%self.zk_root)

    def register(self, topic):
        self.create_mw()
        self.sub_mid.register(topic)
        self.zk_client.create('%s/Subscriber/%s'%(self.zk_root, self.name), ('%s,%s'%(self.ip, topic)).encode(),
                              ephemeral=True, makepath=True)
        DataWatch(self.zk_client, "%s/Leader"%self.zk_root, self.update)
        self.logger.info('sub register to broker on %s. ip=%s, topic=%s' % (self.ip_b, self.ip, topic))
        return 0

    def receive(self):
        msg = ''
        if self.comm_type == sub_direct:
            msg = self.sub_mid.receive()
        if self.comm_type == sub_broker:
            msg = self.sub_mid.notify()
        self.logger.info('receive a msg=%s' % msg)
        return msg

    '''
    subscriber cancels a topic
    '''
    def unregister(self, topic):
        self.sub_mid.unregister(topic)

    '''
    subscriber wants to exit the system
    '''
    def exit(self):
        self.exited = True
        self.sub_mid.exit()
        return 0


