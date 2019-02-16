from middleware.broker import BrokerType1, BrokerType2
from logger import get_logger
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from middleware.broker import RegisterTable
from kazoo.recipe.watchers import ChildrenWatch, DataWatch

flag = True

class Broker:

    def __init__(self, config):
        self.config = config
        self.logger = get_logger(config['logfile'])
        self.zk = KazooClient(hosts=config['zookeeper'])
        self.im_leader = False
        self.broker = None
        self._znode = None
        self._to_watch = None

    def _create_znode(self):
        try:
            self.zk.create('/Election')
        except NodeExistsError:
            pass
        try:
            self.zk.create('/Leader')
        except NodeExistsError:
            pass

    def _on_pub_change(self, children):
        self._sync_map_table()
        self.logger.info('Publisher changes. Table=%s' % self.broker.table)

    def _on_sub_change(self, children):
        self._sync_map_table()
        self.logger.info('Subscriber changes. Table=%s' % self.broker.table)

    def _on_previous_leader_die(self, event):
        self._become_leader()

    def _sync_map_table(self):
        table = RegisterTable()
        pubs = self.zk.get_children('/Publisher')
        subs = self.zk.get_children('/Subscriber')
        for item in pubs:
            data, _ = self.zk.get('/Publisher/%s'%item)
            data = data.decode()
            ip, topics = data.split(',')
            table.add_pub(ip, topics)
        for item in subs:
            data, _ = self.zk.get('/Subscriber/%s'%item)
            data = data.decode()
            ip, topics = data.split(',')
            table.add_sub(ip, topics)
        self.broker.table = table

    def _become_leader(self):
        self._sync_map_table()
        self.zk.set('Leader', self.config['broker_addr'].encode())
        self.im_leader = True
        self.logger.info('i am the current leader. Table=%s'%self.broker.table)

    def _register_to_zk(self):
        others = self.zk.get_children('/Election')
        znode = self.zk.create('/Election/Broker_', ephemeral=True, sequence=True)
        self._znode = znode
        if not others:
            self._become_leader()
        else:
            ids = sorted([znode]+others)
            to_watch = ''
            for i, n in enumerate(ids):
                if n == znode:
                    to_watch = ids[i-1]
            self._to_watch = to_watch
            self.zk.get('/Election/%s'%to_watch, watch=self._on_previous_leader_die)
            self.logger.info('I am currently not the leader. Waiting for %s to die'%self._to_watch)

    def start(self):

        self.zk.start()

        if int(self.config['mode']) == 1:
            broker = BrokerType1(self.config)
        else:
            broker = BrokerType2(self.config)
        self.broker = broker
        self._register_to_zk()
        pub_wather = ChildrenWatch(self.zk, '/Publisher', self._on_pub_change)
        sub_wather = ChildrenWatch(self.zk, '/Subscriber', self._on_sub_change)
        self.logger.info('broker started. mode=%s, port=%s, znode=%s'%(self.config['mode'],
                                                                       self.config['port'], self._znode))
        while flag:
            broker.handle_req()
