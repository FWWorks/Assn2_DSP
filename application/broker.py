from middleware.broker import BrokerType1, BrokerType2
from logger import get_logger
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from middleware.broker import RegisterTable
from kazoo.recipe.watchers import ChildrenWatch, DataWatch


class Broker:

    def __init__(self, config, zk_root=''):
        self.zk_root = zk_root
        self.config = config
        self.logger = get_logger(config['logfile'])
        self.zk = KazooClient(hosts=config['zookeeper'])
        self.im_leader = False
        self.broker = None
        self.flag = True
        self._znode = None
        self._to_watch = None

    def _create_znode(self):
        for node in ['Election', 'Leader', 'Publisher', 'Subscriber']:
            try:
                self.zk.create('%s/%s'%(self.zk_root, node))
            except NodeExistsError as e:
                self.logger.error(str(e))
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
        pubs = self.zk.get_children('%s/Publisher'%self.zk_root)
        subs = self.zk.get_children('%s/Subscriber'%self.zk_root)
        for item in pubs:
            data, _ = self.zk.get('%s/Publisher/%s'%(self.zk_root, item))
            data = data.decode()
            ip, topics = data.split(',')
            table.add_pub(ip, topics)
        for item in subs:
            data, _ = self.zk.get('%s/Subscriber/%s'%(self.zk_root, item))
            data = data.decode()
            ip, topics = data.split(',')
            table.add_sub(ip, topics)
        self.broker.table = table

    def _become_leader(self):
        self._sync_map_table()
        self.zk.set('%s/Leader'%self.zk_root, self.config['broker_addr'].encode())
        self.im_leader = True
        self.logger.info('i am the current leader. Table=%s'%self.broker.table)

    def _register_to_zk(self):
        others = self.zk.get_children('%s/Election'%self.zk_root)
        znode = self.zk.create('%s/Election/Broker_'%self.zk_root, ephemeral=True, sequence=True)
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
            self.zk.get('%s/Election/%s'%(self.zk_root, to_watch), watch=self._on_previous_leader_die)
            self.logger.info('I am not the current leader. Waiting for %s to die'%self._to_watch)

    def start(self):
        self.zk.start()
        self._create_znode()
        self.flag = True
        if int(self.config['mode']) == 1:
            broker = BrokerType1(self.config)
        else:
            broker = BrokerType2(self.config)
        self.broker = broker
        self._register_to_zk()
        pub_wather = ChildrenWatch(self.zk, '%s/Publisher'%self.zk_root, self._on_pub_change)
        sub_wather = ChildrenWatch(self.zk, '%s/Subscriber'%self.zk_root, self._on_sub_change)
        self.logger.info('broker started. mode=%s, port=%s, znode=%s'%(self.config['mode'],
                                                                       self.config['port'], self._znode))
        while self.flag:
            try:
                broker.handle_req()
            except RuntimeError as e:
                if e.args != ('again', ):
                    raise

    def stop(self):
        self.flag = False
        self.logger.info('deleting znode: %s'%self._znode)
        try:
            self.zk.delete(self._znode)
        except:
            pass
