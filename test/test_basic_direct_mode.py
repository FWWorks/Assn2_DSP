from application.pub import Publisher
from application.sub import Subscriber
from application.broker import Broker
import time
import threading
from kazoo.client import KazooClient

mode=1

zk_root = '/test_temp_dir'

zookeeper = '127.0.0.1:2181'
zk = KazooClient(hosts=zookeeper)

broker1 = Broker({'mode': mode, 'port': 5557, 'zookeeper':zookeeper,
                  'logfile': 'log/temp_broker1.log', 'broker_addr':'tcp://127.0.0.1:5557'},zk_root=zk_root)
broker2 = Broker({'mode': mode, 'port': 5558, 'zookeeper':zookeeper,
                  'logfile': 'log/temp_broker2.log', 'broker_addr':'tcp://127.0.0.1:5558'},zk_root=zk_root)

pub = Publisher(mode=mode, ip_address='tcp://127.0.0.1:5001',pub_name='Pub2',
                zk_address=zookeeper, strength=0, logfile='log/temp_pub.log', zk_root=zk_root)

sub = Subscriber(ip_self='tcp://127.0.0.1:5006', ip_zookeeper=zookeeper, name='Sub2',
                   comm_type=mode, logfile='log/temp_sub.log', zk_root=zk_root)

def test_start():
    zk.start()
    try:
        zk.create(zk_root)
    except:
        pass

    thread1 = threading.Thread(target=broker1.start)
    thread1.start()
    time.sleep(1)

    thread2 = threading.Thread(target=broker2.start)
    thread2.start()

def test_publisher_register():
    res = pub.register('hello')
    assert res == 0

def test_subscriber_register():
    res = sub.register('hello')
    assert res == 0

def test_publish_and_receive():

    def publish():
        for i in range(3):
            pub.publish('hello', 'world')

    pub_thread = threading.Thread(target=publish)
    pub_thread.start()
    msg = sub.receive()
    assert msg['topic'] == 'hello'
    assert msg['value'] == 'world'
    pub_thread.join()

def test_pub_exit():
    pub.drop_system()

def test_sub_exit():
    sub.exit()

def test_broker_exit():
    broker1.stop()
    broker2.stop()

def test_clean():
    try:
        zk.delete(zk_root, recursive=True)
    except:
        pass