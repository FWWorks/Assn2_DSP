from application.pub import Publisher
from application.sub import Subscriber
from application.broker import Broker, flag
import time
import threading


mode=2

broker_addr = 'tcp://127.0.0.1:5556'

broker = Broker({'mode': mode, 'port': 5556, 'logfile': 'log/temp_broker.log'})

pub = Publisher(mode=mode, ip_address='tcp://127.0.0.1:5001',
                  broker_address=broker_addr, strength=0, logfile='log/temp_pub.log')

sub = Subscriber(ip_self='tcp://127.0.0.1:5006', ip_broker=broker_addr,
                   comm_type=mode, logfile='log/temp_sub.log')

thread = threading.Thread(target=broker.start)
thread.start()
time.sleep(1)


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
