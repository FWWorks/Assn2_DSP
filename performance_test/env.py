from application.pub import Publisher
from application.sub import Subscriber
from application.broker import Broker
import threading
import time

def publish(pub, topic, msg_num, msg_len):
    pub.register(topic)
    time.sleep(5)
    for i in range(msg_num):
        # msg = '%s_msg_%s_' % (topic, i)
        # msg += 'payload'*msg_len
        pub.publish(topic, '%s_msg_%s' % (topic, i))

def receive(sub, topic):
    sub.register(topic)
    while 1:
        sub.receive()

class Simulator:

    def __init__(self, mode, pub_num, sub_num, topic='o2o', msg_len=1):
        self.mode = mode
        self.pub_num = pub_num
        self.sub_num = sub_num
        self.topic = topic
        self.msg_len = msg_len
        self.broker = None
        self.pubs = []
        self.subs = []
        self.pub_threads = []
        self.sub_threads = []
        self.broker_thread = None

    def clear_log(self):
        import os, shutil
        folder = 'temp_log'
        try:
            files = os.listdir(folder)
        except:
            os.mkdir(folder)
            return
        for the_file in files:
            file_path = os.path.join(folder, the_file)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
            except Exception as e:
                print(e)

    def start(self):
        self.clear_log()
        self.build()
        self.broker_thread.start()
        time.sleep(2)
        for t in self.pub_threads:
            t.start()
            time.sleep(5)
        time.sleep(1+0.1*self.pub_num)
        for t in self.sub_threads:
            t.start()
            time.sleep(5)

    def build(self):

        self.broker = Broker({'mode': self.mode, 'port': 5555, 'logfile': 'temp_log/broker.log',
                              'zookeeper': '127.0.0.1:2181', 'broker_addr':'tcp://127.0.0.1:5555'})
        self.broker_thread = threading.Thread(target=self.broker.start)
        for i in range(self.pub_num):
            pub = Publisher(mode=self.mode, ip_address='tcp://127.0.0.1:%s'%(5050+i),
                            zk_address='127.0.0.1:2181',
                            strength=0, logfile='temp_log/pub%s.log'%i, pub_name='pub%s'%i)
            self.pubs.append(pub)
            self.pub_threads.append(threading.Thread(target=publish, args=(pub, 'hello%s'%i, 1000, self.msg_len)))

        for i in range(self.sub_num):
            sub = Subscriber(ip_self='tcp://127.0.0.1:%s'%(6000+i), ip_zookeeper='127.0.0.1:2181',
               comm_type=self.mode, logfile='temp_log/sub%s.log'%i, name='sub%s'%i)
            self.subs.append(sub)

            if self.topic == 'o2o':
                topic = 'hello%s'%i
            elif self.topic == 'm2o':
                topic = 'hello0'
            elif self.topic == 'o2m':
                topic = ['hello%s'%i for i in range(self.pub_num)]
            else:
                topic = 'hello'
            self.sub_threads.append(threading.Thread(target=receive, args=(sub, topic)))

