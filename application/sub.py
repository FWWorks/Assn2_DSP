import threading,time
from middleware.sub import *
from logger import get_logger

sub_direct = 1
sub_broker = 2


class Subscriber:

    def __init__(self, ip_self, ip_broker, comm_type, logfile='log/sub.log'):
        self.ip = ip_self
        self.ip_b = ip_broker
        self.heartthread = threading.Thread(target=self.send_heart_beat)
        if comm_type == sub_direct:
            self.sub_mid = SubDirect(self.ip, self.ip_b)
        elif comm_type == sub_broker:
            self.sub_mid = SubBroker(self.ip, self.ip_b)
        else:
            print("Error in communication type: Only 1 and 2 are accepted.")
            exit(1)
        self.comm_type = comm_type
        self.exited = False
        self.logger = get_logger(logfile)

    def register(self, topic):
        self.sub_mid.register(topic)
        if self.heartthread.is_alive() == False:
            self.heartthread.start()
        self.logger.info('sub register to bloker on %s. ip=%s, topic=%s' % (self.ip_b, self.ip, topic))
        return 0

    def receive(self):
        msg = ''
        if self.comm_type == sub_direct:
            msg = self.sub_mid.receive()
        if self.comm_type == sub_broker:
            msg = self.sub_mid.notify()
        self.logger.info('receive a msg=%s'%msg)
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
            self.sub_mid.socket_heartbeat.send_json(
                (json.dumps({'type': 'sub_heartbeat', 'ip': self.ip, 'mess': "5"})))
            res = self.sub_mid.socket_heartbeat.recv_json()
        return 0



