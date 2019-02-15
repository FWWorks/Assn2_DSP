import zmq
import json


class SubDirect:

    def __init__(self, ip_self, ip_broker):
        self.ip = ip_self
        self.ip_b = ip_broker
        self.context_sub = None
        self.context_rcv = None
        self.socket_sub = None
        self.socket_rcv = None
        self.topics_list = set()
        self.socket_heartbeat = None

    def register(self, topic):
        self.topics_list.add(topic)
        self.context_sub = zmq.Context()
        self.socket_sub = self.context_sub.socket(zmq.REQ)
        self.socket_sub.connect(self.ip_b)
        self.socket_sub.send_json(json.dumps({"type": "add_subscriber", "ip": self.ip, "topic": topic}))
        ip = self.socket_sub.recv_json()['msg']
        self.context_rcv = zmq.Context()
        self.socket_rcv = self.context_rcv.socket(zmq.SUB)
        self.socket_rcv.setsockopt_string(zmq.SUBSCRIBE, '')
        self.socket_rcv.connect(ip)
        context2 = zmq.Context()
        self.socket_heartbeat = context2.socket(zmq.REQ)
        self.socket_heartbeat.connect(self.ip_b)

    def receive(self):
        msg = self.socket_rcv.recv_json()
        if msg['topic'] in self.topics_list:
            print("receive a message: topic = %s, value = %s" % (msg["topic"], msg["value"]))
        return msg

    def unregister(self, topic):
        self.socket_sub.send_json(json.dumps({"type": "sub_unregister_topic", "ip": self.ip, "topic": topic}))

    def exit(self):
        self.socket_sub.send_json(json.dumps({"type": "sub_exit_system", "ip": self.ip, "topic": "all"}))


class SubBroker:

    def __init__(self, ip_self, ip_broker):
        # print(ip_self, ip_broker, 'XXX')
        self.ip = ip_self
        self.ip_b = ip_broker
        self.context_sub = None
        self.context_ntf = None
        self.socket_sub = None
        self.socket_ntf = None
        self.socket_heartbeat = None

    def register(self, topic):
        self.context_sub = zmq.Context()
        self.socket_sub = self.context_sub.socket(zmq.REQ)
        self.socket_sub.connect(self.ip_b)

        if isinstance(topic, list):
            for t in topic:
                self.socket_sub.send_json({"type": "add_subscriber", "ip": self.ip, "topic": t})
                res = self.socket_sub.recv_json()
        else:
            self.socket_sub.send_json({"type": "add_subscriber", "ip": self.ip, "topic": topic})
            res = self.socket_sub.recv_json()

        self.context_ntf = zmq.Context()
        self.socket_ntf = self.context_ntf.socket(zmq.REP)
        self.socket_ntf.bind("tcp://*:%s" % self.ip.split(":")[2])
        context2 = zmq.Context()
        self.socket_heartbeat = context2.socket(zmq.REQ)
        self.socket_heartbeat.connect(self.ip_b)

    def notify(self):
        msg = self.socket_ntf.recv_json()
        print("receive a message: topic = %s, value = %s" % (msg["topic"], msg["value"]))
        self.socket_ntf.send_json({'msg': 'success'})
        return msg

    def unregister(self, topic):
        self.socket_sub.send_json(json.dumps({"type": "sub_unregister_topic", "ip": self.ip, "topic": topic}))

    def exit(self):
        self.socket_sub.send_json(json.dumps({"type": "sub_exit_system", "ip": self.ip, "topic": "all"}))
