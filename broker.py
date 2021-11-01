import socket
import json
import threading
import dataclasses
import typing
import time

_DEFAULT_BUFFER_SIZE = 1024

@dataclasses.dataclass
class SubscriberData:
    subscrib_Ip: int
    subscrib_Port: int
    topics: typing.List[str]

class Broker:

    def __init__(self, broker_Ip, broker_Port):
        self.buffer_size = _DEFAULT_BUFFER_SIZE
        self.broker_Ip = broker_Ip
        self.broker_Port = broker_Port

        self.udp_send_socket = socket.socket(
            family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.udp_listen_socket = socket.socket(
            family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.udp_listen_socket.bind((self.broker_Ip, self.broker_Port))

        self.lock = threading.Lock()

        self.subscribers = {}
        self.messages = {}

        
    def sendMessage(self, ):
        thread = threading.Thread(target = Broker.sendMessage_Function, args = (self, ))
        thread.start()

    @staticmethod
    def sendMessage_Function(broker):
        while True:
            broker.lock.acquire()
            for topic, messages in broker.messages.items():
                if messages:
                    message = messages[0]
                    for subscriber, subscriberdata in broker.subscribers.items():
                        topics = subscriberdata.topics
                        if topic in topics:
                            subscrib_Ip = subscriberdata.subscrib_Ip
                            subscrib_Port = subscriberdata.subscrib_Port
                            broker.udp_send_socket.sendto(str.encode(message), (subscrib_Ip, subscrib_Port))               
                    
                    broker.messages[topic].pop(0)
            broker.lock.release()
                    
    def listen(self, ):
        thread = threading.Thread(target = Broker.listen_function, args = (self, ))
        thread.start()

    @staticmethod
    def listen_function(broker):
        while True:
            response = broker.udp_listen_socket.recvfrom(broker.buffer_size)[0]
            response = json.loads(response.decode())
            broker.lock.acquire()
            topic = response["topic"]
            type = response["type"]

            if type == "subscribe":
                subscrib_Ip = response["node_Ip"]
                subscrib_Port = response["node_Port"]
                if f"{subscrib_Ip}:{subscrib_Port}" in broker.subscribers:
                    if topic not in broker.subscribers[f"{subscrib_Ip}:{subscrib_Port}"].topics:
                        broker.subscribers[f"{subscrib_Ip}:{subscrib_Port}"].topics.append(topic)              
                else:
                    broker.subscribers[f"{subscrib_Ip}:{subscrib_Port}"] = SubscriberData(subscrib_Ip, subscrib_Port, [topic])

                print(f"Subed from: {subscrib_Port} to topic: {topic}")
            elif type == "publish":
                pub_ip = response["node_Ip"]
                pub_port = response["node_Port"]
                message = response["message"]
                if topic in broker.messages:
                    broker.messages[topic].append(message)
                else:
                    broker.messages[topic] = [message]
                print("Message to publish received on broker side")
            elif type == "unsubscribe":
                subscrib_Ip = response["node_Ip"]
                subscrib_Port = response["node_Port"]
                if f"{subscrib_Ip}:{subscrib_Port}" in broker.subscribers:
                    if topic in broker.subscribers[f"{subscrib_Ip}:{subscrib_Port}"].topics:
                        broker.subscribers[f"{subscrib_Ip}:{subscrib_Port}"].topics.remove(topic)              

                print(f"Unsubed from: {subscrib_Port} from topic: {topic}")
            broker.lock.release()

