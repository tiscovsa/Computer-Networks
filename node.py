import socket
import threading
import json

_DEFAULT_BUFFER_SIZE = 1024

class Node:

    def __init__(self, node_Ip: str, node_Port: int, broker_Ip: str, broker_Port: int):
        self.buffer_size = _DEFAULT_BUFFER_SIZE
        self.broker_Ip = broker_Ip
        self.broker_Port = broker_Port
        self.node_Ip = node_Ip
        self.node_Port = node_Port

        self.udp_send_socket = socket.socket(
            family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.udp_listen_socket = socket.socket(
            family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.udp_listen_socket.bind((self.node_Ip, self.node_Port))


    def publish(self, topic: str, message: str):
            
        payload= {
            "topic": topic,
            "type": "publish",
            "message": message,
            "node_Ip":self.node_Ip,
            "node_Port": self.node_Port,
        }
        data = json.dumps(payload)
        self.udp_send_socket.connect((self.broker_Ip, self.broker_Port))
        #self.udp_send_socket.sendto(data.encode(),(self.broker_Ip, self.broker_Port))
        self.udp_send_socket.sendall((data.encode()))
        #brocker_response = brocker_response.decode("utf-8")
        #print((f"Most recent response received {brocker_response[0]}:" 
        #    f"from{brocker_response[1]}"))

    def subscribe(self, topic: str):
            
        payload= {
            "topic": topic,
            "type": "subscribe",
            "node_Ip":self.node_Ip,
            "node_Port": self.node_Port,
        }
        data = json.dumps(payload)
        self.udp_send_socket.connect((self.broker_Ip, self.broker_Port))
        self.udp_send_socket.sendall((data.encode()))
        #brocker_response = brocker_response.decode("utf-8")
        #print((f"Most recent response received {brocker_response[0]}" 
        #    f"from{brocker_response[1]}"))

    def unsubscribe(self, topic: str):
            
        payload= {
            "topic": topic,
            "type": "unsubscribe",
            "node_Ip":self.node_Ip,
            "node_Port": self.node_Port,
        }
        data = json.dumps(payload)
        self.udp_send_socket.connect((self.broker_Ip, self.broker_Port))
        self.udp_send_socket.sendall((data.encode()))
        #brocker_response = brocker_response.decode("utf-8")
        #print((f"Most recent response received {brocker_response[0]}" 
        #    f"from{brocker_response[1]}"))

    def listen(self, ):
        thread = threading.Thread(target = Node.listen_function, args = (self, ))
        thread.start()

    @staticmethod
    def listen_function(node):
        while True:
            response = node.udp_listen_socket.recvfrom(node.buffer_size)
            message = response[0].decode()
            address = response[1]
            print(f"Message: {message}")
            print(f"Address: {address}")