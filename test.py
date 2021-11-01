from broker import Broker
from node import Node
import time 


def main():
    broker = Broker("127.0.0.1", 49000)
    publisher = Node("127.0.0.1", 49001, "127.0.0.1", 49000)
    subscriber_1 = Node("127.0.0.1", 49002, "127.0.0.1", 49000)
    subscriber_2 = Node("127.0.0.1", 49003, "127.0.0.1", 49000)

    broker.listen()
    subscriber_1.listen()
    subscriber_2.listen()

    subscriber_1.subscribe("Strength")
    subscriber_1.subscribe("Sport")
    subscriber_2.subscribe("Strength")

    publisher.publish("Strength", "In what is power bro?.")
    publisher.publish("Sport", "Power is not in money.")
    publisher.publish("Strength", "Power is in truth.")

    broker.sendMessage()

    subscriber_1.unsubscribe("Strength")
    publisher.publish("Strength", "Power is force by accelaration dumbass.")
if __name__ == "__main__":
    main()