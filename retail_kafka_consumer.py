from __future__ import annotations

import argparse
from itertools import repeat
from json import loads as json_loads
from multiprocessing import Pool

import colorama
from colorama import Fore, Style
from kafka import KafkaConsumer


def create_consumer(consumer_id:int, servers: list, consumer_group="default-group", topic="default-topic") -> None:
    """
    Creates a single consumer for Json messages\n
    """
    print(f"{servers}:{consumer_group}:{topic}")
    try:
        _consumer = KafkaConsumer(topic,
                            group_id=consumer_group,
                            bootstrap_servers=servers,
                            value_deserializer=lambda m: json_loads(m.decode('ascii')))
                            
        # Action to perform per message
        for message in _consumer:
            RetailConsumer.read_messages(consumer_id, message)
    except Exception as e:
        print(e)
        return

def consumer_helper(arg):
    return create_consumer(*arg)

class RetailConsumer():
    """
    Models a kafka consumer of the pub/sub model.\n
    Consumer represents the retail/shop.
    """

    def __init__(self, servers: list, consumer_group="default-group", topic="default-topic") -> None:
        """
        - servers: connections to kafka brokers
        - consumer_group: group name for consumers
        - topic: topic from which consumers read messages
        """
        self.servers = servers
        self.consumer_group = consumer_group
        self.topic = topic

    @staticmethod
    def read_messages(consumer_id:int, message) -> None:
        """
        Callback function: starts reading the messages received the consumer\n
        - consumer: Kafka consumer instance
        - consumer_id: param used to print id
        """
        # To consume latest messages and auto-commit offsets
        print(f"[consumer [{consumer_id}] t: {message.topic} p:{message.partition} o:{message.offset}] -> [key:{message.key} value:{message.value}]")
        

    def create_consumers(self, processes=2):
        """
        You can configure the number of processes to act as consumers
        - processes: number of consumers
        """
        
        print(Fore.GREEN + "=== Initializng consumer ===" + Style.RESET_ALL)
        pool = Pool(processes)
        try:
            iterable = [x for x in range(processes)]
            res = pool.map_async(consumer_helper, iterable=zip(iterable, self.servers, repeat(self.consumer_group), repeat(self.topic)))
            res.get(60) # Without the timeout this blocking call ignores all signals.
        except KeyboardInterrupt:
            print("Caught KeyboardInterrupt, terminating workers")
            pool.terminate()
        else:
            pool.close()
        pool.join()
    
if __name__ == "__main__":
    colorama.init()
    parser = argparse.ArgumentParser(description='test.')
    parser.add_argument('--s', type=str, nargs='+', dest='servers', help='list of servers', required=True)
    parser.add_argument('--c', type=str , dest='consumer_group', help='', required=True)
    parser.add_argument('--t', type=str , dest='topic', help='', required=True)
    args = parser.parse_args()
    print(f"Script called with args:  {args}")
    builder = RetailConsumer(args.servers, args.consumer_group, args.topic)
    builder.create_consumers(processes=2)
