import datetime
from enum import Enum, IntEnum
from time import sleep
from uuid import uuid4
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import KafkaError, InvalidPartitionsError, TopicAlreadyExistsError
from kafka.admin import NewPartitions, NewTopic
from threading import Thread
from random import randint, randrange, sample
import json
import signal

from customers_generator import Customer


class CustomerProducer():

    """
    This is a kafka producer that simulates the "customer" behaviour.
    It is capable of creating transactions e.g buying items from a retail/shop.
    """

    class Settings(Enum):
        MINUTES = int(1)
        HOURS = int(2)

    update_intervals = ["MINUTE", ""]
    __servers = []
    __producer = None
    __admin = None
    __items = []
    __condition = True

    def __init__(self, servers: list, topic: str, items: list, customer: list[Customer]) -> None:
        """
        - servers: connections to kafka brokers
        - topic: topic channel for the retail e.x "<city>.<shopid>"
        - items: items sold by the shop
        """
        class topic_settings(IntEnum):
            PARTITIONS = 1
            REPLICATION_FACTOR = 1

        self.__servers = servers
        self.__topic = topic
        self.__items = items
        self.__customer_list = customer
        self.__admin =  KafkaAdminClient(bootstrap_servers=servers)
        self.create_topic(topic, int(topic_settings.PARTITIONS), int(topic_settings.REPLICATION_FACTOR))
        self.create_producer()

    
    def create_topic(self, topic_name:str, topic_partitions=1, replication_factor=1):
        topic_list = []
        topic_list.append(NewTopic(name=topic_name, num_partitions=topic_partitions, replication_factor=replication_factor))
        try:
            self.__admin.create_topics(new_topics=topic_list, validate_only=False)
        except TopicAlreadyExistsError as ex:
            print(ex)

    def create_producer(self,) -> None:
        """
        Creates an instance of the KafkaProducer
        - partitions: how many parallel queue for the topic to create
        """

        # Producer with json encoding
        self.__producer = KafkaProducer(bootstrap_servers=self.__servers,
                                        value_serializer=lambda m: json.dumps(m).encode('ascii'),
                                        )


    def create_transaction(self, num_items_to_buy=5, customer="") -> dict:
        """
        Generating a mock of a transaction to be later sent
        - customer: Customer
        """
        if num_items_to_buy <= 0:
            return {}
        
        if not customer:
            customer = self.__customer_list[randrange(0, len(self.__customer_list))]
            print(customer)
        transaction = {'txn_id': str(uuid4()), 'date': datetime.datetime.now().isoformat(), 'client_id': customer['client_id']}
        items_to_buy = sample(self.__items, num_items_to_buy)
        # Add item and quantity
        keys_to_keep = ['upc', 'description', 'price']

        # TODO remove the parser
        def __parser(item):
            return {k: item[k] for k in keys_to_keep}
        items_to_buy = list(map(__parser, items_to_buy))
        for item in items_to_buy:
            print(item)
            item['quantity'] = randrange(1,3)
        transaction['shopping_list'] = items_to_buy
        transaction['total_cost'] = sum([x['quantity']*x['price'] for x in items_to_buy])
        return transaction

    def send_transaction(self, transaction: dict) -> None:
        """
        Sends data to the brokers. Data is a transaction
        """
        def __on_send_success(record_metadata):
            print(record_metadata.topic)
            print(record_metadata.partition)
            print(record_metadata.offset)

        def __on_send_error(excp):
            print('I am an errback', exc_info=excp)
            # handle exception in some way...
        
        # produce asynchronously
        self.__producer.send(self.__topic, value=transaction).add_callback(__on_send_success).add_errback(__on_send_error)

        # block until all async messages are sent
        self.__producer.flush()

    def __activate_transaction_stream(self, update_interval=Settings.MINUTES):
        """
        Stream of transactions are sent to the configured retail
        by the customer
        """

        while self.__condition:

            txn = self.create_transaction()
            self.send_transaction(txn)
            sleep(1)
    
    def create_producers_threads(self, quantity=10):
        """
        Produces shares the same KafkaProducer instance. Different threads
        can be spawned to send messages.
        """
        assert quantity >= 1
        # create and start "quantiy" threads
        threads = []
        for n in range(1, quantity + 1):
            t = Thread(target=self.__activate_transaction_stream, args=())
            threads.append(t)
            t.daemon = True
            t.start()

        # wait for the threads to complete
        threads = [t.join() for t in threads if t is not None and t.is_alive()] 
    

# Driver code
if __name__ == "__main__":

    # CTRL-C management
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)
 
