import datetime
import json
from enum import IntEnum
from random import randrange, sample
from threading import Thread
from time import sleep, time
from uuid import uuid4

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import  NewTopic
from kafka.errors import TopicAlreadyExistsError

from customers_generator import Customer
from time import time

class KafkaProducerWrapper(object):

    __servers = []
    __admin = None
    producer = None
    topic = None

    def __init__(self, servers: list, topic: str) -> None:
        """
        - servers: connections to kafka brokers
        - topic: topic to create
        """
        class topic_settings(IntEnum):
            PARTITIONS = 2
            REPLICATION_FACTOR = 1

        self.__servers = servers
        self.topic = topic
        self.__admin = KafkaAdminClient(bootstrap_servers=servers)
        self.create_topic(topic, int(topic_settings.PARTITIONS), int(topic_settings.REPLICATION_FACTOR))
        self.create_producer()

    
    def create_topic(self, topic_name:str, topic_partitions=1, replication_factor=1):
        topic_list = []
        topic_list.append(NewTopic(name=topic_name, num_partitions=topic_partitions, replication_factor=replication_factor))
        try:
            self.__admin.create_topics(new_topics=topic_list, validate_only=False)
        except TopicAlreadyExistsError as ex:
            # Does not really matter
            pass

    def create_producer(self) -> None:
        """
        Creates an instance of the KafkaProducer
        - partitions: how many parallel queue for the topic to create
        """

        # Producer with json encoding
        self.producer = KafkaProducer(bootstrap_servers=self.__servers,
                                value_serializer=lambda m: json.dumps(m).encode('ascii'))


class CustomerProducer(KafkaProducerWrapper):

    """
    This is a kafka producer that simulates the "customer" behaviour.
    It is capable of creating transactions e.g buying items from a retail/shop.
    """
    __items = []
    __condition = True

    def __init__(self, servers: list, topic: str, items: list, customer: list) -> None:
        super().__init__(servers, topic)
        self.__producer = self.producer
        self.__topic = self.topic

        """
        - servers: connections to kafka brokers
        - topic: topic channel for the retail e.x "<city>.<shopid>"
        - items: items sold by the shop
        """
        self.__items = items
        self.__customer_list = customer

    def create_transaction(self, num_items_to_buy=5, customer="") -> dict:
        """
        Generating a mock of a transaction to be later sent
        - num_items_to_buy: How many different items a customer should buy.\n
          Note: Each item quantity is random
        - customer: Customer. If not specified, a random Customer is chosen
        """
        if num_items_to_buy <= 0:
            return {}
        
        if not customer:
            customer = self.__customer_list[randrange(0, len(self.__customer_list))]
        transaction = {'txn_id': str(uuid4()), 'date': datetime.datetime.now().isoformat(), 'client_id': customer['client_id']}
        items_to_buy = sample(self.__items, num_items_to_buy)
        
        # Adding additional info to transaction:
        # 1. Random items
        # 2. Quantities per item
        # 3. Total cost
        keys_to_keep = ['upc', 'description', 'price']

        def __parser(item):
            return {k: item[k] for k in keys_to_keep}
        items_to_buy = list(map(__parser, items_to_buy))
        for item in items_to_buy:
            item['quantity'] = randrange(1,3)
        transaction['shopping_list'] = items_to_buy
        transaction['total_cost'] = round(sum([x['quantity']*x['price'] for x in items_to_buy]), 2)
        return transaction

    def send_transaction(self, transaction: dict, thread_id:int) -> None:
        """
        Sends data to the brokers. Data is a transaction
        """
        def __on_send_success(record_metadata):
            print(f"Thread [{thread_id}]: [topic: {record_metadata.topic} partition: {record_metadata.partition} offset: {record_metadata.offset}]")

        def __on_send_error(excp):
            print('I am an errback', exc_info=excp)
            # handle exception in some way...
        
        # produce asynchronously
        self.__producer.send(self.__topic, value=transaction).add_callback(__on_send_success).add_errback(__on_send_error)

        # block until all async messages are sent
        self.__producer.flush()
    
    def stop_stream(self):
        self.__condition = False

    def __activate_transaction_stream(self, thread_id:int, sleep_time:int, simulation_time:int):
        """
        Stream of transactions are sent to the configured retail
        by the customer
        """
        start_time = time()

        while self.__condition:

            txn = self.create_transaction()
            self.send_transaction(txn, thread_id)
            sleep(sleep_time)
            # Check end of simulation
            _time = time() - start_time
            if ( _time >= simulation_time):
                return
    
    def create_producers_threads(self, turnout:list, simulation_time:int = 60*60*12, quantity:int = 10) -> list:
        """
        Produces shares the same KafkaProducer instance. Different threads
        can be spawned to send messages.
        - turnout: list with affluence values
        - simulation_time: total simulation time in seconds, default is 12h [8->20]
        - quantity: how many threads
        """
        # calculating frequency of txn to be sent in order to reach the turnout
        sleep_timings = []
        for tval in turnout:
            sleep_time = 600 / round(tval/quantity, 2) # in seconds
            sleep_timings.append(sleep_time)

        assert quantity >= 1
        # create and start "quantiy" threads
        threads = []
        for n in range(1, quantity + 1):
            t = Thread(target=self.__activate_transaction_stream, args=(n, sleep_timings[n], simulation_time))
            t.setDaemon(True)
            threads.append(t)
            t.start()

        return threads


 
