from time import sleep
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import KafkaError, InvalidPartitionsError
from kafka.admin import NewPartitions
from threading import Thread
import json
import signal

def create_producer(servers:list, topic:str) -> KafkaProducer:
    """
    Returns an instance of the KafkaProducer
    """

    # Producer with json encoding
    producer = KafkaProducer(bootstrap_servers=servers,
                            value_serializer=lambda m: json.dumps(m).encode('ascii'),
                            )

    # Creating some partitions for the topic
    admin_client = KafkaAdminClient(bootstrap_servers=servers)
    topic_partitions = {}
    topic_partitions[topic] = NewPartitions(total_count=2)
    try:
        admin_client.create_partitions(topic_partitions)
    except InvalidPartitionsError:
        pass
    return producer


def send_data(thread_id, producer:KafkaProducer, topic:str, limit=1000):
    """
    Sends data to the brokers. Data is auto generated as simple Json
    """
    def __on_send_success(record_metadata):
        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)

    def __on_send_error(excp):
        print('I am an errback', exc_info=excp)
        # handle exception in some way...
    
    # produce asynchronously
    for i in range(limit):
        producer.send(topic, value={'test-key' : 'test-value'}).add_callback(__on_send_success).add_errback(__on_send_error)
        sleep(0.01) # Ratio

    # block until all async messages are sent
    producer.flush()

    
def start_producers(producer:KafkaProducer, topic:str, quantity=10):
    """
    Produces shares the same KafkaProducer instance. Different threads
    can be spawned to send messages.
    """
    assert quantity >= 1
    # create and start "quantiy" threads
    threads = []
    for n in range(1, quantity + 1):
        t = Thread(target=send_data, args=(n, producer, topic))
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

    # Creating producers
    bootstrap_servers = ['localhost:29092', 'localhost:39092']
    topic = "test-topic"
    producer = create_producer(bootstrap_servers, topic)
    start_producers(producer, topic, 10)
