from ast import arg
from pprint import pp
from kafka import KafkaConsumer
from json import loads as json_loads
from multiprocessing import Pool, pool
import signal

def start_consumer(foobar):
    """
    Starts the consumer
    """
    # To consume latest messages and auto-commit offsets
    try:
        consumer = KafkaConsumer('test-topic',
                                group_id='test-group',
                                bootstrap_servers=['localhost:29092', 'localhost:39092'],
                                value_deserializer=lambda m: json_loads(m.decode('ascii')))
                                
        for message in consumer:
            print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                message.offset, message.key,
                                                message.value))
    except KeyboardInterrupt:
        # Parent stopped worker
        return

def main(processes=2):
    """
    You can configure the number of processes to act as consumers
    """
    print("Initializng")
    pool = Pool(processes)
    try:
        res = pool.map_async(start_consumer, iterable=['a','b'])
        res.get(60) # Without the timeout this blocking call ignores all signals.
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, terminating workers")
        pool.terminate()
    else:
        pool.close()
    pool.join()

# Driver code
if __name__ == "__main__":
    main(2)

