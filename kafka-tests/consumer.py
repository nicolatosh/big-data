# Simple kafka consumer that prints available data of "test" topic

from kafka import KafkaConsumer
consumer = KafkaConsumer('test', bootstrap_servers=['localhost:9091'])
for message in consumer:
    print (message)