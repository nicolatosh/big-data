# Simple producer. Produces the "Hello World" message using topic "test"
# Make sure the "test" topic has been created. To do so use:
# docker exec -it <kafka-container-name> kafka-topics --create --topic <topic-name> --bootstrap-server localhost:9091

from kafka import KafkaProducer

bootstrap_servers = ['localhost:9091']
topicName = 'test'

producer = KafkaProducer(bootstrap_servers = bootstrap_servers)

producer.send(topicName, b'Hello World!')
producer.flush()