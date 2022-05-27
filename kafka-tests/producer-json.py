import json
from time import sleep
from json import dumps
from kafka import KafkaProducer

DEBUG_PRINT = True
bootstrap_servers = ['localhost:9091']
topicName = 'test'

# The value serializer will automatically convert and encode the data
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

def produce_numbers_up_to(limit: int) -> None:
    for e in range(limit):
        data = {'number' : e}
        producer.send('test', value=data)
        if DEBUG_PRINT:
            print(f'Sent data: {data}')
        sleep(5)

# When using this function make sure to start the json consumer script
def produce_numbers_from_json():
    # Opening JSON file
    f = open('./numbers.json')
    data = json.load(f)
    for elem in data:
        producer.send('test', value=elem)
        if DEBUG_PRINT:
            print(f'Sent data: {elem}')
        sleep(5)

if __name__ == '__main__':
    produce_numbers_from_json()