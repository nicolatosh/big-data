from __future__ import annotations

import argparse
from ast import For
from itertools import repeat
from json import loads as json_loads, dumps as json_dumps
from multiprocessing import Pool
from uuid import uuid4


from colorama import Fore, Style, init as colorama_init
from kafka import KafkaConsumer, KafkaProducer

from database_manager import DatabaseManager
from retail_inventory import RetailInventory


def create_orders_consumer(servers:list, consumer_group: str ="orders_default", topic: str = "default-topic"):
    pass

def create_consumer(consumer_id:int, servers: list, consumer_group="default-group", topic: str = "default-topic") -> None:
    """
    Creates a single consumer for Json messages\n
    Consumer consumes the transactions and sends the "valid" ones
    to the broker for later usage by other consumers.
    The topic of the valid transactions is "<city>.<shopid>.validtxns"
    """
    # Enable colored print
    colorama_init()

    # Get a db instance
    db_manager = get_databasemanager()
    shop_id = topic.split(".")[1]
    valid_txn_topic = f"{topic}.validtxns"

    # Get the reoder client
    order_producer = create_order_producer(servers)
    orders_topic = f"{shop_id}.requestorder" 
    
    # Debug print
    print(f"{servers}:{consumer_group}:{topic}")
    try:
        _consumer = KafkaConsumer(topic,
                            group_id=consumer_group,
                            bootstrap_servers=servers,
                            value_deserializer=lambda m: json_loads(m.decode('ascii')))
        
        # Producer with json encoding
        _producer = KafkaProducer(bootstrap_servers=servers,
                                    value_serializer=lambda m: json_dumps(m).encode('ascii'))
                            
        # Action to perform per message
        for message in _consumer:
            # 1. Reading message aka print
            read_messages(consumer_id, message)
            # 2. Update records via db Transactions and send restock order if needed
            res = apply_transaction(message, db_manager, shop_id, order_producer, orders_topic)
            if res:
                # 3. Send the "valid" transaction to broker
                _producer.send(valid_txn_topic, value=message.value)
                _producer.flush()
    except Exception as e:
        print(e)
        return

def consumer_helper(arg):
    return create_consumer(*arg)

def apply_transaction(txn, db_manager: DatabaseManager, shop_id, orderer:KafkaProducer, order_topic:str) -> bool:
    """
    Applies the transaction to the inventory of the shop.\n
    Provides ACID guarantees.
    """
    client = db_manager.get_client()
    txn = txn.value
    items = txn['shopping_list']

    # Starting a transaction:
    # Updates each inventory record providing ACID guarantees
    # Every item must be updated or rollback
    try:
        with client.start_session() as session:
            with session.start_transaction():
                for item in items:
                    res = db_manager.update_many(query={"retailId": shop_id, "inventory.stock_level": {"$gte": item['quantity'] }}, newvalues={"$inc": {"inventory.$[item].stock_level": -item['quantity']}}, filters=[{"item.upc": item['upc']}])

                    if (not(res.acknowledged) or(res.matched_count  != 1) or (res.modified_count != 1)):
                        # This provides for automatic rollback
                        raise Exception(Fore.YELLOW + f"Transaction {txn['txn_id']} failed " + Style.RESET_ALL)
                
                    res = db_manager.execute_query([{"retailId": shop_id}, {"_id": 0, "inventory": {"$elemMatch": {"upc": item['upc']}}}])
                    # Check wheter to reorder
                    res_item = list(res)[0]
                    res_item = res_item["inventory"][0]
                    print(f"ITEM {res_item}")
                    if ((res_item['stock_level'] < res_item['rop']) and not res_item['in_restock']):
                        order = {"order_id": uuid4(), "upc": item['upc'], "quantity": res_item['reorder_quantity']}
                        # Set restock flag
                        args = [{"retailId": shop_id, "inventory.upc": item['upc']}, {"$set": {"inventory.$.in_restock": True}}]
                        res = db_manager.update(*args)
                        issue_order(orderer, order_topic, order)   
    except Exception as e:
        print(f"EX {e}")
        return False    
    print(Fore.GREEN +  f"Transaction {txn['txn_id']} done " + Style.RESET_ALL)
    return True

def get_databasemanager() -> DatabaseManager:
    """
    Returns an instance of db manager
    """
    db_manager = DatabaseManager()
    db_manager.connect_to_database(database_name="inventories", collection_name=RetailInventory.collection_name)
    db_manager.select_collection(collection_name=RetailInventory.collection_name)
    return db_manager



def create_consumers(servers:list, consumer_group:str, topic:str, processes=2):
    """
    You can configure the number of processes to act as consumers
    - servers: list of servers <host>:<port>
    - consumer_group: consumer group name
    - topic: topic name
    - processes: number of consumers
    """
    
    print(Fore.GREEN + "=== Initializng consumer ===" + Style.RESET_ALL)
    pool = Pool(processes)
    try:
        iterable = [x for x in range(processes)]
        pool.map_async(consumer_helper, iterable=zip(iterable, servers, repeat(consumer_group), repeat(topic)))
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, terminating workers")
        pool.terminate()
    else:
        pool.close()
    pool.join()
        
def read_messages(consumer_id:int, message) -> None:
    """
    Helper function for reading the message received by the consumer
    """
    # To consume latest messages and auto-commit offsets
    print(f"[consumer [{consumer_id}] t: {message.topic} p:{message.partition} o:{message.offset}] -> [key:{message.key} value:{message.value}]")

 
def create_order_receiver(servers:list, topic:str, consumer_group:str = "orders-group"):
    """
    Creates a Kafka consumer to manage restock orders coming from chain
    """
    # Get a db instance
    db_manager = get_databasemanager()

    shop_id = topic.split(".")[1]
    receive_order_topic = f"{shop_id}.receiveorder"

    consumer = KafkaConsumer(receive_order_topic,
                            group_id=consumer_group,
                            bootstrap_servers=servers,
                            value_deserializer=lambda m: json_loads(m.decode('ascii')))
    
    # Action to perform per message
    for order in consumer:
        # Apply the order received e.g update the item quantity
        res = apply_order(order, db_manager)
        if res:
            print("order processed")
            
def apply_order(order, db_manager:DatabaseManager):
    
    upc = order['upc']
    args = [{"inventory.upc": upc}, {"$inc": {"inventory.$.stock_level": order['quantity']}}]
    
    try:
        res = db_manager.update(*args)
        if (not(res.acknowledged) or(res.matched_count  != 1) or (res.modified_count != 1)):
            raise Exception(Fore.YELLOW + f"Order {order['order_id']} failed " + Style.RESET_ALL)
    except Exception as e:
        print(f"Exception: {e}")
        return False
    return True    


def create_order_producer(servers: list) -> KafkaProducer:
    """
    Creates a Kafka producer to send restock order to chain
    """
    # Producer with json encoding
    return KafkaProducer(bootstrap_servers=servers,
                                value_serializer=lambda m: json_dumps(m).encode('ascii'))

def issue_order(producer:KafkaProducer, topic:str, order):
    print(Fore.GREEN + "sending order" + Style.RESET_ALL)
    producer.send(topic, value=order)


if __name__ == "__main__":
    colorama_init()
    db_manager = get_databasemanager()
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--s', type=str, nargs='+', dest='servers', help='list of servers <addr>:<port>', required=True)
    parser.add_argument('--c', type=str , dest='consumer_group', help='consumer group', required=True)
    parser.add_argument('--t', type=str , dest='topic', help='topic name', required=True)
    args = parser.parse_args()

    # Debug print
    print(f"Script called with args: {args}")
    create_consumers(args.servers, args.consumer_group, args.topic, 2)
    create_order_receiver(args.servers, args.topic)