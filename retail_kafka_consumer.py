from __future__ import annotations

import argparse
from ast import For, arg
from itertools import repeat
from json import loads as json_loads
from multiprocessing import Pool


import colorama
from colorama import Fore, Style
from kafka import KafkaConsumer

from database_manager import DatabaseManager
from retail_inventory import RetailInventory


def create_consumer(consumer_id:int, servers: list, consumer_group="default-group", topic="default-topic") -> None:
    """
    Creates a single consumer for Json messages\n
    """
    # Get a db instance
    db_manager = get_databasemanager()
    shop_id = topic.split(".")[1]
    
    # Debug print
    print(f"{servers}:{consumer_group}:{topic}")
    try:
        _consumer = KafkaConsumer(topic,
                            group_id=consumer_group,
                            bootstrap_servers=servers,
                            value_deserializer=lambda m: json_loads(m.decode('ascii')))
                            
        # Action to perform per message
        for message in _consumer:
            read_messages(consumer_id, message)
            apply_transaction(message, db_manager, shop_id)
    except Exception as e:
        print(e)
        return

def consumer_helper(arg):
    return create_consumer(*arg)

def apply_transaction(txn, db_manager, shop_id):
        """
        Applies the transaction to the inventory of the shop
        """
        txn = txn.value
        items = txn['shopping_list']
    
        for item in items[:1]:
            args = [{"retailId": shop_id, "inventory.upc": item['upc'], "inventory.stock_level": {"$gte": item['quantity'] }}, {"$inc": {"inventory.$.stock_level": -item['quantity']}}]
            res = db_manager.execute_query([{"retailId": shop_id},{"_id": 0}])
            print(res)
            res = db_manager.update(*args)
            print(res.raw_result)

            if (not(res.acknowledged) or(res.matched_count  != 1) or (res.modified_count != 1)):
                print(Fore.YELLOW + "failed" + Style.RESET_ALL)
            else:
                print(Fore.GREEN +  "don9e" + Style.RESET_ALL)

def get_databasemanager() -> DatabaseManager:
    db_manager = DatabaseManager()
    db_manager.connect_to_database(database_name="inventories", collection_name=RetailInventory.collection_name)
    db_manager.select_collection(collection_name=RetailInventory.collection_name)
    return db_manager



def create_consumers(servers:list, consumer_group:str, topic:str, processes=2):
    """
    You can configure the number of processes to act as consumers
    - processes: number of consumers
    """
    
    print(Fore.GREEN + "=== Initializng consumer ===" + Style.RESET_ALL)
    pool = Pool(processes)
    try:
        iterable = [x for x in range(processes)]
        res = pool.map_async(consumer_helper, iterable=zip(iterable, servers, repeat(consumer_group), repeat(topic)))
        res.get(60) # Without the timeout this blocking call ignores all signals.
    except KeyboardInterrupt:
        print("Caught KeyboardInterrupt, terminating workers")
        pool.terminate()
    else:
        pool.close()
    pool.join()
        
def read_messages(consumer_id:int, message) -> None:
    """
    Callback function: starts reading the messages received the consumer\n
    - consumer: Kafka consumer instance
    - consumer_id: param used to print id
    """
    # To consume latest messages and auto-commit offsets
    print(f"[consumer [{consumer_id}] t: {message.topic} p:{message.partition} o:{message.offset}] -> [key:{message.key} value:{message.value}]")

 
if __name__ == "__main__":
    colorama.init()
    db_manager = get_databasemanager()
    parser = argparse.ArgumentParser(description='test.')
    parser.add_argument('--s', type=str, nargs='+', dest='servers', help='list of servers <addr>:<port>', required=True)
    parser.add_argument('--c', type=str , dest='consumer_group', help='consumer group', required=True)
    parser.add_argument('--t', type=str , dest='topic', help='topic name', required=True)
    args = parser.parse_args()
    print(f"Script called with args: {args}")
    create_consumers(args.servers, args.consumer_group, args.topic, 2)
    