from redis import Redis
from colorama import Fore, Style, init as colorama_init
from kafka import KafkaConsumer
from redis_manager import RedisManager
import argparse
from json import dumps as json_dump, loads as json_loads

"""
This script should be used to fetch valid transaction from kafka broker\n
and append them in a persistent storage for later usage/analysis. In particular\n
Redis db is used.
"""

def create_consumer(servers: list, topics:list, consumer_group="transactions-group") -> None:
    """
    Creates a transaction consumer\n
    """
    # Enable colored print
    colorama_init()

    # Get a db instance
    db_manager = RedisManager().get_instance()
    assert db_manager.ping()

    # Updating topics
    new_topics = []
    for t in topics:
       new_topics.append(str(t + ".validtxns")) 
        
    # Debug print
    print(f"{servers}:{consumer_group}:{topics}")
    try:
        _consumer = KafkaConsumer(*new_topics,
                            group_id=consumer_group,
                            bootstrap_servers=servers,
                            value_deserializer=lambda m: json_loads(m.decode('ascii')))
                            
        # Action to perform per message
        for message in _consumer:
            res = process_transaction(db_manager, message)
            if res <= 0:
                raise Exception(f"List append of transaction:{message} failed")
    except Exception as e:
        print(e)
        return

def process_transaction(db_manager: Redis, txn, list_name="transactions") -> int:
    """
    Push transactions in the collection (list)
    """
    txn = txn.value
    print(Fore.CYAN + f"pushing {txn['txn_id']}" + Style.RESET_ALL)
    value =  json_dump(txn)
    return db_manager.lpush(list_name, value)


if __name__ == "__main__":
    colorama_init()
    print(Fore.GREEN + "== Batch transaction manager ==" + Style.RESET_ALL)
    parser = argparse.ArgumentParser(description='')
    parser.add_argument("-s", "--servers", type=str, nargs='+', dest='servers', help='list of servers <addr>:<port>', required=True)
    parser.add_argument('-c',"--consumer", type=str , dest='consumer_group', help='consumer group', required=True)
    parser.add_argument("-t", "--topics", type=str , nargs='+', dest='topics', help='topis list', required=True)
    args = parser.parse_args()

    create_consumer(args.servers, args.topics, args.consumer_group)

