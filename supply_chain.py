from ast import For
from threading import Thread
from time import sleep
from uuid import uuid4
from kafka import KafkaConsumer, KafkaProducer
from database_manager import DatabaseManager
from inventory import Inventory
from json import loads as json_loads, dumps as json_dumps
from colorama import init as colorama_init, Fore, Style
from retail_kafka_consumer import read_messages
import argparse

class SupplyChain():

    def __init__(self, id: int, servers: list, topics: str) -> None:
        """
        Create a supply chain instance
        - id: id of chain
        - servers: list of kafka brokers
        - topics: <city>.<retailId>
        """

        # Creating topics for retail/chain communication about orders
        _reorder_topics=[]
        for t in topics:
            shop_id = t.split(".")[1]
            _reorder_topics.append(f"{shop_id}.requestorder")    
        self.reorder_topics = _reorder_topics

        self.chain_id = id
        self.servers = servers
        self.inventory_db_manager = DatabaseManager()
        self.inventory_db_manager.connect_to_database("inventory", "chain_inventory")
        self._inventory = Inventory()
        self.create_inventory()
        self.create_chain_consumers()
        print(Fore.RED + "DONE INIT" + Style.RESET_ALL)


    def create_chain_consumers(self, num_threads=2) -> list:
        threads = []
        functions = [self.create_orders_consumer, self.create_orders_producer]
        for n in range(0, num_threads):
            t = Thread(target=functions[n], args=())
            t.setDaemon(True)
            threads.append(t)
            t.start()
        return threads

    def create_inventory(self):
        
        # Inventory already present check
        self.inventory_db_manager.select_collection("inventory")
        res = self.inventory_db_manager.execute_query([{},{ "_id": 0}])
        if len(list(res)) != 0:
            return 

        # Creating inventory and saving to database
        items = self._inventory.create_inventory(scale_factor=100)
        self.inventory_db_manager.insert_document(items)
    

    def create_orders_consumer(self, consumer_group="chain-group-orders") -> None:
        """
        Creates a consumer for restock requests sent by retails.\n
        It schedules the restock operation to take place at right moment depending on items lead-time.
        - servers
        - topics must be a list of topics "<retailId>.requestorder"
        """

        # Connecting to database
        self.oders_db_manager = DatabaseManager()
        self.oders_db_manager.connect_to_database("orders", "chain_orders")

        consumer = KafkaConsumer(
                                group_id=consumer_group,
                                bootstrap_servers=self.servers,
                                value_deserializer=lambda m: json_loads(m.decode('ascii')))
        consumer.subscribe(topics=self.reorder_topics)

        # Action per retail request
        while True:
            for message in consumer:
                # 1. Try to reserve the quantity for the given order
                if self.apply_order(message):
                    # 2. Save request/order
                    self.oders_db_manager.insert_document([message.value])    

    def create_orders_producer(self):
        
        # Producer with json encoding
        self.producer = KafkaProducer(bootstrap_servers=self.servers,
                                value_serializer=lambda m: json_dumps(m).encode('ascii'))
    
    def send_orders(self):
        """
        Chain sends to the retail the quantity of the item requested
        """
        
        # Load orders
        res = self.oders_db_manager.execute_query([{},{ "_id": 0}])
        if len(list(res)) == 0:
            return 
        
        orders = list(res)
        for order in orders:
            # produce asynchronously
            message = {Fore.GREEN + "[Chain]" + Style.RESET_ALL + "upc": order['upc'], "quantity": order['quantity']}
            topic = f"{order['retailId']}.receiveorder"
            self.producer.send(topic, value=message)
        
        # Delete orders
        self.oders_db_manager.delete_many({})

        
    def apply_order(self, order) -> bool:
        """
        Applies the transaction order to the orders of the chain.\n
        It means to decrease the quantity of the inventory of the amount requested by the order.
        Provides ACID guarantees.
        """
        client = self.inventory_db_manager.get_client()
        order = order.value
        items = order['shopping_list']

        # Updates each item record providing ACID guarantees
        # Every item must be updated or rollback
        try:
            with client.start_session() as session:
                with session.start_transaction():
                    for item in items:
                        args = [{"upc": item['upc']}, {"$inc": {"stock_level": -item['quantity']}}]
                        res = self.inventory_db_manager.update(*args)

                        if (not(res.acknowledged) or(res.matched_count  != 1) or (res.modified_count != 1)):
                            # This provides for automatic rollback
                            raise Exception(Fore.YELLOW + f"[Chain] Order {order['order_id']} failed " + Style.RESET_ALL)
                        else:
                            print(Fore.GREEN + f"[Chain] Order {order['order_id']} received " + Style.RESET_ALL)
        except Exception as e:
            print(e)
            return False    
        return True


    def calculate_and_update_rop(self, velocities: list):
        """
        Calculates the ROP as [velocity * lead_time"] then updates the current rop value\n
        per each item. 
        """
        print(Fore.GREEN + "[Chain]" + Style.RESET_ALL + " updating rops...")
        try:
            for item in velocities:
                upc = item[0]
                velocity = item[1]
                args = [{"upc": upc}, {"$set": {"rop": "lead_time" * velocity}}]
                res = self.inventory_db_manager.update(*args)
                print(res.raw_result)
                if (not(res.acknowledged) or(res.matched_count  != 1) or (res.modified_count != 1)):
                    raise Exception(Fore.YELLOW + f"[Chain] Rop update failed " + Style.RESET_ALL)
        except Exception as e:
            print("EX", e)
            return False

