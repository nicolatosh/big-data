from json import dumps as json_dumps
from json import loads as json_loads
from threading import Thread

from colorama import Fore, Style
from kafka import KafkaConsumer, KafkaProducer

from database_manager import DatabaseManager
from inventory import Inventory


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
        print(Fore.GREEN + "[Chain] initialized!" + Style.RESET_ALL)


    def create_chain_consumers(self, num_threads=2) -> list:
        """
        Use this method to setup chain components e.g Kafka clients
        - num_threads: must be an even number. Can be used to scale!
        """
        assert (num_threads % 2) == 0 # even threads
        threads = []
        functions = [self.create_orders_consumer, self.create_orders_producer]
        for n in range(0, num_threads):
            t = Thread(target=functions[n], args=())
            t.setDaemon(True)
            threads.append(t)
            t.start()
        return threads

    def create_inventory(self, scale_factor:int = 100):
        """
        Creates the Chain inventory. It is like a "retail" inventory but with much larger scale
        - scale_factor: default is 100
        """
        
        # Inventory already present check
        self.inventory_db_manager.select_collection("inventory")
        res = self.inventory_db_manager.execute_query([{},{ "_id": 0}])
        if len(list(res)) != 0:
            return 

        # Creating inventory and saving to database
        items = self._inventory.create_inventory(scale_factor=scale_factor)
        self.inventory_db_manager.insert_document(items)
    

    def create_orders_consumer(self, consumer_group="chain-group-orders") -> None:
        """
        Creates a consumer for restock requests sent by retails.\n
        It schedules the restock operation to take place at right moment depending on items lead-time.\n
        Reorder topic: must be a list of topics "<retailId>.requestorder"
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
                    res = self.oders_db_manager.insert_document([message.value])
                    if not res:
                        print(Fore.RED + "[Chain] Error in saving orders" + Style.RESET_ALL)
                        return    

    def create_orders_producer(self):
        """
        Creates an instance of KafkaProducer
        """
        # Producer with json encoding
        self.producer = KafkaProducer(bootstrap_servers=self.servers,
                                value_serializer=lambda m: json_dumps(m).encode('ascii'))
    
    def send_orders(self):
        """
        Chain sends a supply order to the retail.\n 
        The order contains the quantity of the item as requested by the retail\n
        NOTE: orders are deleted by the db of the chain once delivered
        """
        
        # Load orders
        res = self.oders_db_manager.execute_query([{}, {"_id": 0}])
        orders = list(res)
        if len(orders) == 0:
            print(Fore.YELLOW + "[Chain] No order to send")
            return 

        for order in orders:
            # produce asynchronously
            print(Fore.GREEN + "[Chain]" + Style.RESET_ALL + f' Sending odrder ["upc": {order["upc"]}, "quantity": {order["quantity"]}]')
            topic = f"{order['retail_id']}.receiveorder"
            self.producer.send(topic, value=order)
        
        # Delete delivered orders
        self.oders_db_manager.delete_many({})

        
    def apply_order(self, order) -> bool:
        """
        Applies the transaction order to the orders of the chain.\n
        It means to decrease the quantity of the inventory of the amount requested by the order.
        Provides ACID guarantees.
        """
        order = order.value
        try:
            args = [{"upc": order['upc']}, {"$inc": {"stock_level": -order['quantity']}}]
            res = self.inventory_db_manager.update(*args)

            if (not(res.acknowledged) or(res.matched_count  != 1) or (res.modified_count != 1)):
                raise Exception(Fore.YELLOW + f"[Chain] Order {order['order_id']} failed " + Style.RESET_ALL)
            else:
                print(Fore.GREEN + f"[Chain] Order {order['order_id']} received " + Style.RESET_ALL)
                return True
        except Exception as e:
            print(Fore.RED + f"Exception {e}" + Style.RESET_ALL)
            return False    


    def calculate_and_update_rop(self, velocities: list):
        """
        Calculates the ROP as [velocity * lead_time] then updates the current rop value\n
        per each item. 
        """
        print(Fore.GREEN + "[Chain]" + Style.RESET_ALL + " updating rops...")
        try:
            for item in velocities:
                upc = item[0]
                velocity = item[1]
                args = [{"upc": upc}, [{"$set": {"rop": {"$multiply": ["$lead_time", int(velocity)]}}}]]
                res = self.inventory_db_manager.update(*args)
                if (not(res.acknowledged) or(res.matched_count  != 1) or (res.modified_count != 1)):
                    raise Exception(f"[Chain] Rop update failed")
                
                res = self.inventory_db_manager.execute_query([{"upc": upc}, {"_id": 0}])
                res_list = list(res)[0]
                assert velocity == res_list['rop']
        except Exception as e:
            print(Fore.RED + f"Exception {e}" + Style.RESET_ALL)
            return 

