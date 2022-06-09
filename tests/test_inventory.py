from random import randint, sample
from time import sleep
from retail_items import RetailItems
from database_manager import DatabaseManager
from threading import Thread
import pytest


class TestInventory():
    """
    Test for Inventory.

    """
    @pytest.fixture(autouse=True)
    def initalizer(self) -> None:
        self.__retail_items = RetailItems()
        self.__database_manager = DatabaseManager()
        self.__database_manager.connect_to_database(self.__retail_items.collection_name, self.__retail_items.database_name)

    def test_crud(self,):
        self.__retail_items.create_and_store_items()
        assert self.__retail_items.get_items() != None 

    def test_concurrency(self,):
        """
        This test aims to check MongoDb concurrency control.
        It allows for multiple documents update if they are "embedded" 
        into an external single document.

        Different clients (threads) execute operations on
        the same item (document) concurrently.
        """

        # Simple item used
        quantity = 100
        test_document = [{
            "shopName": "test-shop",
            "shopId": "test-id",
            "inventory": [
                {
                "item": "cipolla",
                "quantity": quantity
                },
                {
                "item": "fagioli",
                "quantity": quantity
                },
            ]
        }]

        # Inserting item into database if not present
        self.__database_manager.select_collection("test-inventory")
        res = self.__database_manager.execute_query([{}])
        if len(list(res)) == 0:
            assert self.__database_manager.insert_document(test_document)

        # Methods to simulate concurrent operations
        # on a embedded document 
        def increase():
            args = [{"item": "cipolla", "quantity": {"$gt": 0 }}, {"$inc": {"quantity": 1}}]
            self.__database_manager.update(*args)
    
        def decrease():
            args = [{"item": "cipolla", "quantity": {"$gt": 0 }}, {"$inc": {"quantity": -1}}]
            self.__database_manager.update(*args)
        
        # Clients = threads
        MAX_TH = 10
        threads = []
        for n in range(1, MAX_TH + 1):
            t = Thread(target=increase, args=())
            t2 = Thread(target=decrease, args=())
            threads.append(t)
            threads.append(t2)
            t.daemon = True
            t2.daemon = True
        
        # Activating 2*MAX_TH threads in a random order
        # with random dealys (millis) among activations
        random_order = sample(range(2*MAX_TH), 2*MAX_TH)
        for i in random_order:
            threads[i].start()
            sleep(randint(1, 10)/100)

        # wait for the threads to complete
        threads = [t.join() for t in threads if t is not None and t.is_alive()]  
        
        # Quantity must be the same because the balanced
        # number of threads updated and decreased the "quantity"
        # by the same amount
        res = self.__database_manager.execute_query([{"inventory": {"$elemMatch": {"item": "cipolla"}}},{ "_id": 0, "inventory.$": 1}])
        res_list = list(res)
        assert len(res_list) == 1
        assert res_list[0]['inventory'][0]['quantity'] == quantity
        