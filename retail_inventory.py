from dataclasses import dataclass
from database_manager import DatabaseManager
from inventory import Inventory


class RetailInventory():

    database_name = "retail"
    collection_name = "inventories"

    def __init__(self) -> None:
        self.__manager = DatabaseManager()
        self.__manager.connect_to_database(self.database_name, self.collection_name)
        self.__manager.select_collection(self.collection_name)
        self.__inventory = Inventory()

    def build_inventories(self, retails_list: list[dict], inventory_size=10, random_items=True) -> None:
        """
        Method to create shops/retails with inventory. Each
        item will have random quantity. Check Inventory script
        for details.
        """

        res = self.__manager.execute_query([{},{ "_id": 0}])
        if len(list(res)) != 0:
            return 

        retails_with_inventories = []
        print(retails_list)
        for retail in retails_list:
            items = self.__inventory.create_inventory(inventory_size, random_items)

            # Shop with all goods
            retail_with_inventory = {
                "retailId": retail['id'],
                "inventory": items
            }
            retails_with_inventories.append(retail_with_inventory)

        self.__manager.insert_document(retails_with_inventories)

    
    def get_inventories(self, retail_id=""):
        """
        Returns all inventories or a single shop iventory
        - retail_id: string
        """

        if retail_id:
            return self.__manager.execute_query([{"retailId": retail_id},{"_id": 0}])
        return self.__manager.execute_query([{}, {"_id": 0}])
