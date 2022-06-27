from colorama import Fore, Style
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
        retails_with_inventories = []
        print(retails_list)
        for retail in retails_list:

            res = self.__manager.execute_query([{"retailId": retail['id']},{ "_id": 0}])
            if len(list(res)) != 0:
                return 
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
        Returns all inventories or some shop iventories
        - retail_id: list of retail ids
        """

        if retail_id:
            return self.__manager.execute_query([{"retailId": {"$in": [x for x in retail_id]}},{"_id": 0}])
        return self.__manager.execute_query([{}, {"_id": 0}])

    def set_quantity(self, retail_id: int = "", item_upc: str ="", quantity: int = 100) -> bool:
        """
        Allows to update items stock level in inventories
        - retail_id: specific retail to update, default is all
        - item_upc: update specific item, default is all
        - quantity: stock level to add
        """
        # Empty inventories check
        res = self.__manager.execute_query([{},{ "_id": 0}])
        if len(list(res)) == 0:
            return False
        
        res = None
        if retail_id and item_upc:
            args = [{"retailId": retail_id, "inventory.upc": item_upc}, {"$set": {"inventory.$.stock_level": quantity}}]
            res = self.__manager.update(*args)
        else:
            args = [{}, {"$set": {"inventory.$[].stock_level": quantity}}, []]
            res = self.__manager.update_many(*args)
        if (not(res.acknowledged) or(not res.matched_count) or (not res.modified_count)):
            print(Fore.YELLOW + "Retail inventory error:" + Style.RESET_ALL + " failed to update item quantity")
            return False
        return True