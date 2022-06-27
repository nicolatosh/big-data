from retail_items import RetailItems
from random import randint, sample, shuffle
class Inventory():

    retail_items_builder = None
    __available_items = []

    minmax_per_category = {

        'carne': (10, 20),

        'Confezionati alimentari': (30, 40),

        'frutta_e_verdura': (10, 20),

        'gastronomia': (10, 20),

        'latticini_e_formaggi': (20, 30),

        'pane_e_pasticcieria': (10, 20),

        'pesce_e_sushi': (20, 30),

        'surgelati_e_gelati': (30, 40),

        'vegetali': (10, 20)

    }


    def __init__(self):
        self.retail_items_builder = RetailItems()
        self.__available_items = self.retail_items_builder.get_items()
        categories = [c for c in set(map(lambda x: x["category"], self.__available_items))]
        self.category_intervals = {}

        for category in categories:
            self.category_intervals[category] = self.minmax_per_category[category]


    def create_inventory(self, inventory_size=0, random_items=True, scale_factor=1) -> list:

        """
        This function returns a list of (limit) dictionaries. 
        Each dictionary is an item in the inventory of a retail.
        - inventory_size: how many items the inventory needs to have, default all the available
          Note: if it set greater than available items, the latter size is used
        - random_items: True if the method should choose randomly from avaliable items
        - scale_factor: Items quantity are multiplied by this factor. Use to create a large inventory.\n
            E.g 10 -> 10 times large
        """
        items_size = len(self.__available_items)
        if ((inventory_size <= 0) or (inventory_size > items_size) or (not inventory_size)):
            inventory_size = items_size
        
        # Check for random selection
        items = [x for x in (sample(self.__available_items, inventory_size) if random_items else self.__available_items[:inventory_size])]
 
        for item in items:
            quantity  = self.get_random_quantity(item)*scale_factor
            item['stock_level'] = quantity # quantity in the retailer inventory before transactions
            item['reorder_quantity'] = quantity # quantity to reorder when stock_level goes below reorder_point
            item['rop'] = item['stock_level']-(0.5*item['stock_level']) # reorder_point
            item['in_restock'] = False # flag to know whether the item has been reordered
            item['lead_time'] = 1 # days needed for item to travel        
        return items

    def get_random_quantity(self, item) -> int:
        """
        Returns a random quantity for a given item based on its category.\n
        Each category has its own min-max ranges from which pick a random quantity.
        """
        _category = self.category_intervals[item['category']]
        return randint(_category[0], _category[1])


    


