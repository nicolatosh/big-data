from database_manager import DatabaseManager


from retail_items import RetailItems


from retail_outlet import RetailBuilder



class Inventory():



    retail_items = None

    minmax_per_category = {

        'carne': (10,20),

        'Confezionati alimentari': (30,40),

        'frutta_e_verdura': (10,20),

        'gastronomia': (10,20),

        'latticini_e_formaggi': (20,30),

        'pane_e_pasticceria': (10,20),

        'pesce_e_sushi': (20,30),

        'surgelati_e_gelati': (30,40),

        'vegetali': (10,20)

    }



    def __init__(self):


        self.retail_items = RetailItems()


        categories = [c for c in set(map(lambda x: x["category"], self.retail_items.get_items(2)))]


        self.category_intervals = {}

        for category in categories:


            self.category_intervals[category] = minmax_per_category[category]


        print(categories)



    def create_inventory(self, limit=2) -> list:

        """


        This function returns a list of (limit) dictionaries. 


        Each dictionary is an item in the inventory of a retail.


        """


        items = self.retail_items.get_items(limit)

        for item in items:


            item['stock_level'] = self.get_quantity(item) #quantity in the retailer inventory before transactions


            item['rop'] = item['stock_level']-(0.5*item['stock_level']) #reorder point
        return items
    


    def get_quantity(self, item) -> int:


        _category = self.category_intervals[item['category']]


        return randint(_category[0], _category[1])




inventory = Inventory()


print(inventory.create_inventory())
    


