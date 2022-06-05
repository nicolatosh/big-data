from database_manager import DatabaseManager
from retail_items import RetailItems
from retail_outlet import RetailBuilder

class Inventory():

    retail_items = None

    def __init__(self):
        self.retail_items = RetailItems()
        categories = [c for c in set(map(lambda x: x["category"], self.retail_items.get_items(2)))]
        self.category_intervals = {}
        minmax = (2, 10)
        for category in categories:
            self.category_intervals[category] = minmax
        print(categories)

    def test(self, limit=2) -> list:
        """
        This function returns a list of (limit) dictionaries. 
        Each dictionary is an item in the inventory of a retail.
        """
        items = self.retail_items.get_items(limit)
        for item in items:
            item['stock_level'] = self.quantity(item) #quantity in the retailer inventory before transactions
            item['rop'] = item['stock_level']-(0.5*item['stock_level']) #reorder point
        return items
    
    def quantity(self, item) -> int:
        _category = self.category_intervals[item['category']]
        return randint(_category[0], _category[1])


inventory = Inventory()
print(inventory.test())
        
        # match category:
        #     case 'carne': 
        #         return 
        #     case 'Confezionati alimentari':
        #         return 2
        #     case 'frutta_e_verdura':
        #         return 3
        #     case 'gastronomia':
        #         return 2
        #     case 'latticini_e_formaggi':
        #         return 5
        #     case 'pane_e_pasticceria':
        #         return 2
        #     case 'pesce_e_sushi':
        #         return 3
        #     case 'surgelati_e_gelati':
        #         return 7
        #     case 'vegetali':
        #         return 2
