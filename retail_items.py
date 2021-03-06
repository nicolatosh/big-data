from random import randrange
import re, os, json
from colorama import Fore, Style
from database_manager import DatabaseManager

class RetailItems:

    database_name = "retails"
    collection_name = "retails_items"

    def __init__(self):
        self.__db_manager = DatabaseManager()
        self.__db_manager.connect_to_database(database_name=self.database_name, collection_name=self.collection_name)

    def __label_to_price_per_kg(self, label):
        '''
        Utility to label items 
        '''
        regex = r'Euro (\d+,\d+) \/ ([a-z]+)'
        match = re.match(regex, label)
        
        if not match:
            return None
        
        groups = list(match.groups())
        price_per_kg = float(groups[0].replace(',', '.'))
        
        # Adjust price per kg if necessary
        if groups[1] == 'g' or groups[1] == 'ml':
            price_per_kg *= 1000
        elif groups[1] == 'hg':
            price_per_kg *= 10
        elif groups[1] == 'pz':
            price_per_kg = None
        
        return price_per_kg

    def __parse_har(self, har_filename):
        '''
        Utiliy to parse items from HAR files downloaded from Esselunga webstore
        '''
        # Parse har as json
        with open('har/{}'.format(har_filename)) as f:
            js = json.loads(f.read())
        
        # Iterate over responses and get all the entities
        entities = []
        for entry in js['log']['entries']:
            response = json.loads(entry['response']['content']['text'])
            entities.extend(response['entities'])
            
        for entity in entities:
            # Strip description
            entity['description'] = entity['description'].strip()
            
            # Calculate price per kg by parsing the label
            entity['price_per_kg'] = self.__label_to_price_per_kg(entity['label'])
            
            # Add qty
            entity['qty'] = ' '.join([entity['unitValue'], entity['unitText']])
            
            # Add category
            entity['category'] = har_filename[:-4]

            # Only keep products with price per kg
        entities = list(filter(lambda entity: entity['price_per_kg'], entities))
        
        # Keep only interesting keys
        keys_to_keep = ['description', 'category', 'price', 'qty', 'price_per_kg']
        
        # Create dictionary with product id as key
        entities = { str(entity['id']) : { k: entity[k] for k in keys_to_keep } for entity in entities }
        
        return entities

    def create_and_store_items(self):

        # Check for items already present
        res = list(self.__db_manager.execute_query([{}]))
        if len(res) != 0:
            print(Fore.YELLOW + f'Info: items are already present in collection [{self.collection_name}]' + Style.RESET_ALL)
            return

        # Records creation
        entities = {}
        for har_filename in next(os.walk('har'))[2]:
            parsed_entities = self.__parse_har(har_filename) 
            print(list(parsed_entities.items())[:1])
            entities.update(parsed_entities)
        
        # Adding an universal product code
        for i, entity in enumerate(entities.values()):
            barcode = "".join([str(x) for x in [randrange(0,9) for x in range(12)]])
            entity['upc'] = f"{i}-{barcode}"
        # Saving to database
        self.__db_manager.insert_document(document = [elem for elem in entities.values()])


    def get_items(self, limit="") -> list:
        '''
        Retuns the items/goods that can be sold by retails
        - limit: how many items to retrieve. Default you get all items 
        '''
        res = self.__db_manager.execute_query([{},{ "_id": 0,}])
        res_copy = list(res)
        if len(res_copy) == 0:
           return []
        return res_copy[:limit] if limit else res_copy
        
        