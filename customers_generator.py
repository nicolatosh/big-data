
from random import randint
from database_manager import DatabaseManager
from colorama import init as colorama_init, Fore, Style
import uuid

class CustomersGenerator():

    names_male = ["nicola", "giovanni", "luca", "elio", "luigi", "alfredo", "federico", "massimo", "mario"]
    names_female = ["elisa", "giovanna", "lucia", "elena", "matilde", "erica", "alessandra", "maria", "noemi", "giulia"]
    surnames = ["rossi", "bianchi", "modigliani", "giacomelli", "sestrieri", "caruso", "oppari", "ufaldi", "foscolo", "vecchi"]

    def __init__(self, database_name="retail", collection_name="customers", default_customers=10) -> None:
        """
        Creates some default customers if not already present into database
        """
        # Connection to database
        self.__db_manager = DatabaseManager()
        self.__db_manager.connect_to_database(database_name, collection_name)
        self.__db_manager.select_collection(collection_name)
        res = list(self.__db_manager.execute_query({})) 

        # Check if default data of customers is available
        if len(res) == 0:
            print(Fore.YELLOW + 'Warning: ' + Style.RESET_ALL + f"default [{collection_name}] collection is empty. Downloading...")
            customers = self.generate(default_customers)

            # Saving customers to database
            self.__db_manager.insert_document(customers)
        

    def generate(self, quantity=10, male=True, female=True) -> list[dict]:
        """
        Generates random customers e.g Mario Rossi etc. and extends the customer's database collection
        """        
        
        person_list = []
        mixed = False

        if ((not male) and (not female)):
            return []

        if male and female:
            mixed = True

        if male:
            person_list.extend(self.__generate_customers(quantity= quantity if not mixed else int(quantity/2), female= False))
        
        if female:
            person_list.extend(self.__generate_customers(quantity= quantity if not mixed else int(quantity/2), female= True))

        return person_list
        
        
    def __generate_customers(self, quantity=10, female=True) -> list[dict]:
        
        names_len = None
        surnames_len = len(self.surnames) - 1
        if female:
            names_len = len(self.names_female) - 1
        else:
            names_len = len(self.names_male) - 1

        persons = []
        for i in range(quantity):
            _name = randint(0, names_len)
            _surname = randint(0, surnames_len)

            p = {
                    'client_id': str(uuid.uuid4()),
                    'name': self.names_female[_name] if female else self.names_male[_name], 
                    'surname': self.surnames[_surname],
                    'fidaty_card': str(randint(0,1) > 0)
                }
            persons.append(p)
        return persons


    def get_customers(self, limit="") -> list:
        '''
        Retuns the customers 
        '''
        res = self.__db_manager.execute_query([{},{ "_id": 0,}])
        res = list(res.limit(limit if limit else 0))
        if len(res) == 0:
           return []
        return res