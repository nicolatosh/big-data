import pymongo 
from colorama import Fore, Style

class DatabaseManager:
    '''
    Class to manage CRUD operations on a database.
    Before doing any operation, user should connect to database
    and select the proper collection to operate with
    '''

    def __init__(self, user="admin", password="password", port="27017") -> None:
        self.__myclient = pymongo.MongoClient(f"mongodb://{user}:{password}@127.0.0.1:{port}/")

    def connect_to_database(self, collection_name:str ,database_name="default"):
        self.__database = self.__myclient[database_name]
        self.__collection = self.__database[collection_name]

    def select_collection(self, collection_name):
        # Check collection availability
        collections = self.__database.list_collection_names()
        if not collection_name in collections:
            print(Fore.YELLOW + '[DB] Warning: ' + Style.RESET_ALL + f"collection [{collection_name}] is empty. Will be created")
        self.__collection = self.__database[collection_name]

    # General Write operation
    def insert_document(self, document:list[dict]) -> bool:
        res = False
        if len(document) > 1:
            res = self.__collection.insert_many(document) != None
        else:
            res = self.__collection.insert_one(document[0]) != None
        return res
    
    # General Read operation
    def execute_query(self, query):
        collections = self.__database.list_collection_names()
        # Check collection availability
        if not self.__collection.name in collections:
            print(Fore.YELLOW + '[DB] Warning: ' + Style.RESET_ALL + f"collection [{self.__collection.name}] does not exist")
            return []

        return self.__collection.find(*query)
