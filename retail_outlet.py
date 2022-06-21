
import re
from requests import get
from colorama import Fore, Style
from database_manager import DatabaseManager
from uuid import uuid4

API_RETAILS = 'https://www.punti-vendita.com/esselunga{}.htm'
ONLINE_MODE = False
        
class RetailBuilder:
    """
    Class to manage the creation of shops/retails
    """
    database_collections_names = {"cities": "retails_cities", "retails": "retails_list"}
    __available_retails_cities = {}
    __manager = None

    def __init__(self, database_name="retail", collection_name="retails_cities") -> None:
        # Connection to database
        self.__manager = DatabaseManager()
        self.__manager.connect_to_database(database_name, collection_name)
        self.__manager.select_collection(collection_name)
        res = list(self.__manager.execute_query({})) 

        # Check if default data is available
        if len(res) == 0:
            print(Fore.YELLOW + 'Warning: ' + Style.RESET_ALL + f"default [{collection_name}] collection is empty. Downloading...")
            self.download_retails()    

            # Saving retials cities to database
            self.__manager.insert_document([self.__available_retails_cities])
        
        # Loading cities from database
        cities = self.__manager.execute_query([{},{ "_id": 0}])
        for elem in cities[0]:
            self.__available_retails_cities[str(elem)] = API_RETAILS.format('-' + elem)
        

    def download_retails(self) -> list:
        '''
        Downloads the cities in which supermarkets (Esselunga) are available
        '''

        # Extracting cities 
        # Regex extracts the available cities stores
        regex = r"\<a href=\"esselunga\-(.*?).htm\">"
        f = open('esselunga.htm', 'r')
        match = re.findall(regex, f.read())
        if not match:
            print(Fore.RED + 'Error: ' + Style.RESET_ALL + "failed to parse retails list")
            return 
        for elem in match:
            self.__available_retails_cities[str(elem)] = API_RETAILS.format('-' + elem)
        return self.__available_retails_cities.keys()

    def show_available_cities(self):
        print(Fore.GREEN + 'Available cities:' + Style.RESET_ALL)
    
        # Getting all the cities from database
        for i, city in enumerate(self.__available_retails_cities):
            print(f"{i}. {city}")

    def create_retails_by_city(self, city: str) -> None:
        '''
        Creates the list of retails given a city. It is saved into database collection
        '''
        city = city.lower()
        self.__manager.select_collection(self.database_collections_names['retails'])
        res = list(self.__manager.execute_query([{"city" : city},{ "_id": 0,}]))

        # Retails not found check
        if len(res) == 0:
            if city.lower() not in map(lambda x : x.lower(), self.__available_retails_cities):
                print(Fore.RED + 'Error: ' + Style.RESET_ALL + f"city [{city}] not available")
                return

            # Downloading retails per city
            # Regex below extracts tuples:
            # 1. shop name
            # 2. address
            # 3. phone number
            regex = r"<strong>(.*)</strong>|Mappa.*\n\s+(.*)<br />|Telefono:(.*\d+)"
            data = None
            if ONLINE_MODE:
                page = get(self.__available_retails_cities[city])
                data = page.text
            else:
                # Offline mode: data is taken from dump of online pages         
                try:
                    data = open(f'esselunga-{city}.htm', 'r')
                except FileNotFoundError:
                    print(Fore.RED + 'Error: ' + Style.RESET_ALL + f"could not retrieve shops for city [{city}]. Using default [como]")
                    city = "como"
                    data = open(f'esselunga-como.htm', 'r')
                data = data.read()

            match = re.findall(regex, data)
            if not match:      
                print(Fore.RED + 'Error: ' + Style.RESET_ALL + "failed to parse city retails info")
                return

            retails = []
            # From regex tuples create the retail element
            for elem in [(match[i:i+3]) for i in range(0, len(match), 3)]:
                retail = {"id": str(uuid4()), "name": elem[0][0], "address": elem[1][1], "phone": elem[2][2]}
                retails.append(retail)
            
            # Saving into database
            data = {"city": city, "retails": retails}
            self.__manager.insert_document([data])

        print(Fore.GREEN + f'Retails of city [{city}] ready' + Style.RESET_ALL)

    
    def create_all_retails(self):
        """
        Creates retails for all the available cities
        """
        for city in self.__available_retails_cities:
            self.create_retails_by_city(city)


    def get_retails(self, city="") -> list:
        '''
        Returns supermarkets/retails given a city
        '''
        city = city.lower()
        self.__manager.select_collection(self.database_collections_names['retails'])
        res = []
        if city:
            res = list(self.__manager.execute_query([{"city" : city}]))
        else:
            res = list(self.__manager.execute_query([{},{"_id": 0}]))
        if len(res) == 0:
            print(Fore.YELLOW + f'No retails found in the city [{city}]' + Style.RESET_ALL)
            return []
        return res[0]['retails'] 

