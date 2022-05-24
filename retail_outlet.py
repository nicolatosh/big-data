
import re
from requests import get
from colorama import init, Fore, Style

API_RETAILS = 'https://www.punti-vendita.com/esselunga{}.htm'

class RetailBuilder:
    """
    Class to manage the creation of shops/retails
    """

    __available_retails = {}

    def __init__(self) -> None:
        self.__download_retails()
    
    def __download_retails(self) -> list:

        # Extracting cities 
        # Regex extracts the available cities stores
        regex = r"\<a href=\"esselunga\-(.*?).htm\">"
        f = open('esselunga.htm', 'r')
        match = re.findall(regex, f.read())
        if not match:
            print(Fore.RED + 'Error: ' + Style.RESET_ALL + "failed to parse retails list")
            return 

        for elem in match:
            self.__available_retails[str(elem)] = API_RETAILS.format('-' + elem)

    def show_available_cities(self):
        print(Fore.GREEN + 'Available cities:' + Style.RESET_ALL)
        for city in self.__available_retails:
            print(f" {city}")

    def create_retails_by_city(self, city: str) -> None:
        if city.lower() not in map(lambda x : x.lower(), self.__available_retails):
            print(Fore.RED + 'Error: ' + Style.RESET_ALL + f"city: [{city}] not available")
            return

        # Downloading retails per city
        # Regex below extracts tuples:
        # 1. shop name
        # 2. address
        # 3. phone number
        regex = r"<strong>(.*)</strong>|Mappa.*\n\s+(.*)<br />|Telefono:(.*\d+)"
        f = None
        try:
            f = open(f'esselunga-{city}.htm', 'r')
        except FileNotFoundError:
            print(Fore.RED + 'Error: ' + Style.RESET_ALL + f"could not retrieve shops for city [{city}]. Using default")
            f = open(f'esselunga-como-default.htm', 'r')

        match = re.findall(regex, f.read())
        if not match:      
            print(Fore.RED + 'Error: ' + Style.RESET_ALL + "failed to parse city retails info")
            return

        # From regex tuples create the retail element
        for elem in [(match[i:i+3]) for i in range(0, len(match), 3)]:
            retail = {"name": elem[0][0], "address": elem[1][1], "phone": elem[2][2]}
            print(retail)

if __name__ == "__main__":
    init()
    print(Fore.GREEN + 'Retail script started ' + Style.RESET_ALL)
    retail = RetailBuilder()
    retail.show_available_cities()
    retail.create_retails_by_city('Chiasso')