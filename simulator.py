from retail_items import RetailItems
from retail_outlet import RetailBuilder
from colorama import init as colorama_init, Fore, Style

# Driver code
if __name__ == "__main__":
    colorama_init() 
    print(Fore.GREEN + 'Retail script started ' + Style.RESET_ALL)
    
    # Creating the retails 
    retail = RetailBuilder()
    retail.show_available_cities()
    selected_city = input("Select a city: ")
    retail.create_retails_by_city(selected_city)
    retails = retail.get_retails(selected_city)

    # Collecting available goods
    retail_items = RetailItems()
    retail_items.create_and_store_items()
    items = retail_items.get_items(limit=10)
    print(items)