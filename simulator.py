from retail_items import RetailItems
from retail_outlet import RetailBuilder
from customers_generator import CustomersGenerator
from colorama import init as colorama_init, Fore, Style

def simple_printer(collection:list, item_name:str):
    print(Fore.GREEN + f"\nAvailable {item_name}:\n" + Style.RESET_ALL)
    for i, item in enumerate(collection):
        print(f'{i}. {item}' + Style.RESET_ALL)

# Driver code
if __name__ == "__main__":
    colorama_init() 
    print(Fore.GREEN + 'Retail script started! ' + Style.RESET_ALL)
    
    # Creating the retails 
    retail_builder = RetailBuilder()
    retail_builder.show_available_cities()
    selected_city = input("Select a city: ")
    retail_builder.create_retails_by_city(selected_city)
    retails = retail_builder.get_retails(selected_city)
    simple_printer(retails, "retails")

    # Collecting available goods
    retail_items = RetailItems()
    retail_items.create_and_store_items()
    items = retail_items.get_items()
    simple_printer(items[:10], "items")

    food_categories = [category for category in set(map(lambda x: x["category"], items))]
    simple_printer(food_categories, "food categories")
  
    # Generating customers
    customers_gen = CustomersGenerator()
    customers = customers_gen.get_customers()
    simple_printer(customers, "customers")