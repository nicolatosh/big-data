from customer_kafka_producer import CustomerProducer
from retail_items import RetailItems
from retail_outlet import RetailBuilder
from customers_generator import CustomersGenerator
from retail_inventory import RetailInventory
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
    items = retail_items.get_items(10)
    simple_printer(items, "items")

    food_categories = [category for category in set(map(lambda x: x["category"], items))]
    simple_printer(food_categories, "food categories")
  
    # Generating inventories
    inventory_builder = RetailInventory()
    inventory_builder.build_inventories(retails)
    inventories = list(inventory_builder.get_inventories())
    simple_printer(inventories, "inventories")

   

    # Starting simulation of transactions
    bootstrap_servers = ['localhost:29092', 'localhost:39092']
    topics = []
    producers = []
    for i, retail in enumerate(retails):
        topic = f"{selected_city}.{retail['id']}"
        topics.append(topic)

        # Generating customers for each city-shop
        customers_gen = CustomersGenerator()
        customers = customers_gen.get_customers()
        #simple_printer(customers, "customers")
        producers.append(CustomerProducer(bootstrap_servers, topic, inventories[i]['inventory'] ,customers))

    for p in producers:
        p.create_producers_threads(2)  
     


    """
    per ogni città:
    - generare shop
    - generare clienti
    - prendere un gruppo random di clienti della città secondo una distribuzione
    - ogni cliente prende random oggeti da quello shop (configurabile)
    - esecuzione transazione e.g send al topic shop-città  
    """

