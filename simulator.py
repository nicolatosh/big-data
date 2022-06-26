from subprocess import CREATE_NEW_CONSOLE, PIPE, Popen, call
from sys import executable
from time import sleep

from colorama import Fore, Style
from colorama import init as colorama_init

from customer_kafka_producer import CustomerProducer
from customers_generator import CustomersGenerator
from retail_inventory import RetailInventory
from retail_items import RetailItems
from retail_outlet import RetailBuilder
from supply_chain import SupplyChain
from turnout_function import TurnoutFunction
from batch_transactions_processor import TxnProcessor
from signal import signal, SIGINT, SIGTERM
from os import name as os_name, kill as os_kill


def simple_printer(collection:list, item_name:str):
    print(Fore.GREEN + f"\nAvailable {item_name}:\n" + Style.RESET_ALL)
    for i, item in enumerate(collection):
        print(f'{i}. {item}' + Style.RESET_ALL)

def ctrlc_manager(signum, frame):
    print(Fore.GREEN + " \n== Terminating application ==" + Style.RESET_ALL)
    for p in consumers_processes:
        if os_name == 'nt':  # windows
            Popen("TASKKILL /F /PID {pid} /T".format(pid=p.pid))
        else:
            os_kill(p.pid, SIGTERM)
    print(Fore.GREEN + "Consumers stopped" + Style.RESET_ALL)
    print(Fore.GREEN + " \n== Exiting... ==" + Style.RESET_ALL)
    exit(0)

def end_simulation():
    print(Fore.CYAN + " \n== Ending simulation ==" + Style.RESET_ALL)
    for p in consumers_processes:
        if os_name == 'nt':  # windows
            Popen("TASKKILL /F /PID {pid} /T".format(pid=p.pid))
        else:
            os_kill(p.pid, SIGTERM)
    print(Fore.CYAN + "Consumers stopped" + Style.RESET_ALL)
    

# CTRL-C management
signal(SIGTERM, ctrlc_manager)
signal(SIGINT, ctrlc_manager)

consumers_processes = []
producers_threads = []

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
    inventory_builder.set_quantity()
    simple_printer(inventories, "inventories")

    NUM_DAYS = 5
    for j in range(NUM_DAYS):
        print(Fore.BLUE + f"=== SIMULATION DAY {str(j)} START ===" + Style.RESET_ALL)    

        # Starting simulation of transactions
        print(Fore.GREEN + "=== STARTING PRODUCERS ===" + Style.RESET_ALL)
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

        turnout = TurnoutFunction()
        for p in producers:
            # Sarting producers e.g customers
            _threads = p.create_producers_threads(turnout.get_affluence(), 20, 2)  
            producers_threads.extend(_threads)

        # Starting consumers
        for i, topic in enumerate(topics):
            group = f"{selected_city}.{i}"
            
            p = Popen([executable, "retail_kafka_consumer.py", "--s", *bootstrap_servers, "--c", f'{group}', "--t", f'{topic}'], creationflags=CREATE_NEW_CONSOLE)
            consumers_processes.append(p)

        # Starting batch transaction manager
        txn_manager = Popen([executable, "transactions_kafka_consumer.py", "-s", *bootstrap_servers, "-c", "transactions_group", "-t", *topics], creationflags=CREATE_NEW_CONSOLE)
        consumers_processes.append(txn_manager)

        # Starting supply chain
        # chain = Popen([executable, "supply_chain.py", "--s", *bootstrap_servers, "--i", "1", "--t", *topics], creationflags=CREATE_NEW_CONSOLE)
        chain = SupplyChain(1, bootstrap_servers, topics)
        chain.create_inventory()
        chain.create_chain_consumers()

        # Waiting for producers stream to end
        for t in producers_threads:
            if t is not None and t.is_alive():
                sleep(10)
                      

        print(Fore.BLUE + f"=== SIMULATION DAY {str(j)} END ===" + Style.RESET_ALL) 
        
        # At this point simulation ended
        # 1. End simulation by stpping processes/threads
        # 2. Start batch processing
        # 3. Execute supply chain update of ROP
        # 4. Chain check for sending orders to retails
        
        batch_processor = TxnProcessor()
        sales_velocity = batch_processor.calculate_daily_sales()
        chain.calculate_and_update_rop(sales_velocity)
        chain.send_orders()
        sleep(10)
        #end_simulation()
        exit()

        # - start batch processing etc
        # end_simulation()


    
    """
    per ogni città:
    - generare shop
    - generare clienti
    - prendere un gruppo random di clienti della città secondo una distribuzione
    - ogni cliente prende random oggeti da quello shop (configurabile)
    - esecuzione transazione e.g send al topic shop-città  
    """

