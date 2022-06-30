from cmath import e
from os import kill as os_kill
from os import name as os_name
from signal import SIGINT, SIGTERM, signal
from subprocess import Popen
from sys import executable
from time import sleep

from colorama import Fore, Style
from colorama import init as colorama_init

from batch_transactions_processor import TxnProcessor
from customer_kafka_producer import CustomerProducer
from customers_generator import CustomersGenerator
from retail_inventory import RetailInventory
from retail_items import RetailItems
from retail_outlet import RetailBuilder
from supply_chain import SupplyChain
from turnout_function import TurnoutFunction
from os import name as os_name


### -- CONFIGURATION PARAMS -- ###
INTERACTIVE_MODE = True # False to disable console views
NUM_TRIALS = 1 # simulation cycles
SIMULATION_TRIAL_TOTAL_TIME = 10 #seconds
CUSTOMERS_PER_CITY = 10 # random persons/consumers
CUSTOMERS_THREADS = 2
RETAILS_PROCESSES = 2
TIME_STEP = 0.1 # 0.1 hours = 6 min
TRANSACTIONS_PER_STEP = 10 # 1000 txns within "TIME_STEP" minutes
### -- END PARAMS -- ###


def simple_printer(collection:list, item_name:str):
    """
    Helper for printing lists
    """
    print(Fore.GREEN + f"\nAvailable {item_name}:\n" + Style.RESET_ALL)
    for i, item in enumerate(collection):
        print(f'{i}. {item}' + Style.RESET_ALL)

def ctrlc_manager(signum, frame):
    """
    Manages CTRL + C termination
    """
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
    """
    Kills proceesses
    """
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
    print(Fore.GREEN + 'Starting creation of entites' + Style.RESET_ALL)
    
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
    inventories = list(inventory_builder.get_inventories([x['id'] for x in retails]))
    inventory_builder.set_quantity()
    simple_printer(inventories, "inventories")

    for j in range(NUM_TRIALS):
        print(Fore.BLUE + f"=== SIMULATION TRIAL {str(j)} START ===" + Style.RESET_ALL)    

        # Starting simulation of transactions
        print(Fore.GREEN + "=== STARTING PRODUCERS ===" + Style.RESET_ALL)
        bootstrap_servers = ['localhost:29092', 'localhost:39092'] # kafka brokers
        topics = []
        producers = [] 
        for i, retail in enumerate(retails):
            topic = f"{selected_city}.{retail['id']}"
            topics.append(topic)

            # Generating customers for each city-shop
            customers_gen = CustomersGenerator(default_customers=CUSTOMERS_PER_CITY)
            customers = customers_gen.get_customers()
            #simple_printer(customers, "customers")
            producers.append(CustomerProducer(bootstrap_servers, topic, inventories[i]['inventory'] ,customers))

        turnout = TurnoutFunction(time_step=TIME_STEP, txn_within_step=TRANSACTIONS_PER_STEP)
        for p in producers:
            # Sarting producers e.g customers
            # Here we specify:
            # 1. turnout function to simulate customers affluence
            # 2. how much time a simulation trial is (in seconds)
            # 3. number of threads
            _threads = p.create_producers_threads(turnout.get_affluence(), SIMULATION_TRIAL_TOTAL_TIME, CUSTOMERS_THREADS)  
            producers_threads.extend(_threads)

        # Starting consumers
        for i, topic in enumerate(topics):
            group = f"{selected_city}.{i}"
            # Consumer process on new pyhton shell
            if os_name != 'nt':
                p = Popen(['python3', "retail_kafka_consumer.py", "-s", *bootstrap_servers, "-c", f'{group}', "-t", f'{topic}', "-p", str(RETAILS_PROCESSES)])      
            else:
                from subprocess import CREATE_NEW_CONSOLE, CREATE_NO_WINDOW
                p = Popen([executable, "retail_kafka_consumer.py", "-s", *bootstrap_servers, "-c", f'{group}', "-t", f'{topic}', "-p", str(RETAILS_PROCESSES)], creationflags= CREATE_NEW_CONSOLE if INTERACTIVE_MODE else CREATE_NO_WINDOW)      
            consumers_processes.append(p)

        # Starting batch transaction manager
        if os_name != 'nt':
            txn_manager = Popen(['python3', "transactions_kafka_consumer.py", "-s", *bootstrap_servers, "-c", "transactions_group", "-t", *topics])
        else:
            txn_manager = Popen([executable, "transactions_kafka_consumer.py", "-s", *bootstrap_servers, "-c", "transactions_group", "-t", *topics], creationflags= CREATE_NEW_CONSOLE if INTERACTIVE_MODE else CREATE_NO_WINDOW)
        consumers_processes.append(txn_manager)

        # Starting supply chain
        chain = SupplyChain(1, bootstrap_servers, topics)
        chain.create_inventory()
        chain.create_chain_consumers()

        # Waiting for producers stream to end
        for t in producers_threads:
            if t is not None and t.is_alive():
                sleep(10)
                      

        print(Fore.BLUE + f"=== SIMULATION TRIAL {str(j)} END ===" + Style.RESET_ALL) 
        
        # At this point simulation ended
        # 1. End simulation by stpping processes/threads
        # 2. Start batch processing
        # 3. Execute supply chain update of ROP
        # 4. Chain check for sending orders to retails
        batch_processor = TxnProcessor()

        # batch processing
        stats = []
        sales_velocity = batch_processor.calculate_daily_sales()
        stats.append(sales_velocity)
        stat_customerbest = batch_processor.get_kpi_customer_highest()
        stats.append(stat_customerbest)
        stat_productbestsell = batch_processor.get_kpi_product_sold_the_most()
        stats.append(stat_productbestsell)
        stat_producthighest = batch_processor.get_kpi_product_highest_revenue()
        stats.append(stat_producthighest)

        # saving stats to db
        batch_processor.save_stats(stats, str(j))        

        print(Fore.GREEN + f" == STATISTICS TRIAL {str(j)} ==\n" + Style.RESET_ALL)
        print(Fore.GREEN + "Best customer:" + Style.RESET_ALL + f"{stat_customerbest[0]} spent: {stat_customerbest[1]}")
        print(Fore.GREEN + "Product sold the most:" + Style.RESET_ALL + f"{stat_productbestsell[0]} quantity: {stat_productbestsell[1]}")
        print(Fore.GREEN + "Product highest revenue:" + Style.RESET_ALL +f"{stat_producthighest[0]} revenue: {stat_producthighest[1]}")
        print(Fore.GREEN + "\n == END STATISTICS == \n" + Style.RESET_ALL)
        batch_processor.turn_off_spark()

        chain.calculate_and_update_rop(sales_velocity)
        chain.send_orders()
        sleep(5) 
        end_simulation()
    exit(0)


