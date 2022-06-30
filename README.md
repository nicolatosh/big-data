### Warehouse Reorder system

<img src="https://user-images.githubusercontent.com/61838905/176528786-c88dbfe4-0ceb-4500-91f1-cfd34a450fcb.png" alt="Esselunga - Apps on Google Play" style="width: 50px;" />

<b>Esselunga supply chain</b> 

---

***Abstract***

The project is a big data system to manage automatic reorder of goods for supermarkets large supply chains. GDO is organized in a hierarchical structure in which a large supply chain provides items for many retailers. The system includes three different players: customers, retailers and the supplier, that exchange messages in a realistic fashion using Kafka message queue.
In short the system implements a pipeline for processing data coming from clients transactions.

---

##### Framework functionalities



- [x] Creation of clients and retail stores using scraping 

  > - https://www.esselungaacasa.it/ecommerce/
  > - https://www.punti-vendita.com/esselunga.htm

- [x] Creation of Inventories for each retail shop starting from a raw list of products

- [x] Simulation of customers e.g clients with random/configurable `transactions`(products with quantity and price)

- [x] Simulation of supply chain - retail interaction with `orders` and `restock` messages

- [x] Simulation of clients affluence *turnout* 

- [x] Batch processing: data analysis with Spark (Rop etc) daily based

- [ ] Forecasting 

- [ ] Data analysis within a time window

- [ ] Management of damaged goods, expired ones etc.

  

---

##### Technologies

+ Spark
+ Kafka in multi-broker setup
+ MongoDB, Acid transactions, replica-sets
+ Docker with compose
+ Redis



---

##### Architecture

![Esselunga_project drawio](https://user-images.githubusercontent.com/61838905/176533733-2c342f80-1883-4be3-8182-f263f1c4420c.png)



---



##### Project files

> 1. `database_manager`:  manages CRUD methods to interact with MongoDB cluster
> 2. `redis_manager`: enables management of Redis stack



> Spark related
>
> 1. `retail_kafka_consumer` Retail shop with its inventory, capable of sending orders and manage client's transactions
> 2. `customer_kafka_producer`: Customer that buys items form a shop (Kafka producer)
> 3. `supply_chain`: Kafka producer & consumer to simulate the large Inventory system that manages all the retailers
> 4. `transactions_kafka_consumer`: Kafka consumer that pushes valid transactions to Redis database.
> 5. `batch_transaction_processor`: PySpark, batch processing of transactions



> Folders
>
> 1. `default_data:` contains default data. "COMO" is the default city loaded, simulation can start with shops of Como
>
> 2. `docker-files`: contains the compose files to build up the framework containers stack
>
>    ***Note*** Because of readability there are three different *.yaml* files instead of a unified one



---

#### How to run


The project comes with a `dockerfile` that you can use to create an image an then a running container.
```bash
docker build -f .\dockerfile.yaml -t bigdataimg:latest . # creation of the image, might take some minutes
docker run --name bigdata -it --network="host" bigdataimg   # starting of container
```

At this point you should have interactive access to the docker container.
To run the other stacks you need, open a new shell and:

```bash
./start-stack.sh # This will bootstrap all the docker compose stacks required
./stop-stack.sh  # To stop the stack 
```

Now you have you host system with docker stack running.
To activate the simulation with default params just run the following command
within your interactive container
```
pyhton3 ./simulator.py # then select for "como"
```
You can tune the params of the simulator script within the script itself

> *NOTE*: if you want to use different cities you should set *TRUE* the *ONLINE_MODE* flag in *retail_outlet.py* 


##### Alternative for running
You can run the framework directly installing dependencies on your machine:

```bash
1 pip install -r requirements.txt 
2 ./start-stack.sh
3 python3 ./simulator.py

#Once simulator started, type "como" as city to run the experiment

To stop the stack run:
 ./stop-stack.sh
```
Framework has been tested with:

- Python 3.9
- Docker desktop 4.8.2
- Windows 10/11 & Ubuntu 20.1

---

#### Simulator

Simulation aims to reproduce working days of communication between parties.

How it works?

- Generates clients, retailers based on user city selection
- Simulation trial starts with tunable params
- Clients "living" in the city of the shop simulate transactions following a particular turnout function; given a time instant there will be a given amount of transactions to be produced
- At the end of each simulation simple statistics are printed out
- Simulation works with GUI applications

```python
#Tunable params
#You can modify them in the souce code of simulator.py
#When using "COMO" as standard default setup which has only 4 shops, leave the CUSTOMER_THREADS and PROCESSES as it is

INTERACTIVE_MODE = True # False to disable console views
NUM_TRIALS = 1 # simulation cycles
SIMULATION_TRIAL_TOTAL_TIME = 60 #seconds, you can set up to 60*60*12
CUSTOMERS_PER_CITY = 10 # random persons/consumers generate
CUSTOMERS_THREADS = 2 # How many pyhton threads should be used to act as clients
RETAILS_PROCESSES = 2 # How many pyhton threads should be used to act as shops
TIME_STEP = 0.1 # 0.1 hours = 6 min
TRANSACTIONS_PER_STEP = 1000 # 1000 txns within "TIME_STEP" minutes, can scale to Millions
```



---

*Additional stuff:*

##### List of Kafka topics

+ `<city>.<shopid>` each city retail pair has its own topic, used to send transactions towards a retailer
+ `<city>.<shopid>.validtxns`valid transactions goes there
+ `<shopid>.receiveorder` shops receive orders from the chain
+ `<shopid>.requestorder` retailer can request restock order to the chain

---

##### Messages

```python
transaction = {'txn_id': uuid, 'date': X, 'client_id': X, 'total_cost': X, 'shopping_list': [{'upc': Y, 'description': Y, 'quantity': Y} ...]}

order = {"order_id": uuid, "retail_id": X, "upc": X, "quantity": X}
                        
       
```

