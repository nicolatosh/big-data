
import json
from json import loads as json_loads

from colorama import init as colorama_init
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, explode_outer, max, round)

from redis_manager import RedisManager
from database_manager import DatabaseManager
from datetime import datetime
from os import remove as os_remove

class TxnProcessor():
    """
    Batch processing
    """
    database_name = "batchstats"
    collection_name = "statistics"

    spark = None

    def __init__(self) -> None:
        self.__redis_manager = RedisManager().get_instance()
        self.__mongo_db_manager = DatabaseManager()
        self.__mongo_db_manager.connect_to_database(self.database_name, self.collection_name)
        self.spark = SparkSession \
            .builder \
            .appName("Spark_batch_processing") \
            .getOrCreate()

    def load_list(self, list_name:str) -> list:
        """
        Loads a whole list of transactions
        """
        elems = [json_loads(x) for x in  self.__redis_manager.lrange(list_name, 0, -1)]
        
        # List can be saved, moved etc...
        # By now it can be deleted
        self.__redis_manager.delete(list_name)
        return elems
    
        
    
    def calculate_daily_sales(self) -> list:
        """
        Calculate the number of sales per item by reading transactions
        """
        colorama_init()
        
        # Loading transactions
        transactions = self.load_list('transactions')

        # Using temporary file
        with open("transactions.json", "w") as outfile:
            json.dump(transactions, outfile)
        
        
        # Load json file of transactions
        multiline_df = self.spark.read.option("multiline","true").option("inferSchema","true") \
            .json("transactions.json")


        # Extracting shopping list
        exploding = multiline_df.withColumn("products_new", explode_outer("shopping_list"))

        #Table with the fields of the transaction and the fields exploded from shopping_list
        self.table = exploding.select(['client_id','date','txn_id','total_cost','products_new.*'])

        velocity_table = self.table.groupBy('upc','description').sum('quantity').withColumnRenamed('sum(quantity)','daily sales velocity')
        x = velocity_table.select(["upc", "daily sales velocity"]).rdd.reduceByKey(lambda x, y: (x, y)).collect()
        return x

    
    # Below functions:
    # Batch processing of the transactions for KPIs (Key Performance Indicators) calculation
        
    def get_kpi_customer_highest(self) -> tuple:
        """
        Customer with the highest total cost\n
        returns the total spent by client
        """
        customer_highest = self.table.groupBy('client_id').agg(max('total_cost')).sort(col('max(total_cost)').desc())
        customer_highest_first = customer_highest.limit(1)
        chf = customer_highest_first.select(["client_id","max(total_cost)"]).rdd.reduceByKey(lambda x, y: (x, y)).collect()
        return chf[0]

    def get_kpi_product_sold_the_most(self) -> tuple:
        """
        Product sold the most (highest daily sales velocity)\n
        returns sum of quantities
        """
        self.product_sold_most = self.table.groupBy('upc','description','price').sum('quantity').withColumnRenamed('sum(quantity)','daily sales velocity').sort(col('daily sales velocity').desc())
        _product_sold_most_first = self.product_sold_most.limit(1)
        psmf = _product_sold_most_first.select(["upc","daily sales velocity"]).rdd.reduceByKey(lambda x, y: (x, y)).collect()
        return psmf[0]

    def get_kpi_product_highest_revenue(self) -> tuple:
        """
        Product with highest revenue\n
        returns (price*quantity)
        """
        product_highest_revenue = self.product_sold_most.withColumn('revenue', self.product_sold_most['price']*self.product_sold_most['daily sales velocity'])
        product_highest_revenue = product_highest_revenue.select('*',round('revenue',2)).drop('revenue').withColumnRenamed('round(revenue, 2)', 'revenue')
        product_highest_revenue_first = product_highest_revenue.sort(col('revenue').desc()).limit(1)
        phrf = product_highest_revenue_first.select(["upc","revenue"]).rdd.reduceByKey(lambda x, y: (x, y)).collect()
        return phrf[0]
    

    def save_stats(self, data:list, trial:int) -> bool:
        """
        Save statistics data. Data will be timestamped.
        - data: whatever data in list 
        - trial: the trial number which data refers to
        """
        stats = {'timestamp': str(datetime.now().isoformat()), 'trial_number': trial, 'data': data}
        return self.__mongo_db_manager.insert_document([stats])

    def turn_off_spark(self):
        self.spark.stop()
        # removing temp file
        os_remove("transactions.json")
        return
