
import json
from json import loads as json_loads

from colorama import init as colorama_init
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import explode_outer

from redis_manager import RedisManager


class TxnProcessor():

    spark = None

    def __init__(self) -> None:
        self.__redis_manager = RedisManager().get_instance()
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

        #Table with just the fields of each product
        new_table = exploding.select('products_new.*')

        velocity_table = new_table.groupBy('upc','description').sum('quantity').withColumnRenamed('sum(quantity)','daily sales velocity')
        x = velocity_table.select(["upc", "daily sales velocity"]).rdd.reduceByKey(lambda x, y: (x, y)).collect()
        self.spark.stop()
        return x
