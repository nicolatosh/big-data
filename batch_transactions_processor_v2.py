
from redis_manager import RedisManager
from json import loads as json_loads
from colorama import init as colorama_init, Fore, Style
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import array, explode_outer, lit, floor, rand, when, from_json, max, col, round
from pyspark.sql.types import StructType,StructField, StringType, NumericType, MapType
import json

class TxnProcessor():
    """
    TODO 
    implement batch processing with spark
    """

    def __init__(self) -> None:
        self.__redis_manager = RedisManager().get_instance()

    def load_list(self, list_name:str) -> list:
        """
        Loads a whole list
        """
        return [json_loads(x) for x in  self.__redis_manager.lrange(list_name, 0, -1)]
        

if __name__ == "__main__":
    """
    Start the script to see saved transactions
    """
    colorama_init()
    processor = TxnProcessor()
    transactions = processor.load_list('transactions')
    #for i, elem in enumerate(processor.load_list("transactions")):
    #    print(Fore.GREEN + f"{i} => " + Style.RESET_ALL + str(elem['txn_id']))
    #print(transactions)

    with open("transactions.json", "w") as outfile:
        json.dump(transactions, outfile)
    
    spark = SparkSession \
          .builder \
          .appName("Spark_json_test") \
          .getOrCreate()
    
    
    #Load json file of transactions

    multiline_df = spark.read.option("multiline","true").option("inferSchema","true") \
         .json("transactions.json")
    #multiline_df.show()
    exploding = multiline_df.withColumn("products_new", explode_outer("shopping_list"))

    schema = StructType([ 
        StructField("upc",StringType(),True), 
        StructField("description",StringType(),True), 
        StructField("price",NumericType(),True), 
        StructField("quantity", NumericType(), True)
    ])

    #Table with the fields of the transaction and the fields exploded from shopping_list
    new_table = exploding.select(['client_id','date','txn_id','total_cost','products_new.*'])

    #Batch processing of the transactions for KPIs (Key Performance Indicators) calculation
    #Customer with the highest total cost
    customer_highest = new_table.groupBy('client_id').agg(max('total_cost')).sort(col('max(total_cost)').desc())
    customer_highest_first = customer_highest.limit(1)
    
    #Product sold the most (highest daily sales velocity)
    product_sold_most = new_table.groupBy('upc','description','price').sum('quantity').withColumnRenamed('sum(quantity)','daily sales velocity').sort(col('daily sales velocity').desc())
    product_sold_most_first = product_sold_most.limit(1)

    #Product with highest revenue (price*quantity)
    product_highest_revenue = product_sold_most.withColumn('revenue', product_sold_most['price']*product_sold_most['daily sales velocity'])
    product_highest_revenue = product_highest_revenue.select('*',round('revenue',2)).drop('revenue').withColumnRenamed('round(revenue, 2)', 'revenue')
    product_highest_revenue_first = product_highest_revenue.sort(col('revenue').desc()).limit(1)

    

