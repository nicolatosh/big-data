
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, explode_outer, from_json)
from pyspark.sql.types import StructType,StructField, StringType

"""
This file is worki-in-progress not part of the final project version.
It is an example of NRT processing with spark streaming
"""

bootstrap_servers = 'localhost:29092'
topic = "como.*"

spark = SparkSession \
        .builder \
        .appName("NrtProcessor") \
        .config("spark.sql.debug.maxToStringFields", "100") \
        .getOrCreate()

multiline_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribePattern", topic) \
        .load()


schema = StructType([ 
        StructField("upc",StringType(),True), 
        StructField("description",StringType(),True), 
        StructField("price",StringType(),True), 
        StructField("quantity",StringType(), True)
])


exploding = multiline_df.select(from_json(col("value").cast("string"), schema))
exploding = exploding.withColumn("products_new", explode_outer("shopping_list"))
exploding = exploding.withColumn("products", explode_outer("products_new"))

#Table with the fields of the transaction and the fields exploded from shopping_list
table = exploding.select(['client_id','date','txn_id','total_cost','products.*'])

velocity_table = table.groupBy('upc','description').sum('quantity').withColumnRenamed('sum(quantity)','daily sales velocity')
x = velocity_table.select(["upc", "daily sales velocity"]).rdd.reduceByKey(lambda x, y: (x, y)).collect()


query = exploding \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()