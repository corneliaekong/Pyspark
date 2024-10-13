from pyspark import SparkConf, SparkContext
from pyspark.sql import functions as F

# Set up Spark context
conf = SparkConf().setAppName("MyRDDApp").set("spark.ui.port", "4041")
sc = SparkContext(conf=conf)

# Example: Create an RDD and perform transformations
rdd = sc.parallelize([1, 2, 3, 4, 5,1])

# Perform a transformation (lazy)
squared_rdd = rdd.map(lambda x: x ** 2)

# Perform an action to trigger the computation
result = squared_rdd.collect()

# Print the result
print(result)
total_rdd = rdd.map(lambda x: (x,1))
res = total_rdd.reduceByKey(lambda x,y :x+y).collect()
print(res)

# Stop the Spark context
sc.stop()
