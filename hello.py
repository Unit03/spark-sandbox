from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType

schema = StructType([StructField("num", IntegerType(), True)])

conf = SparkConf().setAppName("foo").setMaster("spark://localhost:7077")
context = SparkContext(conf=conf)
spark = SparkSession(context)

# rdd = sc.parallelize(l)
# spark.createDataFrame(rdd).collect()

in_ = context.parallelize([(1, ), (2, ), (3, ), (4, )])
df = spark.createDataFrame(in_, schema)
# print(df.show())

# df = df.withColumn("num", df["num"] ** 3)
# out = df.map(lambda x: (x[0] ** 3,))

print(df.show())
# path = "hdfs://namenode:9000/user/hdfs/bar"
# path = "hdfs://172.27.1.5:8020/user/hdfs/bar"
path = "bar"
print("Writing to ", path)
# out.write.save(path)
df.write.csv(path, mode="overwrite", header=True)
# out.rdd.saveAsTextFile(path)
print("Written to ", path)

# print("Reading from ", path)
# df2 = spark.read.load(path + "/*", format="parquet")
# print("Loaded from", path)
# print(df2.show())
