# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DataType 

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "ccb92a9b-1a18-41f1-933a-c7759279bd4a",
"fs.azure.account.oauth2.client.secret": '_Du8Q~-~omzXI1mxsFvaVBJ_3WiH~RpkJszNwcBT',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/f34a8c6e-775d-484a-ae41-c344c39a4f91/oauth2/token"}


dbutils.fs.mount(
source = "abfss://tokyo-olympic-data-container@tokyoolympicdatarocky.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymicrocky",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs ls "/mnt/tokyoolymicrocky/transform-data/"

# COMMAND ----------

Athletes = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymicrocky/raw-data/Athletes.csv")
Coaches = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymicrocky/raw-data/Coaches.csv")
EntriesGender = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymicrocky/raw-data/EntriesGender.csv")
medelData = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymicrocky/raw-data/medelData.csv")
teams = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/tokyoolymicrocky/raw-data/teams.csv")

# COMMAND ----------

Athletes.show()

# COMMAND ----------

Athletes.printSchema()

# COMMAND ----------

Coaches.show()

# COMMAND ----------

Coaches.printSchema()

# COMMAND ----------

EntriesGender.show()

# COMMAND ----------

EntriesGender = EntriesGender.withColumn("Female",col("Female").cast(IntegerType()))\
                                .withColumn("Male",col("Male").cast(IntegerType()))\
                                .withColumn("Total",col("Total").cast(IntegerType()))

# COMMAND ----------

EntriesGender.printSchema()

# COMMAND ----------

medelData.show()

# COMMAND ----------

medelData.printSchema()

# COMMAND ----------

teams.show()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

gold_medal = medelData.orderBy("Gold",ascending=False).select ("Team_Country","Gold").show() 

# COMMAND ----------

Athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymicrocky/transform-data/Athletes")
teams.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymicrocky/transform-data/teams")
medelData.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymicrocky/transform-data/medelData")
EntriesGender.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymicrocky/transform-data/EntriesGender")
Coaches.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymicrocky/transform-data/Coaches")
