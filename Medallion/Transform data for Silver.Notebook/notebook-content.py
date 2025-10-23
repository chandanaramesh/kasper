# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "93258e6c-6295-4f8c-8c77-6b0b33036d4c",
# META       "default_lakehouse_name": "Sales",
# META       "default_lakehouse_workspace_id": "71235bdf-644e-40cc-8b23-78beaff4f80e",
# META       "known_lakehouses": [
# META         {
# META           "id": "93258e6c-6295-4f8c-8c77-6b0b33036d4c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

# Import necessary PySpark modules
from pyspark.sql.types import *

# Create the schema for the table
orderSchema = StructType([
     StructField("SalesOrderNumber", StringType()),
     StructField("SalesOrderLineNumber", IntegerType()),
     StructField("OrderDate", DateType()),
     StructField("CustomerName", StringType()),
     StructField("Email", StringType()),
     StructField("Item", StringType()),
     StructField("Quantity", IntegerType()),
     StructField("UnitPrice", FloatType()),
     StructField("Tax", FloatType())
])

# Import all CSV files from the bronze folder of the lakehouse into a DataFrame
df = spark.read.format("csv").option("header", "true").schema(orderSchema).load("Files/bronze/*.csv")

# Display the first 10 rows of the DataFrame to preview the data
display(df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


#Adding new columns to the dataframe so you can track the source file name, 
#whether the order was flagged as being a before the fiscal year of interest, 
#and when the row was created and modified

from pyspark.sql.functions import when, lit, col, current_timestamp, input_file_name
    
# Add columns IsFlagged, CreatedTS and ModifiedTS
df = df.withColumn("FileName", input_file_name()) \
     .withColumn("IsFlagged", when(col("OrderDate") < '2019-08-01',True).otherwise(False)) \
     .withColumn("CreatedTS", current_timestamp()).withColumn("ModifiedTS", current_timestamp())
    
# Update CustomerName to "Unknown" if CustomerName null or empty
df = df.withColumn("CustomerName", when((col("CustomerName").isNull() | (col("CustomerName")=="")),lit("Unknown")).otherwise(col("CustomerName")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#### ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.
from pyspark.sql import SparkSession
from delta import *

spark = SparkSession.builder.appName("DeltaTableCreation").getOrCreate()

# Define the schema for the sales_silver table
schema = StructType() \
    .add("SalesOrderNumber", StringType()) \
    .add("SalesOrderLineNumber", IntegerType()) \
    .add("OrderDate", DateType()) \
    .add("CustomerName", StringType()) \
    .add("Email", StringType()) \
    .add("Item", StringType()) \
    .add("Quantity", IntegerType()) \
    .add("UnitPrice", FloatType()) \
    .add("Tax", FloatType()) \
    .add("FileName", StringType()) \
    .add("IsFlagged", BooleanType()) \
    .add("CreatedTS", DateType()) \
    .add("ModifiedTS", DateType())

# Create DeltaTable if not exists
delta_table = DeltaTable.createIfNotExists(spark) \
    .tableName("sales.sales_silver") \
    .addColumns(schema) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Update existing records and insert new ones based on a condition defined by the columns SalesOrderNumber, OrderDate, CustomerName, and Item.

from delta.tables import *
    
deltaTable = DeltaTable.forPath(spark, 'Tables/sales_silver')
    
dfUpdates = df
    
deltaTable.alias('silver') \
  .merge(
    dfUpdates.alias('updates'),
    'silver.SalesOrderNumber = updates.SalesOrderNumber and silver.OrderDate = updates.OrderDate and silver.CustomerName = updates.CustomerName and silver.Item = updates.Item'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "SalesOrderNumber": "updates.SalesOrderNumber",
      "SalesOrderLineNumber": "updates.SalesOrderLineNumber",
      "OrderDate": "updates.OrderDate",
      "CustomerName": "updates.CustomerName",
      "Email": "updates.Email",
      "Item": "updates.Item",
      "Quantity": "updates.Quantity",
      "UnitPrice": "updates.UnitPrice",
      "Tax": "updates.Tax",
      "FileName": "updates.FileName",
      "IsFlagged": "updates.IsFlagged",
      "CreatedTS": "updates.CreatedTS",
      "ModifiedTS": "updates.ModifiedTS"
    }
  ) \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Update existing records and insert new ones based on a condition defined by the columns SalesOrderNumber, OrderDate, CustomerName, and Item.

from delta.tables import *
    
deltaTable = DeltaTable.forPath(spark, 'Tables/sales_silver')
    
dfUpdates = df
    
deltaTable.alias('silver') \
  .merge(
    dfUpdates.alias('updates'),
    'silver.SalesOrderNumber = updates.SalesOrderNumber and silver.OrderDate = updates.OrderDate and silver.CustomerName = updates.CustomerName and silver.Item = updates.Item'
  ) \
   .whenMatchedUpdate(set =
    {
          
    }
  ) \
 .whenNotMatchedInsert(values =
    {
      "SalesOrderNumber": "updates.SalesOrderNumber",
      "SalesOrderLineNumber": "updates.SalesOrderLineNumber",
      "OrderDate": "updates.OrderDate",
      "CustomerName": "updates.CustomerName",
      "Email": "updates.Email",
      "Item": "updates.Item",
      "Quantity": "updates.Quantity",
      "UnitPrice": "updates.UnitPrice",
      "Tax": "updates.Tax",
      "FileName": "updates.FileName",
      "IsFlagged": "updates.IsFlagged",
      "CreatedTS": "updates.CreatedTS",
      "ModifiedTS": "updates.ModifiedTS"
    }
  ) \
  .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
