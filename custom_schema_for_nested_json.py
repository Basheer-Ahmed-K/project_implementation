# Databricks notebook source
from pyspark.sql.functions import explode_outer, col, from_json
import json
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.json("dbfs:/FileStore/project/vtex_test_data.json")
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

firstColumn = spark.read.json("dbfs:/FileStore/project/JSON_FirstColumn.json")
display(firstColumn)

# COMMAND ----------

firstColumn.printSchema()

# COMMAND ----------

my_part = spark.read.json("dbfs:/FileStore/project/myPart.json", multiLine=True)
my_part.printSchema()

# COMMAND ----------

seller_schema = StructType([
    StructField("fulfillmentEndpoint", StringType(), True),
    StructField("id", StringType(), True),
    StructField("logo", StringType(), True),
    StructField("name", StringType(), True)])
 
currency_format_info_schema = StructType([
    StructField("CurrencyDecimalDigits", LongType(), True),
    StructField("CurrencyDecimalSeparator", StringType(), True),
    StructField("CurrencyGroupSeparator", StringType(), True),
    StructField("CurrencyGroupSize", LongType(), True),
    StructField("StartsWithCurrencySymbol", BooleanType(), True)])
 
store_preferences_data_schema = StructType([
    StructField("countryCode", StringType(), True),
    StructField("currencyCode", StringType(), True),
    StructField("currencyFormatInfo", currency_format_info_schema, True),
    StructField("currencyLocale", LongType(), True),
    StructField("currencySymbol", StringType(), True),
    StructField("timeZone", StringType(), True)])
 
main_schema = StructType([
    StructField("sellers", ArrayType(StructType([
        StructField("element", seller_schema, containsNull=True)]), True), True),
    StructField("storePreferencesData", store_preferences_data_schema, True),
    StructField("subscriptionData", StringType(), True),
    StructField("taxData", StringType(), True)])

# COMMAND ----------

clientPreferencesData_schema = StructType([
    StructField('locale', StringType(), True),
    StructField('optinNewsLetter', BooleanType(), True)
])
itemMetadata_schema = StructType([
    StructField("Items", ArrayType(StructType([
        StructField("AssemblyOptions", ArrayType(
            StructType([
                StructField("Composition", StringType(), True),
                StructField("Id", StringType(), True),
                StructField("InputValue", StructType([
                    StructField("garatia-estendida", StructField([
                        StructField("Domain", ArrayType(
                            StructField("element", StringType(), True)
                        ), True)
                        StructField("MaximumNumberOfCharacters", LongType(), True)
                    ])),
                    StructField("takeback", StructType(), True)
                ]))
            ])
        ), True)
    ])
    ), True)
])




# COMMAND ----------


