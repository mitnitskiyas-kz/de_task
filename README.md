* How to perform deployment into Azure Devops

## Step 1 

>>> df = spark.read.format("csv").option("header","true").load("file:///c:/DE_TASK/data/olist_customers_dataset.csv")
>>> df.printSchema()
root
 |-- customer_id: string (nullable = true)
 |-- customer_unique_id: string (nullable = true)
 |-- customer_zip_code_prefix: string (nullable = true)
 |-- customer_city: string (nullable = true)
 |-- customer_state: string (nullable = true)