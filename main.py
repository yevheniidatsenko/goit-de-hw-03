from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round as spark_round
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

# Initialize SparkSession with additional configurations
spark = SparkSession.builder \
    .appName("PySpark Data Analysis") \
    .master("local[*]") \
    .config("spark.hadoop.security.authentication", "simple") \
    .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow") \
    .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
    .getOrCreate()

# 1. Load datasets
users_df = spark.read.csv('data/users.csv', header=True, inferSchema=True)
purchases_df = spark.read.csv('data/purchases.csv', header=True, inferSchema=True)
products_df = spark.read.csv('data/products.csv', header=True, inferSchema=True)

# Display the first 10 rows for each table
print(Fore.CYAN + "Users Table:")
users_df.show(10)

print(Fore.CYAN + "Purchases Table:")
purchases_df.show(10)

print(Fore.CYAN + "Products Table:")
products_df.show(10)

# Display the number of rows before removing null values
print(Fore.YELLOW + 'Number of rows before removing null values')
print(Fore.GREEN + "Users:", users_df.count())
print(Fore.GREEN + "Purchases:", purchases_df.count())
print(Fore.GREEN + "Products:", products_df.count())

print(' ')

# 2. Remove null values
users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()

print(' ')

# Display the number of rows after removing null values
print(Fore.YELLOW + 'Number of rows after removing null values')
print(Fore.GREEN + "Users:", users_df.count())
print(Fore.GREEN + "Purchases:", purchases_df.count())
print(Fore.GREEN + "Products:", products_df.count())

print(' ')

# 3. Calculate the total purchase amount for each product category
print(Fore.CYAN + 'Calculating total purchase amount for each product category...')
purchases_products_df = purchases_df.join(products_df, "product_id", "inner")
full_data_df = purchases_products_df.join(users_df, "user_id", "inner")

full_data_df = full_data_df.withColumn("total_purchase", col("quantity") * col("price"))
total_purchase_per_category = full_data_df.groupBy("category").agg(
    spark_round(spark_sum("total_purchase"), 2).alias("total_purchase_sum")
)

print(Fore.YELLOW + 'Total purchase amount by product category:')
total_purchase_per_category.show()

# 4. Total purchase amount by product category for the age group 18 to 25 inclusive
print(Fore.YELLOW + 'Total purchase amount by product category for the age group 18 to 25 inclusive:')
age_18_25_df = full_data_df.filter((col("age") >= 18) & (col("age") <= 25))
purchase_per_category_18_25 = age_18_25_df.groupBy("category").agg(
    spark_round(spark_sum("total_purchase"), 2).alias("total_purchase_sum_18_25")
)
purchase_per_category_18_25.show()

# 5. Share of purchases for each product category out of total expenses for the age group 18 to 25
print(Fore.YELLOW + 'Share of purchases for each product category out of total expenses for the age group 18 to 25:')
total_purchase_18_25 = purchase_per_category_18_25.agg(
    spark_sum("total_purchase_sum_18_25").alias("total_sum_18_25")
).collect()[0]["total_sum_18_25"]

purchase_share_18_25 = purchase_per_category_18_25.withColumn(
    "percentage_of_total_18_25",
    spark_round((col("total_purchase_sum_18_25") / total_purchase_18_25) * 100, 2)
)
purchase_share_18_25.show()

# 6. Top 3 product categories with the highest spending percentage by consumers aged 18 to 25
print(Fore.YELLOW + 'Top 3 product categories with the highest spending percentage by consumers aged 18 to 25:')
top_3_categories_18_25 = purchase_share_18_25.orderBy(col("percentage_of_total_18_25").desc()).limit(3)
top_3_categories_18_25.show()