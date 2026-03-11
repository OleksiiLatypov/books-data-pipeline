import os
import time  # <--- Added this
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- 1. PATH LOGIC ---
start = time.time()
# Kestra passes the path to the key via this env var
key_path = os.getenv("GCP_KEY_PATH", "gcp-key.json")

# key_path = os.getenv("GCP_KEY_PATH")

if not key_path or not os.path.exists(key_path):
    if os.path.exists("../service-account.json"):
        key_path = "../service-account.json"
    # elif os.path.exists("../service-account.json"):
    #     key_path = "../service-account.json"
    else:
        key_path = "gcp-key.json" # Fallback

print(f"--- Using GCP Key from: {key_path} ---")


# --- 2. SPARK SESSION ---
spark = (SparkSession.builder
    .appName("GCS CSV Transform")
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_path)
    .getOrCreate())


# Add this line here:
spark.sparkContext.setLogLevel("WARN")

# --- 3. PROCESSING ---
input_path = "gs://kestra-bucket-latypov/raw/Books.csv"
# Save to 'spark_output' so Kestra captures it
output_path = "gs://kestra-bucket-latypov/transformed" 

print(f"--- Reading CSV: {input_path} ---")

df = (
    spark.read
         .option("header", True)
         .option("inferSchema", True)
         .option("multiLine", True)
         .option("quote", '"')             # Standard double quote
         .option("escape", '"')            # In many CSVs, "" is the escape for "
         .csv(input_path)
)


# --- 4. Explore ---
print("Schema:")
df.printSchema()

print("Sample rows:")
df.show(5)

# Rename individual columns
df = (df
    .withColumnRenamed("Book-Title", "title")
    .withColumnRenamed("Book-Author", "author")
    .withColumnRenamed("Year-Of-Publication", "year")
    .withColumnRenamed("Publisher", "publisher")
    .withColumnRenamed("Image-URL-S", "image_url_small")
    .withColumnRenamed("Image-URL-M", "image_url_medium")
    .withColumnRenamed("Image-URL-L", "image_url_large")
)

df.show(5)




print(df.count())
print(df.select('ISBN').distinct().count())

for c in df.columns:
    null_counts = df.filter(df[c].isNull()).count()
    print(f'{c}: {null_counts}')


df = df.withColumn( "split_parts", F.split(F.col("title"), r'\";') )
print(df.count())
df.filter(F.size(F.col("split_parts")) > 1).show()



author_condition = (F.col("author").rlike(r'^\d{4}$'))

df = df.withColumn("publisher", F.when(author_condition, F.col("year")).otherwise(F.col("publisher"))) \
                    .withColumn("title", F.when(author_condition, F.col("split_parts").getItem(0)).otherwise(F.col("title"))) \
                    .withColumn("image_url_large", F.when(author_condition, F.col("image_url_medium")).otherwise(F.col("image_url_large"))) \
                    .withColumn("year", F.when(author_condition, F.col("author")).otherwise(F.col("year"))) \
                    .withColumn("author", F.when(author_condition, F.col("split_parts").getItem(1)).otherwise(F.col("author"))) \
                    
                     
                            
print(df.count())
df.filter(F.size(F.col("split_parts")) > 1).show()



# 1. Start with the dirty dataframe
# Create the 'title_clean' column once and we will keep updating IT.
# 2. Setup the fixes (Order is very important!)
# We put the 2-character sequences FIRST so they don't get broken by single-char fixes.
encoding_fixes = {
    # Catalan / Italian fixes
    "í²": "ò",
    "í¨": "è",
    "í¡": "à",
    
    # French / Double-encoded fixes
    "Ã\\?Â©": "é",
    "Ã\\?Â": "à", 
    "Ã\\?Â¨": "è",
    "Ã\\?Âª": "ê",
    "Ã\\?Â«": "ë",
    "Ã\\?Â´": "ô",
    "Ã\\?Â®": "î",
    "Ã\\?Â¯": "ï",
    "Ã\\?Â¹": "ù",
    "Ã\\?Â§": "ç",
    
    # Spanish fixes
    "Ã³": "ó",
    "Ã±": "ñ",
    "Ã¡": "á",
    "Ã©": "é",
    "Ã": "í"  # Keep this single character fix at the very bottom
}

# 3. Apply encoding fixes to the column 'title_clean'
for bad_str, good_str in encoding_fixes.items():
    df = df.withColumn(
        "title", 
        F.regexp_replace(F.col("title"), bad_str, good_str)
    )

# 4. Apply all other cleanups (Notice we continue using df_clean, NOT df_dirty)
df = df.withColumn(
    "title",
    # Step A: Remove backslashes and quotes
    F.translate(F.col("title"), '\\"', '')
).withColumn(
    "title",
    # Step B: Fix HTML entities
    F.regexp_replace(F.col("title"), "&amp;", "&")
).withColumn(
    "title",
    # Step C: Turn / into ,
    F.regexp_replace(F.col("title"), "/", ", ")
).withColumn(
    "title",
    # Step D: Collapse multiple spaces into one and trim
    F.trim(F.regexp_replace(F.col("title"), "\\s+", " "))
)


df = df.drop("split_parts")

df = df.withColumn("year", F.col("year").cast("int"))

df.printSchema()

df.write.mode("overwrite") \
    .option("header", "true") \
    .option("quote", '"') \
    .option("quoteAll", "true") \
    .option("escape", '"') \
    .csv(output_path)



# --- 5. Keep Spark UI alive ---
#print("Sleeping for 120 seconds so you can view the Spark UI at http://localhost:4040")

#time.sleep(120)
end = time.time()
print(f"Elapsed time: {end - start:.2f} seconds")
print(f'Spark version: {spark.version}')
# --- 6. Stop Spark ---
spark.stop()






# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, year, month

# # Start the session
# spark = SparkSession.builder.appName("GCS Parquet Transform").getOrCreate()

# input_path = "gs://kestra-demo-latypov/yellow_tripdata_2024-01.parquet"
# output_path = "gs://kestra-demo-latypov/yellow_taxi_top_10/"

# print(f"--- Processing: {input_path} ---")

# # 1. Read data
# df = spark.read.parquet(input_path)

# # 2. Transformations
# df_transformed = df.filter(col("fare_amount") > 10) \
#     .withColumn("pickup_year", year(col("tpep_pickup_datetime"))) \
#     .withColumn("pickup_month", month(col("tpep_pickup_datetime")))

# # 3. GET THE FIRST 10 ROWS ONLY
# df_top_10 = df_transformed.limit(10)
# print(df_top_10.show())
# # 4. Write to GCS
# # repartition(1) makes sure we get only 1 file in the folder
# print(f"--- Writing 10 rows to: {output_path} ---")

# df_top_10.repartition(1).write \
#     .mode("overwrite") \
#     .parquet(output_path)

# print("--- Write Successful ---")

#spark.stop()