# import os
# import time  # <--- Added this
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from constants import ENCODING_FIXES
# from pyspark.logger import PySparkLogger


# # --- 1. PATH LOGIC ---
# start = time.time()
# # Kestra passes the path to the key via this env var
# key_path = os.getenv("GCP_KEY_PATH", "gcp-key.json")

# # key_path = os.getenv("GCP_KEY_PATH")

# if not key_path or not os.path.exists(key_path):
#     if os.path.exists("../service-account.json"):
#         key_path = "../service-account.json"
#     # elif os.path.exists("../service-account.json"):
#     #     key_path = "../service-account.json"
#     else:
#         key_path = "gcp-key.json" # Fallback

# print(f"--- Using GCP Key from: {key_path} ---")


# # --- 2. SPARK SESSION ---
# spark = (SparkSession.builder
#     .appName("GCS CSV Transform")
#     .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
#     .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
#     .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_path)
#     .getOrCreate())

# logger = PySparkLogger.getLogger("BooksPipeline")
# # Add this line here:
# spark.sparkContext.setLogLevel("WARN")

# # --- 3. PROCESSING ---
# input_path = "gs://kestra-bucket-latypov/raw/Books.csv"
# # Save to 'spark_output' so Kestra captures it
# output_path = "gs://kestra-bucket-latypov/transformed" 

# print(f"--- Reading CSV: {input_path} ---")

# def process_books(spark):
#     logger.info(f"Starting Books Processing", input=input_path, output=output_path)
#     df = (
#         spark.read
#             .option("header", True)
#             .option("inferSchema", True)
#             .option("multiLine", True)
#             .option("quote", '"')             # Standard double quote
#             .option("escape", '"')            # In many CSVs, "" is the escape for "
#             .csv(input_path)
#     )
    
#     # --- 4. Explore ---
#     logger.info("Schema:", df.printSchema())

    
#     logger.info("CSV Loaded successfully", total_rows=df.count())
#     #print("Sample rows:")
#     df.show(5)

#     # Rename individual columns
#     df = (df.withColumnRenamed("Book-Title", "title")
#         .withColumnRenamed("Book-Author", "author")
#         .withColumnRenamed("Year-Of-Publication", "year")
#         .withColumnRenamed("Publisher", "publisher")
#         .withColumnRenamed("Image-URL-S", "image_url_small")
#         .withColumnRenamed("Image-URL-M", "image_url_medium")
#         .withColumnRenamed("Image-URL-L", "image_url_large")
#         )

#     df.show(5)

#     print(df.count())
#     print(df.select('ISBN').distinct().count())

#     for c in df.columns:
#         null_counts = df.filter(df[c].isNull()).count()
#         print(f'{c}: {null_counts}')

#     df = df.withColumn( "split_parts", F.split(F.col("title"), r'\";') )
#     print(df.count())
#     df.filter(F.size(F.col("split_parts")) > 1).show()

#     author_condition = (F.col("author").rlike(r'^\d{4}$'))

#     df = df.withColumn("publisher", F.when(author_condition, F.col("year")).otherwise(F.col("publisher"))) \
#                         .withColumn("title", F.when(author_condition, F.col("split_parts").getItem(0)).otherwise(F.col("title"))) \
#                         .withColumn("image_url_large", F.when(author_condition, F.col("image_url_medium")).otherwise(F.col("image_url_large"))) \
#                         .withColumn("year", F.when(author_condition, F.col("author")).otherwise(F.col("year"))) \
#                         .withColumn("author", F.when(author_condition, F.col("split_parts").getItem(1)).otherwise(F.col("author")))          
                                
#     print(df.count())
#     df.filter(F.size(F.col("split_parts")) > 1).show()

#     # 3. Apply encoding fixes to the column 'title'
#     for bad_str, good_str in ENCODING_FIXES.items():
#         df = df.withColumn("title", F.regexp_replace(F.col("title"), bad_str, good_str))

#     # 4. Apply all other cleanups
#     df = df.withColumn("title", F.translate(F.col("title"), '\\"', '')) \
#                         .withColumn("title", F.regexp_replace(F.col("title"), "&amp;", "&")) \
#                         .withColumn("title", F.regexp_replace(F.col("title"), "/", ", ")) \
#                         .withColumn("title", F.trim(F.regexp_replace(F.col("title"), "\\s+", " "))
#                     )

#     df = df.drop("split_parts")

#     df = df.withColumn("year", F.col("year").cast("int"))

#     df.printSchema()

#     df.write.mode("overwrite").option("header", "true").option("quote", '"').option("quoteAll", "true").option("escape", '"').csv(output_path)



# # --- 5. Keep Spark UI alive ---
# #print("Sleeping for 120 seconds so you can view the Spark UI at http://localhost:4040")
# try:
#     process_books(spark)
# except Exception as e:
#     print(e)
# #time.sleep(120)
# end = time.time()
# print(f"Elapsed time: {end - start:.2f} seconds")
# print(f'Spark version: {spark.version}')
# # --- 6. Stop Spark ---



# spark.stop()





import os

import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from constants import Config

# --- 1. CONFIGURE LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- 2. PATH LOGIC ---
start = time.time()
key_path = os.getenv("GCP_KEY_PATH", "gcp-key.json")

if not key_path or not os.path.exists(key_path):
    if os.path.exists("../service-account.json"):
        key_path = "../service-account.json"
    else:
        key_path = "gcp-key.json"



# --- 3. SPARK SESSION ---
spark = (SparkSession.builder
    .appName("GCS CSV Transform")
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", key_path)
    .getOrCreate())


logger.info(f"Using GCP Key from: {key_path}")
logger.info(f"Spark version: {spark.version}")
# Reduce Spark internal noise
spark.sparkContext.setLogLevel("WARN")

# --- 4. PROCESSING ---
# input_path = "gs://kestra-bucket-latypov/raw/Books.csv"
# output_path = "gs://kestra-bucket-latypov/pyspark_transformed/books" 
# input_path = Config.input_path_books
# output_path = Config.output_path_books

def process_books(spark, input_path, output_path):
    logger.info(f"Reading CSV from: {input_path}")
    
    df = (
        spark.read
            .option("header", True)
            .option("inferSchema", True)
            .option("multiLine", True)
            .option("quote", '"')
            .option("escape", '"')
            .csv(input_path)
    )
    
    logger.info("Schema loaded. Printing schema to stdout:")
    df.printSchema()

    row_count = df.count()
    logger.info(f"CSV Loaded successfully. Total rows: {row_count}")
    
    # df.show() outputs to stdout by default, which is fine for visual logs
    df.show(5)

    # Rename columns
    df = (df.withColumnRenamed("Book-Title", "title")
        .withColumnRenamed("Book-Author", "author")
        .withColumnRenamed("Year-Of-Publication", "year")
        .withColumnRenamed("Publisher", "publisher")
        .withColumnRenamed("Image-URL-S", "image_url_small")
        .withColumnRenamed("Image-URL-M", "image_url_medium")
        .withColumnRenamed("Image-URL-L", "image_url_large")
        )

    isbn_distinct = df.select('ISBN').distinct().count()
    logger.info(f"Distinct ISBN count: {isbn_distinct}")

    # Null checks
    for c in df.columns:
        null_counts = df.filter(df[c].isNull()).count()
        if null_counts > 0:
            logger.warning(f"Column '{c}': {null_counts} null values found")
        else:
            logger.info(f"Column '{c}': No nulls")

    # Handling shifted rows
    df = df.withColumn("split_parts", F.split(F.col("title"), r'\";'))
    shifted_rows = df.filter(F.size(F.col("split_parts")) > 1).count()
    logger.info(f"Found {shifted_rows} rows with title splitting issues")

    author_condition = (F.col("author").rlike(r'^\d{4}$'))

    df = df.withColumn("publisher", F.when(author_condition, F.col("year")).otherwise(F.col("publisher"))) \
                        .withColumn("title", F.when(author_condition, F.col("split_parts").getItem(0)).otherwise(F.col("title"))) \
                        .withColumn("image_url_large", F.when(author_condition, F.col("image_url_medium")).otherwise(F.col("image_url_large"))) \
                        .withColumn("year", F.when(author_condition, F.col("author")).otherwise(F.col("year"))) \
                        .withColumn("author", F.when(author_condition, F.col("split_parts").getItem(1)).otherwise(F.col("author")))          
                                
    # Encoding fixes
    logger.info("Apply ENCODING_FIXES from constants")
    for bad_str, good_str in Config.ENCODING_FIXES.items():
        df = df.withColumn("title", F.regexp_replace(F.col("title"), bad_str, good_str))

    # Regex Cleanup
    df = df.withColumn("title", F.translate(F.col("title"), '\\"', '')) \
                        .withColumn("title", F.regexp_replace(F.col("title"), "&amp;", "&")) \
                        .withColumn("title", F.regexp_replace(F.col("title"), "/", ", ")) \
                        .withColumn("title", F.trim(F.regexp_replace(F.col("title"), "\\s+", " "))
                    )

    df = df.drop("split_parts").withColumn("year", F.col("year").cast("int"))

    logger.info(f"Writing transformed data to: {output_path}")
    #df.write.mode("overwrite").option("header", "true").option("quote", '"').option("quoteAll", "true").option("escape", '"').csv(output_path)
    df.write.mode("overwrite").parquet(output_path)
    logger.info("Write operation completed.")

# --- 5. EXECUTION ---
try:
    process_books(spark, input_path = Config.input_path_books, output_path = Config.output_path_books)
except Exception as e:
    logger.exception("An error occurred during the Spark transformation:")

end = time.time()


spark.stop()