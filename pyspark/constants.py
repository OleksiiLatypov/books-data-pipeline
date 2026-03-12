class Config:
    ENCODING_FIXES = {
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

    EXCEPTIONS = [
            "ny",
            "nyc",
            "la",
            "dc",
            "sf",
            "usa",
            "uk",
            "uae",
            "eu",
            "u.a.e"
        ]

    # --- 4. PROCESSING ---
    INPUT_PATH_BOOKS = "gs://kestra-books-bucket-latypov/raw/Books.csv"
    OUTPUT_PATH_BOOKS = "gs://kestra-books-bucket-latypov/pyspark_transformed/books" 

    INPUT_PATH_USERS = "gs://kestra-books-bucket-latypov/raw/Users.csv"
    OUTPUT_PATH_USERS = "gs://kestra-books-bucket-latypov/pyspark_transformed/users" 

    INPUT_PATH_RATING = "gs://kestra-books-bucket-latypov/raw/Rating.csv"
    OUTPUT_PATH_RATING = "gs://kestra-books-bucket-latypov/pyspark_transformed/rating"
    # Define reusable CSV options
    CSV_OPTIONS = {
        "header": True,
        "inferSchema": True,
        "multiLine": True,
        "quote": '"',
        "escape": '"'
    }
        

# def split_location(df, colName, idx):
#     return df.withColumn(
#         colName,
#         F.when(
#             (F.get(F.col("split_parts"), idx).isNull()) |
#             (F.get(F.col("split_parts"), idx) == "") |
#             (F.get(F.col("split_parts"), idx) == "n/a") |
#             (F.get(F.col("split_parts"), idx) == ","),
#             F.lit("Unknown")
#         ).otherwise(
#             F.when(
#                 F.get(F.col("split_parts"), idx).isin(exceptions),
#                 F.upper(F.get(F.col("split_parts"), idx))
#             ).otherwise(
#                 # Handle hyphens and slashes properly
#                     F.initcap(
#                         F.regexp_replace(F.get(F.col("split_parts"), idx), "[-/]", " ")
#                     )
#                 )
#             )
#         )
    


# def null_check(df):
#     for c in df.columns:
#         null_counts = df.filter(df[c].isNull()).count()
#         if null_counts > 0:
#             logger.warning(f"Column '{c}': {null_counts} null values found")
#         else:
#             logger.info(f"Column '{c}': No nulls")