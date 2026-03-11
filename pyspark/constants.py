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

    # --- 4. PROCESSING ---
    input_path_books = "gs://kestra-bucket-latypov/raw/Books.csv"
    output_path_books = "gs://kestra-bucket-latypov/pyspark_transformed/books" 