from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, lower, to_date, regexp_replace
import psycopg2
import os

spark = SparkSession.builder.appName("MetadataDrivenETL").getOrCreate()

conn = psycopg2.connect(
    user="postgres",
    host="localhost",
    password='2040',
    port='5432',
    database='metadataetl'
)
print("Database connected successfully!")
cursor = conn.cursor()

# 1. Fetch all sources
cursor.execute("SELECT source_id, file_path, delimeter FROM source_config")
sources = cursor.fetchall()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 2. Process EACH source (Notice the indentation!)
for source in sources:
    source_id, file_path, delimeter = source
    full_path = os.path.join(BASE_DIR, file_path)
    print(f"Processing source_id: {source_id} from {file_path}")
    print(f"Resolved path: {full_path}")

    try:
        # Pass the dynamic delimiter here
        df = spark.read.csv(full_path, header=True, inferSchema=True, sep=delimeter, nullValue = "na")

        # 3. Fetch and apply rules for THIS specific source
        # Fixed the SELECT statement to match the 3 variables you unpack
        query = "SELECT column_name, rule_type, rule_value FROM transformation_rules WHERE source_id = %s"
        cursor.execute(query, (source_id,))
        rules = cursor.fetchall()

        for rule in rules:
            column, rule_type, value = rule
            # breakpoint()
            if rule_type == "drop_nulls":
                df = df.dropna(subset=[column])
            elif rule_type == "drop_null":
                df = df.dropna(subset=[column])
            elif rule_type == "cast_double":
                df = df.withColumn(column, col(column).cast("double"))
            elif rule_type == "cast_int":
                df = df.withColumn(column, col(column).cast("int"))
            elif rule_type == "uppercase":
                df = df.withColumn(column, upper(col(column)))
            elif rule_type == "lowercase":
                df = df.withColumn(column, lower(col(column)))
            elif rule_type == 'rename':
                df = df.withColumnRenamed(column, value)
            elif rule_type == 'trim':
                df = df.withColumn(column, trim(col(column)))
            elif rule_type == "fill_null":
                df = df.fillna({column:value})
            elif rule_type == "to_date":
                df = df.withColumn(column, to_date(col(column), value))
            elif rule_type == 'remove_dollar':
                df = df.withColumn(column, regexp_replace(col(column), "\\$", ""))
            
        
        # 4. Fetch target destination
        query = "SELECT target_table, load_type FROM target_config WHERE source_id = %s"
        cursor.execute(query, (source_id,))
        target_table, load_type = cursor.fetchone()

        csv_output_path = f"data/csv_processed/{target_table}"
        
        df.coalesce(1).write.mode("overwrite").csv(csv_output_path, header=True)
        print("CSV file saved...")
        # Write the cleaned data
        
        parquet_output_path = f"data/processed/{target_table}"
        df.write.mode("overwrite").parquet(parquet_output_path)
        print("parquet file saved...")

        # 5. Log the run (Using CURRENT_TIMESTAMP in SQL)
        log_query = """
            INSERT INTO etl_run_log (source_id, run_time, status, rows_processed) 
            VALUES (%s, CURRENT_TIMESTAMP, %s, %s)
        """
        cursor.execute(log_query, (source_id, 'SUCCESS', df.count()))
    
    except Exception as e:
        # 1. Acknowledge the failure and reset the database transaction
        conn.rollback()
        
        # 2. Use %s to safely insert the error message (which contains quote marks)
        error_log_query = """
            INSERT INTO etl_run_log (source_id, run_time, status, rows_processed, error_message)
            VALUES (%s, CURRENT_TIMESTAMP, 'FAILED', 0, %s)
        """
        cursor.execute(error_log_query, (source_id, str(e)))
        
        # 3. Commit the failure log so it actually saves to the database
        conn.commit()
        print(f"Job failed and logged for source_id: {source_id}\n")
        print(e)
        
    else:
        # Commit the log insertion
        conn.commit()
        print(f"Successfully processed and logged source_id: {source_id}\n")

cursor.close()
conn.close()