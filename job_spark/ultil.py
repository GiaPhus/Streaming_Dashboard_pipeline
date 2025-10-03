from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from cassandra.cluster import Cluster
from pyspark.sql.window import Window

def sparkconnection():
    try:
        spark = SparkSession.builder \
            .appName("Spark Streaming") \
            .config("spark.cassandra.connection.host", "cassandra") \
            .config("spark.cassandra.connection.port", "9042") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        print("Spark Connection Successfully")
        return spark
    except Exception as e:
        print("Spark connection failed:", e)
        return None

def write_to_cassandra(df, table, keyspace):
    """
    Ghi DataFrame vào Cassandra
    """
    return (df.writeStream
       .format("org.apache.spark.sql.cassandra")
       .option("checkpointLocation", f"/tmp/checkpoints/{table}")
       .option("keyspace", keyspace)
       .option("table", table)
       .outputMode("append") 
       .start())

def kafka_sub(spark_conn,topic,schema):
    df_raw = spark_conn.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    df_value = df_raw.selectExpr("CAST(value AS STRING) as json_str")
    
    payload_schema = StructType(
        [
            StructField("before",schema),
            StructField("after",schema),
            StructField("op",StringType())
        ]
    )
    parsed = df_value.select(from_json(col("json_str"), payload_schema).alias("data"))
    df_after = parsed.select("data.after.*")

    return df_after
    
def connect_cassendra():
    try :
        cluster = Cluster(['cassandra'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        print(f"Could not create cassandra connection due to {e}")
        return None

def clean_dataframe(df: DataFrame, primary_key: str, required_cols: list) -> DataFrame:
    df_clean = df.dropna(subset=required_cols)
    df_clean = df_clean.dropDuplicates([primary_key])
    return df_clean


# def write_to_cassandra(df, table, keyspace):

#     (df.writeStream
#        .format("org.apache.spark.sql.cassandra")
#        .option("checkpointLocation", f"/tmp/checkpoints/{table}")
#        .option("keyspace", keyspace)
#        .option("table", table)
#        .outputMode("append")
#        .start())

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")


def create_tables(session):
    session.set_keyspace("spark_streams")

    session.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY,
            gender TEXT,
            title TEXT,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            dob_date TEXT,
            dob_age INT,
            registered_date TEXT,
            registered_age INT,
            phone TEXT,
            cell TEXT,
            nat TEXT
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS purchases (
            purchase_id TEXT PRIMARY KEY,
            user_id TEXT,
            product TEXT,
            unit_price DOUBLE,
            quantity INT,
            total DOUBLE,
            timestamp TIMESTAMP
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS logins (
            user_id TEXT PRIMARY KEY,
            username TEXT,
            password TEXT,
            salt TEXT,
            md5 TEXT,
            sha1 TEXT,
            sha256 TEXT
        );
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            user_id TEXT PRIMARY KEY,
            street_number TEXT,
            street_name TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            postcode TEXT,
            latitude TEXT,
            longitude TEXT,
            timezone_offset TEXT,
            timezone_desc TEXT
        );
    """)

    print("All tables created successfully!")
    
    
    

def kafka_sub_locations(spark_conn, topic):
    # Schema tương ứng với bảng locations
    locations_schema = StructType([
        StructField("user_id", StringType()),
        StructField("street_number", StringType()),
        StructField("street_name", StringType()),
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("country", StringType()),
        StructField("postcode", StringType()),
        StructField("latitude", StringType()),
        StructField("longitude", StringType()),
        StructField("timezone_offset", StringType()),
        StructField("timezone_desc", StringType())
    ])
    
    df_raw = spark_conn.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    df_value = df_raw.selectExpr("CAST(value AS STRING) as json_str")

    payload_schema = StructType([
    StructField("before", locations_schema),
    StructField("after", locations_schema),
    StructField("source", StructType([])),   # không cần fields chi tiết
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
    StructField("transaction", StructType([]))
        ])
    
    envelope_schema = StructType([
        StructField("schema", StructType([])),   # mình không cần chi tiết schema ở đây
        StructField("payload", payload_schema)
    ])
    parsed = df_value.select(from_json(col("json_str"), envelope_schema).alias("data"))
    df_after = parsed.select("data.payload.after.*")

    # Flatten và alias toàn bộ cột, chỉ lấy after, filter null
    df_after = parsed.select(
        col("data.payload.after.user_id").alias("user_id"),
        col("data.payload.after.street_number").alias("street_number"),
        col("data.payload.after.street_name").alias("street_name"),
        col("data.payload.after.city").alias("city"),
        col("data.payload.after.state").alias("state"),
        col("data.payload.after.country").alias("country"),
        col("data.payload.after.postcode").alias("postcode"),
        col("data.payload.after.latitude").alias("latitude"),
        col("data.payload.after.longitude").alias("longitude"),
        col("data.payload.after.timezone_offset").alias("timezone_offset"),
        col("data.payload.after.timezone_desc").alias("timezone_desc")
    ).filter(col("user_id").isNotNull())

    return df_after,df_value

def kafka_sub_purchases(spark_conn, topic):
    purschases_schema = StructType([
        StructField("purchase_id", StringType()),
        StructField("user_id", StringType()),
        StructField("product", StringType()),
        StructField("unit_price", DoubleType()),
        StructField("quantity", IntegerType()),
        StructField("total", DoubleType()),
        StructField("timestamp", TimestampType())
    ])
    df_raw = spark_conn.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()
        
    df_value = df_raw.selectExpr("CAST(value AS STRING) as json_str")
    payload_schema = StructType([
    StructField("before", purschases_schema),
    StructField("after", purschases_schema),
    StructField("source", StructType([])),   # không cần fields chi tiết
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
    StructField("transaction", StructType([]))
        ])
    
    envelope_schema = StructType([
        StructField("schema", StructType([])),   # mình không cần chi tiết schema ở đây
        StructField("payload", payload_schema)
    ])
    parsed = df_value.select(from_json(col("json_str"), envelope_schema).alias("data"))
    df_after = parsed.select("data.payload.after.*")
    
    df_after = parsed.select(
        col("data.payload.after.purchase_id").alias("purchase_id"),
        col("data.payload.after.user_id").alias("user_id"),
        col("data.payload.after.product").alias("product"),
        col("data.payload.after.unit_price").alias("unit_price"),
        col("data.payload.after.quantity").alias("quantity"),
        col("data.payload.after.total").alias("total"),
        col("data.payload.after.timestamp").alias("timestamp")
    ).filter(col("purchase_id").isNotNull())
    
    return df_after,df_value

def kafka_sub_users(spark_conn, topic):
    users_schema = StructType([
        StructField("user_id", StringType()),
        StructField("gender",StringType()),
        StructField("title",StringType()),
        StructField("first_name",StringType()),
        StructField("last_name",StringType()),
        StructField("email",StringType()),
        StructField("dob_date",StringType()),
        StructField("dob_age",IntegerType()),
        StructField("registered_date",StringType()),
        StructField("registered_age",IntegerType()),
        StructField("phone",StringType()),
        StructField("cell",StringType()),
        StructField("nat",StringType())
    ])
    
    df_raw = spark_conn.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()
    df_value = df_raw.selectExpr("CAST(value AS STRING) as json_str")

    payload_schema = StructType([
    StructField("before", users_schema),
    StructField("after", users_schema),
    StructField("source", StructType([])),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
    StructField("transaction", StructType([]))
        ])
    
    envelope_schema = StructType([
        StructField("schema", StructType([])),
        StructField("payload", payload_schema)
    ])
    parsed = df_value.select(from_json(col("json_str"), envelope_schema).alias("data"))
    df_after = parsed.select("data.payload.after.*")
    
    df_after = parsed.select(
        col("data.payload.after.user_id").alias("user_id"),
        col("data.payload.after.gender").alias("gender"),
        col("data.payload.after.title").alias("title"),
        col("data.payload.after.first_name").alias("first_name"),
        col("data.payload.after.last_name").alias("last_name"),
        col("data.payload.after.email").alias("email"),
        col("data.payload.after.dob_date").alias("dob_date"),
        col("data.payload.after.dob_age").alias("dob_age"),
        col("data.payload.after.registered_date").alias("registered_date"),
        col("data.payload.after.registered_age").alias("registered_age"),
        col("data.payload.after.phone").alias("phone"),
        col("data.payload.after.cell").alias("cell"),
        col("data.payload.after.nat").alias("nat")
    ).filter(col("user_id").isNotNull())
    
        
    return df_after,df_value


def kafka_sub_logins(spark_conn, topic):
    logins_schema = StructType([
        StructField("user_id", StringType()),
        StructField("username", StringType()),
        StructField("password", StringType()),
        StructField("salt", StringType()),
        StructField("md5", StringType()),
        StructField("sha1", StringType()),
        StructField("sha256", StringType())
    ])
    
    df_raw = spark_conn.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "broker:29092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()
    
    df_value = df_raw.selectExpr("CAST(value AS STRING) as json_str")
    payload_schema = StructType([
    StructField("before", logins_schema),
    StructField("after", logins_schema),
    StructField("source", StructType([])),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
    StructField("transaction", StructType([]))
        ])
    
    envelope_schema = StructType([
        StructField("schema", StructType([])),
        StructField("payload", payload_schema)
    ])
    parsed = df_value.select(from_json(col("json_str"), envelope_schema).alias("data"))
    df_after = parsed.select("data.payload.after.*")
    df_after = parsed.select(
        col("data.payload.after.user_id").alias("user_id"),
        col("data.payload.after.username").alias("username"),
        col("data.payload.after.password").alias("password"),
        col("data.payload.after.salt").alias("salt"),
        col("data.payload.after.md5").alias("md5"),
        col("data.payload.after.sha1").alias("sha1"),
        col("data.payload.after.sha256").alias("sha256")
    ).filter(col("user_id").isNotNull())
    return df_after,df_value
    