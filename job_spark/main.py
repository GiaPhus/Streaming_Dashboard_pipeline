from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from cassandra.cluster import Cluster
from pyspark.sql.window import Window
from ultil import *


spark = sparkconnection()
session = connect_cassendra()

create_keyspace(session)
create_tables(session)

users_df,_     = kafka_sub_users(spark, "mysql_server.staging_db.users")
purchases_df,_ = kafka_sub_purchases(spark, "mysql_server.staging_db.purchases")
logins_df,_    = kafka_sub_logins(spark, "mysql_server.staging_db.logins")
locations_df,_ = kafka_sub_locations(spark, "mysql_server.staging_db.locations")

# users_df     = clean_dataframe(users_df, "user_id", ["user_id", "email"])
# purchases_df = clean_dataframe(purchases_df, "purchase_id", ["purchase_id", "user_id"])
# logins_df    = clean_dataframe(logins_df, "user_id", ["user_id", "username"])
# locations_df = clean_dataframe(locations_df, "user_id", ["user_id", "city"])


# users_df.writeStream \
#     .format("console") \
#     .option("truncate", False) \
#     .start() \
#     .awaitTermination(10)

# query = write_to_cassandra(users_df, "users", "spark_streams")
# query.awaitTermination()

write_to_cassandra(users_df,     "users",     "spark_streams")
write_to_cassandra(purchases_df, "purchases", "spark_streams")
write_to_cassandra(logins_df,    "logins",    "spark_streams")
write_to_cassandra(locations_df, "locations", "spark_streams")

spark.streams.awaitAnyTermination()
