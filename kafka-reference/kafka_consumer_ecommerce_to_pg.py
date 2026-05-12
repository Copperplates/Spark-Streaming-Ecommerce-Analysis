from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum, from_csv, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, BooleanType

class EcommerceDataAnalysis:
    def __init__(self, kafka_bootstrap_servers, kafka_topic):
        self.spark = SparkSession.builder.appName("EcommerceDataAnalysis") \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1') \
            .config('spark.jars', '/Users/nagwasaad/Downloads/postgresql-42.7.1.jar') \
            .getOrCreate()
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic

    #Function to define Schema of CSV publisher
    def define_schema(self):
        return "customer_id INT, gender STRING, age INT, city STRING, membership_type STRING, total_spend FLOAT, items_purchased INT, average_rating FLOAT, discount_applied BOOLEAN, days_since_last_purchase INT, satisfaction_level STRING"

    def start_streaming(self):
        postgres_properties = {
            "user": "postgres",
            "password": "root",
            "driver": "org.postgresql.Driver",
            "url": "jdbc:postgresql://localhost:5432/ecommerce_db"
        }

        def write_to_postgres(df):
            df.write.jdbc(url=postgres_properties["url"],
                          table="city_segments",
                          mode="append",
                          properties=postgres_properties)


        # Read data from Kafka topic
        kafka_stream_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", self.kafka_topic) \
            .load()


        # Convert the value column from Kafka into a string
        kafka_stream_df = kafka_stream_df.selectExpr("CAST(value AS string)")

        # Parse CSV data using the defined schema
        parsed_data_df = kafka_stream_df.select(from_csv(col("value"), self.define_schema()).alias("data")).select("data.*")
        # Create age groups
        ecommerce_df_age_groups = parsed_data_df.withColumn("age_group",
                                                        expr("""
                                                            CASE
                                                                WHEN age < 35 THEN 'Under 35'
                                                                WHEN age >= 35 AND Age <= 50 THEN 'Between 35-50'
                                                                ELSE 'Over 50'
                                                            END
                                                        """))

        # Group by Membership Type, Age Group, and City
        customer_segments = ecommerce_df_age_groups.groupBy("membership_type", "age_group", "city").agg({
            "total_spend": "mean",
            "items_purchased": "sum"
        })

        print("customer segments groups")
        # Show all segments in a single DataFrame
        # Write the results to the console for debugging purposes

        query1 = customer_segments.writeStream \
            .outputMode("update") \
            .foreachBatch(write_to_postgres) \
            .start()
        query1.awaitTermination()

        # Group by City separately
        city_segments = ecommerce_df_age_groups.groupBy("city").agg({
            "total_spend": "mean",
            "items_purchased": "sum"
        })
        print("city segments")

        query2 = city_segments.writeStream \
            .outputMode("update") \
            .foreachBatch(write_to_postgres) \
            .start()
        query2.awaitTermination()


        # Wait for the streaming query to terminate
        '''
         Which customers are at risk of not making future purchases based on their 
         Days Since Last Purchase and Satisfaction Level
        '''
        # Filtering customers at risk based on Days Since Last Purchase and Satisfaction Level
        at_risk_customers = parsed_data_df.filter((parsed_data_df["days_since_last_purchase"] > 30) & (parsed_data_df["satisfaction_level"] == "Unsatisfied"))
        # | customer_id | gender | age | city | membership_type | total_spend | items_purchased | average_rating | discount_applied | days_since_last_purchase | satisfaction_level |

        at_risk_customers = at_risk_customers.select(
            "customer_id",
            "city",
            "total_spend",
            "days_since_last_purchase",
            "satisfaction_level"
        )

        # query3 = at_risk_customers.writeStream \
        #     .outputMode("update") \
        #     .foreachBatch(write_to_postgres) \
        #     .start()
        # query3.awaitTermination()
        print("customer at risk")


if __name__ == "__main__":
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "kafka-topic"

    ecommerce_analysis = EcommerceDataAnalysis(kafka_bootstrap_servers, kafka_topic)
    ecommerce_analysis.start_streaming()
