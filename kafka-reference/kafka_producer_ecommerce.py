import random
from kafka import KafkaProducer
import time
import signal
import sys

class EcommerceDataProducer:
    def __init__(self, kafka_params, kafka_topic):
        self.kafka_params = kafka_params
        self.kafka_topic = kafka_topic
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_params['bootstrap.servers'],
            value_serializer=lambda v: str(v).encode('utf-8'),
            acks='all'
        )

        # Register a signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        print("Received signal to terminate. Flushing and closing Kafka producer.")
        self.producer.flush()
        self.producer.close()
        sys.exit(0)

    def generate_data(self):
        try:
            while True:
                for _ in range(10):  # Adjust the number of records as needed
                    customer_id = random.randint(101, 999)
                    gender = random.choice(['Male', 'Female'])
                    age = random.randint(18, 65)
                    city = random.choice(['New York', 'Los Angeles', 'Chicago', 'San Francisco', 'Miami', 'Houston'])
                    membership_type = random.choice(['Gold', 'Silver', 'Bronze'])
                    total_spend = round(random.uniform(400, 1500), 2)
                    items_purchased = random.randint(8, 19)
                    average_rating = round(random.uniform(3.0, 4.7), 1)
                    discount_applied = random.choice([True, False])
                    days_since_last_purchase = random.randint(12, 55)
                    satisfaction_level = 'Satisfied' if average_rating > 4.0 else 'Neutral' if average_rating == 4.0 else 'Unsatisfied'
                    message = f"{customer_id},{gender},{age},{city},{membership_type},{total_spend},{items_purchased},{average_rating},{discount_applied},{days_since_last_purchase},{satisfaction_level}"

                    self.produce_message(message)
                    print(f"Published CSV data: {message}")

                self.producer.flush()
                print("Sleeping for 20 seconds...")
                time.sleep(20)
        except Exception as e:
            print(f"Error: {e}")
        finally:
            self.producer.flush()
            self.producer.close()

    def produce_message(self, message):
        try:
            self.producer.send(self.kafka_topic, value=message).get()
        except Exception as e:
            print(f"Error producing message: {e}")

# Usage:
kafka_params = {
    'bootstrap.servers': 'localhost:9092'
}
kafka_topic = 'kafka-topic'

# Create an instance of EcommerceDataProducer
ecommerce_producer = EcommerceDataProducer(kafka_params, kafka_topic)

# Start generating and publishing ecommerce data to Kafka topic
ecommerce_producer.generate_data()
