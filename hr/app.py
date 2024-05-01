from confluent_kafka import Consumer

# Configuration parameters for Confluent Cloud
bootstrap_servers = "pkc-12576z.us-west2.gcp.confluent.cloud:9092"  # Replace with your actual bootstrap servers
security_protocol = "SASL_SSL"
sasl_mechanism = "PLAIN"
sasl_username = "HKY4LRKCXXYGYENA"  # Replace with your Confluent Cloud API key
sasl_password = "PB8jDb6lgzmgv8v3GjTtjuKWQLY4MxO6XAa0KoJHym+d/7fAP2iv0PEkxYUCfT5Y"  # Replace with your Confluent Cloud API secret


# Create a consumer instance
consumer = Consumer({
    "bootstrap.servers": bootstrap_servers,
    "security.protocol": security_protocol,
    "sasl.mechanism": sasl_mechanism,
    "sasl.username": sasl_username,
    "sasl.password": sasl_password,
    "group.id": "my-group"  # Choose a unique group ID for your application
})

# Subscribe to the topic you want to consume
topic_name = "a1"  # Replace with the actual topic name
consumer.subscribe([topic_name])

try:
    while True:
        msg = consumer.poll(1.0)  # Wait for a message for up to 1 second
        if msg is None:
            continue

        if msg.error():
            print("Error:", msg.error())
        else:
            print("Received message:", msg.value())

except KeyboardInterrupt:
    pass

# Close the consumer
consumer.close()
