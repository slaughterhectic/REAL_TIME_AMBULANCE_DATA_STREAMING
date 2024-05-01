import tkinter as tk
from tkinter import ttk
from confluent_kafka import Producer
import json
import threading


class MedicalDataEntryUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Medical Data Entry")

        self.fields = [
            "Vital Signs Monitoring*", "Glasgow Coma Scale (GCS)*", "Blood Glucose Monitoring*",
            "Pulse Oximetry*", "Electrocardiogram (ECG)*", "Capnography*", "Cardiac Enzyme Testing*",
            "Pregnancy Testing", "Stroke Assessment Tools", "Pain Assessment"
        ]

        self.entries = {}
        for i, field in enumerate(self.fields):
            label_text = field[:-1] if field.endswith("*") else field
            label = ttk.Label(root, text=label_text)
            label.grid(row=i, column=0, padx=10, pady=5, sticky=tk.W)
            entry = ttk.Entry(root)
            entry.grid(row=i, column=1, padx=10, pady=5, sticky=tk.W)
            self.entries[field] = entry

        # Add button to submit data
        submit_button = ttk.Button(root, text="Submit", command=self.submit_data)
        submit_button.grid(row=len(self.fields), columnspan=2, pady=10)

        # Kafka Configurations
        self.kafka_topic = "a1"
        self.producer = Producer({
            'bootstrap.servers': "pkc-12576z.us-west2.gcp.confluent.cloud:9092",
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': "HKY4LRKCXXYGYENA",
            'sasl.password': "PB8jDb6lgzmgv8v3GjTtjuKWQLY4MxO6XAa0KoJHym+d/7fAP2iv0PEkxYUCfT5Y"
        })

    def submit_data(self):
        # Extract data from UI
        data = {}
        for field, entry in self.entries.items():
            data[field[:-1]] = entry.get()  # Remove * from field names

        # Separate data marked with * and unmarked data
        important_data = {k: v for k, v in data.items() if k.endswith("*")}
        less_important_data = {k: v for k, v in data.items() if not k.endswith("*")}

        # Start a new thread to send important data
        threading.Thread(target=self.send_important_data, args=(important_data,)).start()

        # Send less important data immediately
        self.send_data_to_confluent_cloud(less_important_data)

    def send_important_data(self, important_data):
        self.send_data_to_confluent_cloud(important_data)

    def send_data_to_confluent_cloud(self, data):
        try:
            self.producer.produce(self.kafka_topic, json.dumps(data), callback=self.delivery_callback)
            self.producer.poll(0)
        except Exception as e:
            print(f"Failed to send message to Kafka: {str(e)}")

    def delivery_callback(self, err, msg):
        if err:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to topic: {msg.topic()}")


def main():
    root = tk.Tk()
    app = MedicalDataEntryUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
