#!/usr/bin/env python
import threading, time

from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic


class Consumer(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='lab-python-kafka-brokers.kafka.svc.cluster.local:9092',
                                 auto_offset_reset='earliest',
                                 consumer_timeout_ms=1000)
        consumer.subscribe(['input'])

        while not self.stop_event.is_set():
            for message in consumer:
                print(message)
                if self.stop_event.is_set():
                    break

        consumer.close()


def main():
    # Create 'input' Kafka topic
    try:
        admin = KafkaAdminClient(bootstrap_servers='lab-python-kafka-brokers.kafka.svc.cluster.local:9092')

        topic = NewTopic(name='input',
                         num_partitions=1,
                         replication_factor=1)
        admin.create_topics([topic])
    except Exception:
        pass

    tasks = [
        Consumer()
    ]

    # Start threads of a publisher/producer and a subscriber/consumer to 'input' Kafka topic
    for t in tasks:
        t.start()

    time.sleep(10)

    # Stop threads
    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


if __name__ == "__main__":
    main()
