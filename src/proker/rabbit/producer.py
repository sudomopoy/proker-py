from urllib.parse import urlparse
import os
import pika
import google.protobuf.message
from google.protobuf import json_format
from proker.producer import ProkerProducer

class RabbitProducer(ProkerProducer):
    def __init__(self, queue_name: str):
        self.queue_name = queue_name
        self.connect()

    def connect(self):
        parsed_uri = urlparse(os.getenv('RABBITMQ_URI'))
        connection_params = pika.ConnectionParameters(
            host=parsed_uri.hostname,
            port=parsed_uri.port,
            credentials=pika.PlainCredentials(parsed_uri.username, parsed_uri.password),
            heartbeat=60  # افزایش heartbeat
        )
        self.connection = pika.BlockingConnection(connection_params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

    def publish(self, message: google.protobuf.message.Message):
        try:
            serialized_message = message.SerializeToString()
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=serialized_message)
            print(f"Sent: {json_format.MessageToJson(message)}")
        except pika.exceptions.AMQPConnectionError:
            print("Connection lost, reconnecting...")
            self.connect()
            self.publish(message) 

    def close(self):
        self.connection.close()