import pika
import os
import google.protobuf.message
from google.protobuf import json_format
from urllib.parse import urlparse
from proker.consumer import ProkerConsumer
import json


class RabbitConsumer(ProkerConsumer):
    def __init__(self, queue_name: str, message_type: google.protobuf.message.Message, fn):
        self.queue_name = queue_name
        self.message_type = message_type
        self.fn = fn
        self.connect()

    def connect(self):
        parsed_uri = urlparse(os.getenv('RABBITMQ_URI'))
        connection_params = pika.ConnectionParameters(
            host=parsed_uri.hostname,
            port=parsed_uri.port,
            credentials=pika.PlainCredentials(parsed_uri.username, parsed_uri.password),
            heartbeat=60  
        )
        self.connection = pika.BlockingConnection(connection_params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name)

    def callback(self, ch, method, properties, body):
        try:
            message = self.message_type()
            message.ParseFromString(body)
            self.fn(json.loads(json_format.MessageToJson(message)))
        except Exception as e:
            print(f"Error processing message: {e}")

    def consume(self):
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)
        while True:
            try:
                print('Waiting for messages. To exit press CTRL+C')
                self.channel.start_consuming()
            except pika.exceptions.StreamLostError:
                print("Stream lost, reconnecting...")
                self.connect()  
                self.consume() 

    def close(self):
        self.connection.close()