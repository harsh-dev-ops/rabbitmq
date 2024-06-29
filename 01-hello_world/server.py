from typing import Any
import pika

class PikaServer(object):
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
        self.channel = self.connection.channel()
        
    def callback(self, ch, method, props, body):
        print(f"Recieved: {body}")
    
    def __call__(self):
        self.channel.queue_declare('hello')
        self.channel.basic_consume(queue='hello', on_message_callback=self.callback)
        self.channel.start_consuming()
        
    def disconnect(self):
        self.connection.close() 

try:
    server = PikaServer()
    server()
except KeyboardInterrupt:
    print("\nDisconnecting.....")
    server.disconnect()
