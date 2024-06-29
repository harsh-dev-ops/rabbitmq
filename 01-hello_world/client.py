from typing import Any
import pika

class PikaClient(object):
    
    def __init__(self,):
        self.connection =  pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
        self.channel =  self.connection.channel()
        
    def __call__(self, message) -> Any:
        self.channel.queue_declare(queue='hello')
        self.channel.basic_publish(exchange='',
                                   routing_key='hello',
                                   body=str(message)
                                   )
    
    def disconnect(self,):
        self.connection.close()
        


client = PikaClient()
client(message='Hello')
client.disconnect()