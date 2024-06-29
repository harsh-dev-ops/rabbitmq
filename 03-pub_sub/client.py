import pika
import sys

class PikaClient(object):
    
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='127.0.0.1')
        )
        self.channel = self.connection.channel()
        
    def declare_queue(self, queue:str, durable:bool, *args, **kwargs):
        self.channel.queue_declare(queue=queue, durable=durable)
        
    def declare_exchange(self, exchange:str, exchange_type:str, *args, **kwargs):
        self.channel.exchange_declare(
            exchange=exchange,
            exchange_type=exchange_type
        )
    
    def produce(self, routing_key:str, message:str, *args, **kwargs):
        self.channel.basic_publish(
            exchange='logs',
            routing_key=routing_key,
            body=str(message),
        )
        
    def disconnect(self):
        self.connection.close()
    

client = PikaClient()

client.declare_exchange('logs', 'fanout')

message = ' '.join(sys.argv[1:]) or "Worker Queue Example!"

client.produce('task_queue', message)

client.disconnect()