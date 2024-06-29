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
    
    
    def produce(self, queue:str, message:str, *args, **kwargs):
        self.channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=str(message),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent
            )
        )
        print(f'Message: {message}')
        
    def disconnect(self):
        self.connection.close()
    

client = PikaClient()

client.declare_queue('task_queue', True)

message = ' '.join(sys.argv[1:]) or "Worker Queue Example!"

client.produce('task_queue', message)

client.disconnect()