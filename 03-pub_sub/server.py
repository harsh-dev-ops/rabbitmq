import pika
import time

class PikaServer(object):
    
    def __init__(self, ):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='127.0.0.1')
        )
        self.channel = self.connection.channel()
        self.queue_result = self.declare_queue(queue='', durable=True)
    
    def declare_queue(self, queue:str, durable:bool, *args, **kwargs):
        return self.channel.queue_declare(queue=queue, durable=durable)
    
    def declare_exchange(self, exchange:str, exchange_type:str, *args, **kwargs):
        return self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)
    
    def bind_queue(self, exchange:str, *args, **kwargs):
        return self.channel.queue_bind(exchange=exchange, queue=self.queue_result.method.queue)
    
    def callback(self, ch, method, properties, body):
        print(f" [x] {body.decode()}")
        
    def consume(self, *args, **kwargs):
        
        self.channel.basic_consume(
            queue=self.queue_result.method.queue,
            on_message_callback=self.callback,
            auto_ack=True,
        )
        self.channel.start_consuming()
    
    def disconnect(self):
        self.connection.close()
    

try:
    server = PikaServer()
    
    server.declare_exchange('logs', 'fanout')
    
    # server.declare_queue('', True)
    
    server.bind_queue(exchange='logs')
    
    server.consume()
    
except KeyboardInterrupt:
    print('\n[x] Disconnecting...')
    server.disconnect()
