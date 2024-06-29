import queue
import pika
import time

class PikaServer(object):
    
    def __init__(self, ):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='127.0.0.1')
        )
        self.channel = self.connection.channel()

    
    def declare_queue(self, queue:str, durable:bool, *args, **kwargs):
        self.channel.queue_declare(queue=queue, durable=durable)
    
    def callback(self, ch, method, props, body):
        print(f" [x] Received {body.decode()}")
        time.sleep(body.count(b'.'))
        print(" [x] Done")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def consume(self, queue:str, *args, **kwargs):
        self.channel.basic_qos(prefetch_count=1)
        
        self.channel.basic_consume(
            queue=queue,
            on_message_callback=self.callback
        )
        self.channel.start_consuming()
    
    def disconnect(self):
        self.connection.close()
    

try:
    server = PikaServer()
    server.declare_queue('task_queue', True)
    server.consume('task_queue')
except KeyboardInterrupt:
    print('\n[x] Disconnecting...')
    server.disconnect()
