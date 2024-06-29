import pika
import sys

class PikaServer(object):
    
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='127.0.0.1')
        )
        self.channel = self.connection.channel()
        self.result = self.declare_queue(queue='', durable=True)
        
    def declare_queue(self, *args, **kwargs):
        return self.channel.queue_declare(**kwargs)

    def declare_exchange(self, *args, **kwargs):
        return self.channel.exchange_declare(**kwargs)
    
    def bind_queues(self, exchange:str, severties:list, *args, **kwargs):
        for severty in severties:
            self.channel.queue_bind(
                queue=self.result.method.queue,
                exchange=exchange,
                routing_key=severty
            )
    
            
    def consume(self, auto_ack:bool, *args, **kwargs):
        self.channel.basic_consume(
            queue=self.result.method.queue,
            auto_ack=auto_ack,
            on_message_callback=self.callback
        )
        self.channel.start_consuming()
        
    def callback(self, ch, method, properties, body):
        print(f" [x] {method.routing_key}:{body}")
    
    def disconnect(self):
        self.connection.close()


severities = sys.argv[1:]
if not severities:
    sys.stderr.write("Usage: %s [info] [warning] [error]\n" % sys.argv[0])
    sys.exit(1)

try:
    server = PikaServer()
    server.declare_exchange(exchange='direct_logs', exchange_type='topic')
    
    server.bind_queues(exchange='direct_logs', severties=severities)
    
    server.consume(auto_ack=True)
    
except KeyboardInterrupt:
    print('\n[x] Disconnecting...')
    server.disconnect()