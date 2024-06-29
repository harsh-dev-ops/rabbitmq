import pika
import sys

class PikaClient(object):
    
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='127.0.0.1')
        )
        self.channel = self.connection.channel()
        
    def declare_queue(self, *args, **kwargs):
        return self.channel.queue_declare(**kwargs)

    def declare_exchange(self, *args, **kwargs):
        return self.channel.exchange_declare(**kwargs)
    
    def bind_queue(self, *args, **kwargs):
        return self.channel.queue_bind(**kwargs)
    
    def publish(self, *args, **kwargs):
        self.channel.basic_publish(**kwargs)
    
    def disconnect(self, *args, **kwargs):
        self.connection.close()
        

severity = sys.argv[1] if len(sys.argv) > 1 else 'info'
message = ' '.join(sys.argv[2:]) or 'Hello World!'

client = PikaClient()

client.declare_exchange(exchange='direct_logs', exchange_type='topic')

client.publish(routing_key=severity, body=str(message), exchange='direct_logs')

client.disconnect()