import json
import uuid
import pika
import sys


class FibonacciRpcClient():
    
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='127.0.0.1')
        )
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

        self.response = None
        self.corr_id = None
    
    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body
    
    def call(self, n):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        
        properties = pika.BasicProperties(
            reply_to=self.callback_queue,
            correlation_id=self.corr_id,
            content_type='applicatin/json'
        )

        body = json.dumps({"num": n})
        
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_queue',
            properties=properties,
            body = body
        )
        
        while self.response is None:
            self.connection.process_data_events(time_limit=None)
        
        return json.loads(self.response)
    
num = int(sys.argv[1]) if len(sys.argv) > 1 else 30

fibonacci_rpc = FibonacciRpcClient()

print(f" [x] Requesting fib({num})")
response = fibonacci_rpc.call(num)
print(f" [.] Got {response['result']}")