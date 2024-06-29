#!/usr/bin/env python
import pika
import json

def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


class FibonacciRpcServer(object):
    
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='127.0.0.1')
        )
        self.channel = self.connection.channel()
        self.declare_queue(queue='rpc_queue')
        
    def declare_queue(self, *args, **kwargs):
        return self.channel.queue_declare(**kwargs)
    
    def on_request(self, ch, method, props, body):
        body = json.loads(body)
        n = int(body['num'])

        print(f" [.] fib({n})")
        response = fib(n)
        
        properties = pika.BasicProperties(correlation_id = props.correlation_id, content_type='application/json')
        
        body = json.dumps({'result': response})

        ch.basic_publish(exchange='',
                        routing_key=props.reply_to,
                        properties=properties,
                        body=body)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
            
    def consume(self, *args, **kwargs):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue='rpc_queue',
            on_message_callback=self.on_request
        )
        self.channel.start_consuming()
    
    def disconnect(self):
        self.connection.close()
        

try:
    fibonacci_server = FibonacciRpcServer()
    fibonacci_server.consume()
except KeyboardInterrupt:
    print("\n[x]......Disconnected.......")
    fibonacci_server.disconnect()





