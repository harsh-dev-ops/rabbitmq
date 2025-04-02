#!/usr/bin/env python
import pika
import json
import time
import dataclasses as dc

@dc.dataclass
class RequestSchema:
    num: int
    corr_id: str  # String type
    is_shutdown: bool = False

def fib(n):
    if n == 0: return 0
    if n == 1: return 1
    return fib(n-1) + fib(n-2)

class FibonacciRpcServer:
    def __init__(self, routing_key: str):
        self.routing_key = routing_key
        self.should_stop = False
        
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='127.0.0.1'))
        self.channel = self.connection.channel()
        
        self.channel.exchange_declare(
            exchange='rpc_exchange', 
            exchange_type='direct'
        )
        self.channel.queue_declare(queue=routing_key)
        self.channel.queue_bind(
            exchange='rpc_exchange',
            queue=routing_key,
            routing_key=routing_key
        )
        
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=routing_key,
            on_message_callback=self.on_request
        )

    def on_request(self, ch, method, props, body):
        try:
            request = json.loads(body)
            req = RequestSchema(**request)
            
            if req.is_shutdown:
                print(f"Shutdown initiated by {req.corr_id}")
                response = {
                    'data': None,
                    'message': f'Server stopped by {req.corr_id}'
                }
                self.should_stop = True
            else:
                print(f"Processing fib({req.num})")
                # time.sleep(1000)
                response = {
                    'data': fib(req.num),
                    'message': 'Success'
                }
            
            ch.basic_publish(
                exchange='',
                routing_key=props.reply_to,
                properties=pika.BasicProperties(
                    correlation_id=props.correlation_id,
                    content_type='application/json'
                ),
                body=json.dumps(response)
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
            if self.should_stop:
                self.stop()
                
        except Exception as e:
            print(f"Error: {e}")
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

    def stop(self):
        print("Cleaning up resources...")
        if self.channel.is_open:
            self.channel.close()
        if self.connection.is_open:
            self.connection.close()
        print("Server shutdown complete")

    def start(self):
        print(f"Server listening on '{self.routing_key}'")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.stop()

if __name__ == "__main__":
    server = FibonacciRpcServer("server1")
    server.start()