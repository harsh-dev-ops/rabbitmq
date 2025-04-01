import json
import uuid
import pika
import sys
import dataclasses as dc

@dc.dataclass
class RequestSchema:
    num: int
    corr_id: str  # Now uses string type
    is_shutdown: bool = False

class FibonacciRpcClient:
    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='127.0.0.1'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='rpc_exchange', exchange_type='direct')
        
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )
        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, routing_key: str, payload: RequestSchema):
        self.response = None
        self.corr_id = payload.corr_id
        
        self.channel.basic_publish(
            exchange='rpc_exchange',
            routing_key=routing_key,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
                content_type='application/json'
            ),
            body=json.dumps(dc.asdict(payload))
        )
        
        while self.response is None:
            self.connection.process_data_events()
        
        return json.loads(self.response)
    
    def shutdown_server(self, routing_key: str, custom_corr_id: str):
        shutdown_payload = RequestSchema(
            num=0,
            corr_id=custom_corr_id,
            is_shutdown=True
        )
        return self.call(routing_key, shutdown_payload)

if __name__ == "__main__":
    num = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    custom_shutdown_id = "SHUTDOWN-1234"  # Your custom correlation ID
    
    client = FibonacciRpcClient()
    
    # Regular request
    # response = client.call("server1", RequestSchema(
    #     num=num,
    #     corr_id=str(uuid.uuid4())
    # ))
    # print(f"📦 Response: {response['data']}")
    
    # Shutdown request
    shutdown_response = client.shutdown_server("server1", custom_shutdown_id)
    print(f"🔌 Shutdown status: {shutdown_response['message']}")