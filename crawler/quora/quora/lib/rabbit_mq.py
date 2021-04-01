import pika

def create_connection():
    credentials = pika.PlainCredentials('guest', 'guest')
    rabbit_mq = {}
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost', port=5672, heartbeat=600, blocked_connection_timeout=300, virtual_host='/', credentials=credentials))
    return connection