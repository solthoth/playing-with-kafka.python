import json

from kafka import KafkaProducer

def publish_message(kafka_producer, topic_name, key, value):
    try:
        key_bytes = __toBytes(key)
        value_bytes = __toBytes(value)
        kafka_producer.send(topic_name, key=key_bytes, value=value_bytes)
        print(f'Message published successfully to {topic_name}: key={key}')
    except Exception as ex:
        print(f'[ERROR] failed to publish message {key}'+str(ex))

def __toBytes(data):
    return bytes(data, encoding='utf-8')