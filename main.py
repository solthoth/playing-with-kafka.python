import asyncio
import json
from services import kafka_publisher
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic

def createTopics(kafka_servers):
    kafka_admin = KafkaAdminClient(bootstrap_servers=kafka_servers)
    topicNames = [NewTopic(name='streamers', num_partitions=1, replication_factor=1)]
    print('creating topics via admin client')
    kafka_admin.create_topics(new_topics=topicNames)
    if kafka_admin is not None:
        kafka_admin.close()

def publishStreamerData(kafka_servers):
    topicName = 'streamers'
    kafka_producer = KafkaProducer(bootstrap_servers=kafka_servers, api_version=(0, 10))
    streamers = [
        {
            "id": 0,
            "name": "aws",
            "url": "https://www.twitch.tv/aws"
        },
        {
            "id": 1,
            "name": "Tyrant_UK",
            "url": "https://www.twitch.tv/tyrant_uk"
        },
        {
            "id": 2,
            "name": "Nilaus",
            "url": "https://www.twitch.tv/nilaus"
        }
    ]
    print('publishing messages to streamers topic')
    for streamer in streamers:
        kafka_publisher.publish_message(kafka_producer, 
                                        topicName, 
                                        streamer['id'], 
                                        json.dumps(streamer))
    kafka_producer.flush()
    if kafka_producer is not None:
        kafka_producer.close()
    

async def main():
    servers=['kafka:9092']
    print('Creating topics')
    createTopics(servers)
    print('Publishing streamer data')
    publishStreamerData(servers)
    

if __name__ == "__main__":
    asyncio.run(main())