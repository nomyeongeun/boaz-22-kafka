from kafka import KafkaConsumer
import json

class Consumer:
    def __init__(self, brokers, topicName):
        self.consumer = KafkaConsumer(
            topicName,
            group_id="consumer-group-v3",
            bootstrap_servers=brokers,
            value_deserializer=lambda x: x.decode("utf-8"),
            api_version=(0, 11, 5),
        )

def receive_introduction(consumer):
    for message in consumer:
        data = json.loads(message.value.decode())
        print("---" * 20)
        print("ID          :  " + str(data["id"]))
        print("Nome        :  " + data["nome"])
        print("---" * 20)

if __name__ == "__main__":
    consumer = KafkaConsumer('fake-users', bootstrap_servers=['Kafka00Service:9092'], group_id='consumer-group-v3')
    receive_introduction(consumer)
