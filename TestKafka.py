import kafka
import json


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


class TestKafka:

    def __init__(self):
        print("creating kafka...")
        self.admin = kafka.admin.KafkaAdminClient(bootstrap_servers="localhost:9092"
                                                  , client_id="www"
                                                  )
        print("done")

    def create_topic(self, topic):
        topics = []
        topics.append(kafka.admin.NewTopic(name=topic, num_partitions=1, replication_factor=1))
        try:
            self.admin.create_topics(new_topics=topics)
        except:
            print("An error occurred while creating a topic. A topic with that name may already exist.")

    def delete_topic(self, topic):
        topics = []
        topics.append(topic)
        self.admin.delete_topics(topics=topics, timeout_ms=2000)

    def list_all_topics(host):
        topics = host.admin.list_topics()
        return topics

    def send_file_to_kafka_topic(self, topic, filename):
        producer = kafka.producer.KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda x:
        json.dumps(x).encode('utf-8'))

        """consumer = kafka.consumer.KafkaConsumer(topic,
                                                bootstrap_servers=['localhost:9092'],
                                                auto_offset_reset='earliest',
                                                enable_auto_commit=True,
                                                group_id='my-group',
                                                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
                                                )"""

        file = open(filename, "r")
        str = file.read()
        producer.send(topic=topic, value=str)
        # consumer.poll()

    def kafka_consumer(self, topic, filename):
        consumer = kafka.consumer.KafkaConsumer(topic, bootstrap_servers=['localhost:9092'],
                                                auto_offset_reset='earliest',
                                                enable_auto_commit=True, consumer_timeout_ms=5000,
                                                value_deserializer=lambda x: json.loads(x.decode('utf-8')))
        file = open(filename, "w+")
        output = ""
        for message in consumer:
            output += message.value

        file.write(output)



# k = TestKafka()
# k.create_topic("tempTopic")
# print(k.list_all_topics())
# k.delete_topic(["TestTodasdpic"])
# print(k.list_all_topics(k))
# print(k.list_all_topics(k))
# k.delete_topic('rofl')
# print("all done!!!")
# print(k.list_all_topics())
# k.send_file_to_kafka_topic("top1","file.txt")
# k.kafka_consumer("top1","file20.txt")
