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
        self.consumer = kafka.consumer.KafkaConsumer(bootstrap_servers="localhost:9092")
        self.producer = kafka.producer.KafkaProducer(bootstrap_servers="localhost:9092")
        print("done")

    def create_topic(self, topic):
        topics = []
        topics.append(kafka.admin.NewTopic(name=topic, num_partitions=1, replication_factor=1))
        self.admin.create_topics(new_topics=topic)

    def delete_topic(self, topic):
        topiccs = []
        topiccs.append(kafka.admin.NewTopic(name=topic, num_partitions=1, replication_factor=1))
        self.admin.delete_topics(topics=topic)

    def list_all_topics(self, host):
        topics = host.admin.list_topics()
        return topics

    def send_file_to_kafka_topic(self,topic,filename):
        file = open(filename,"r")
        str = file.read()
        str=json_serializer(str)
        print(str)
        self.producer.send(topic, value=b'hiiiiiiiiiiiiii')

    def kafka_consumer(self,topic,filename):
        file = open(filename, "r")
        str = file.read()
        str = json_serializer(str)
        topiccs = []
        topiccs.append(topic)
        print(self.consumer.bootstrap_connected())
        print(self.consumer.topics())
        output = self.consumer.poll()
        print(output)



    # def delete_topic(self,topic):

    # def kafka_consumer(self,host,topic,filename):


k = TestKafka()
# k.create_topic("hahha")
#print(k.list_all_topics(k))
#k.delete_topic(["TestTodasdpic"])
#print(k.list_all_topics(k))
#print(k.list_all_topics(k))
k.send_file_to_kafka_topic("rofl","file.txt")
#k.kafka_consumer("TestTopic","file.txt")