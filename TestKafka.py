import kafka
import json
import kafka.errors as KafkaError


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


class TestKafka:

    def __init__(self):
        print("creating kafka...")
        try:
            self.admin = kafka.admin.KafkaAdminClient(bootstrap_servers="localhost:9092"
                                                  , client_id="www"
                                                  )
        except KafkaError.BrokerNotAvailableError:
            print("Error! No brokers available. Please make sure the connection is"
                  "correctly established.")
            raise



    def create_topic(self, topic):
        if self.check_if_topic_exists(topic) is True:
            print("Error! A topic with this name already exists.")
            raise KafkaError.TopicAlreadyExistsError("Error! The topic already exists.")
        else:
            topics = []
            topics.append(kafka.admin.NewTopic(name=topic, num_partitions=1, replication_factor=1))
            try:
                self.admin.create_topics(new_topics=topics)
            except KafkaError.BrokerNotAvailableError:
                print("Error! No brokers available. Please make sure the connection is"
                  "correctly established.")
                raise

    def check_if_topic_exists(self,topic):
        topics = self.admin.list_topics()
        for item in topics:
            if item == topic:
                return True
        return False


    def delete_topic(self, topic):
        if self.check_if_topic_exists(topic) is False:
            print("Error! The topic you're trying to delete does not exist.")
            raise KafkaError.InvalidTopicError("Topic does not exist.")
        else:
            topics = []
            topics.append(topic)
            try:
                self.admin.delete_topics(topics=topics, timeout_ms=2000)
            except KafkaError.BrokerNotAvailableError:
                print("Error! No brokers available. Please make sure the connection is"
                  "correctly established.")
                raise

    def list_all_topics(self):
        topics = self.admin.list_topics()
        return topics

    def send_file_to_kafka_topic(self, topic, filename):
        producer = kafka.producer.KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda x:
        json.dumps(x).encode('utf-8'))


        file = open(filename, "r")
        str = file.read()

        producer.send(topic=topic, value=str)
        producer.close()

        file.close()

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

        consumer.unsubscribe()
        consumer.close()

        file.close()

        return output



