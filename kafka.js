const { Kafka } = require("kafkajs");

function createKafkaClient() {
  const kafka = new Kafka({
    clientId: "my-app",
    brokers: ["localhost:9092"],
  });

  return kafka;
}

async function createTopic(topicName, partitionCount, replicationFactor) {
  const kafka = createKafkaClient();
  const admin = kafka.admin();

  await admin.connect();
  await admin.createTopics({
    topics: [
      {
        topic: topicName,
        numPartitions: partitionCount,
        replicationFactor: replicationFactor,
      },
    ],
  });

  await admin.disconnect();
}

async function publishMessage(topicName, message) {
  const kafka = createKafkaClient();
  const producer = kafka.producer();

  await producer.connect();
  await producer.send({
    topic: topicName,
    messages: [{ value: message }],
  });

  await producer.disconnect();
}

async function consumeMessages(topicName, callback) {
  const kafka = createKafkaClient();
  const consumer = kafka.consumer({ groupId: "my-group" });

  await consumer.connect();
  await consumer.subscribe({ topic: topicName });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      callback(message.value.toString());
    },
  });
}
