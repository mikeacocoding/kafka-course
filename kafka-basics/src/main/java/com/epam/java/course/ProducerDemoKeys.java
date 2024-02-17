package com.epam.java.course;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
  private static final Logger log =
      LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

  public static void main(String[] args) {
    log.info("I am a kafka producer with callback!.");

    // Create producer properties
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
    properties.setProperty("key.serializer", StringSerializer.class.getName());
    properties.setProperty("value.serializer", StringSerializer.class.getName());
    properties.setProperty("batch.size", "400");

    // Create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    // The record with the same key, should be sent to the same partition now that we are using the key.
    for (int j = 0; j < 2; j++) {
      for (int i = 0; i < 10; i++) {

        String topic = "demo_java";
        String key = "id_" + i;
        String value = "hello world " + i;

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

        // Send data

        producer.send(
            producerRecord,
            new Callback() {
              @Override
              public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // Executed every time a record successfully sent or an exception is thrown

                if (e == null) {
                  // The record was successfully sent.
                  log.info("Key: " + key + " | Partition: " + recordMetadata.partition());
                  log.info(
                      "[Details] Key: "
                          + key
                          + ", Topic: "
                          + recordMetadata.topic()
                          + ", Partition: "
                          + recordMetadata.partition()
                          + ", Offset: "
                          + recordMetadata.offset()
                          + ", Timestamp: "
                          + recordMetadata.timestamp());

                } else {
                  log.error("Something went wrong :C");
                }
              }
            });
      }

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        throw new RuntimeException(e);
        }
    }

    // Flush and close the producer
    // Tell the producer to send all data and block until done -- synchronous
    producer.flush();
    // this line also runs .flush(), but added the flush for educational purposes.
    producer.close();
  }
}
