package com.epam.java.course;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
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

    // These for loops and the thread, is to demonstrate how the sticky partitioner is working,
    // so every batch of 30 records, will be on the same partition, then the thread sleeps, and creates
    // another batch of records that will be on a different partition.
    for (int j = 0; j < 10; j++) {
      // Create a producer record
      for (int i = 0; i < 30; i++) {
        ProducerRecord<String, String> producerRecord =
            new ProducerRecord<>("demo_java", "Hello world " + i);

        // Send data

        producer.send(
            producerRecord,
            new Callback() {
              @Override
              public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // Executed every time a record successfully sent or an exception is thrown

                if (e == null) {
                  // The record was successfully sent.
                  log.info(
                      "Received new metadata \n"
                          + "Topic: "
                          + recordMetadata.topic()
                          + "\n"
                          + "Partition: "
                          + recordMetadata.partition()
                          + "\n"
                          + "Offset: "
                          + recordMetadata.offset()
                          + "\n"
                          + "Timestamp: "
                          + recordMetadata.timestamp());
                } else {
                  log.error("Something went wrong :C");
                }
              }
            });

        try {
          Thread.sleep(500);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    // Flush and close the producer
    // Tell the producer to send all data and block until done -- synchronous
    producer.flush();
    // this line also runs .flush(), but added the flush for educational purposes.
    producer.close();
  }
}
