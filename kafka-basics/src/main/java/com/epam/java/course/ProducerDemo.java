package com.epam.java.course;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
  private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

  public static void main(String[] args) {
    log.info("I am a kafka producer!.");

    // Create producer properties
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
    properties.setProperty("key.serializer", StringSerializer.class.getName());
    properties.setProperty("value.serializer", StringSerializer.class.getName());

    // Create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

    // Create a producer record
    ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>("demo_java", "Hello world");

    // Send data

    producer.send(producerRecord);

    // Flush and close the producer
    // Tell the producer to send all data and block until done -- synchronous
    producer.flush();
    // this line also runs .flush(), but added the flush for educational purposes.
    producer.close();
  }
}
