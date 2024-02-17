package com.epam.java.course;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoCooperative {
  private static final Logger log =
      LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

  public static void main(String[] args) {
    // With this class, the Idea to see the behavior or rebalancing and groups, we can run and close
    // multiple instances of this class and it
    // will rebalance using every partition available
    // For example, 3 partitions, if we run it 1 time, the application will have 3 partitions
    // assigned.
    // then if we add another instance of the class, we will see that now the first instance will
    // have partitions 0 and 1, and the second instance partition 2.
    // if we run a third instance, then it will rebalance and every instance will have 1 partition.
    // if we run ProducerDemoKeys, it should produce data for 3 different partitions, and we should
    // see on the console of the consumer instances only it's own partitions records.

    log.info("I am a kafka Consumer!.");

    String groupId = "my-java-application";
    String topic = "demo_java";

    // Create producer properties
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

    // Create consumer configs
    properties.setProperty("key.deserializer", StringDeserializer.class.getName());
    properties.setProperty("value.deserializer", StringDeserializer.class.getName());
    properties.setProperty("group.id", groupId);
    properties.setProperty("auto.offset.reset", "earliest");
    properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
    // properties.setProperty("group.instance.id","...."); // strategy for static assignments

    // Create a consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    // Get a reference to the main thread
    final Thread mainThread = Thread.currentThread();

    // adding the shutdown hook
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                  mainThread.join();
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
              }
            });

    try {

      // Subscribe to a topic
      consumer.subscribe(Arrays.asList(topic));

      // Poll for data
      while (true) {
        // log.info("Polling....");

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

        for (ConsumerRecord<String, String> record : records) {
          log.info("key: " + record.key() + ", Value: " + record.value());
          log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
        }
      }
    } catch (WakeupException e) {
      log.info("Consumer is starting to shut down");
    } catch (Exception e) {
      log.error("Unexpected exception in th econsumer", e);
    } finally {
      consumer.close(); // close the consumer, this will also commit offsets
      log.info("The consumer is now gracefully shut down.");
    }
  }
}
