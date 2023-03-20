package com.example;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;

/**
 *
 * @author Dominik_Janiga
 */
public class KafkaAvroConsumer {

    private final static Logger LOGGER = Logger.getLogger(KafkaAvroProducer.class.getName());

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        properties.put("group.id", "customer-consumer-group");
        properties.put("auto.commit.enable", "false");
        properties.put("auto.offset.reset", "earliest");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:8081");
        properties.setProperty("specific.avro.reader", "true");

        KafkaConsumer<String, Customer> kafkaConsumer = new KafkaConsumer<>(properties);
        String topic = "customer-avro";
        kafkaConsumer.subscribe(Collections.singleton(topic));

        LOGGER.info("Waiting for data...");

        try {
            while (true){
                LOGGER.info("Polling");
                ConsumerRecords<String, Customer> records = kafkaConsumer.poll(1000);

                for (ConsumerRecord<String, Customer> record : records){
                    Customer customer = record.value();
                    LOGGER.info(customer.toString());
                }
                kafkaConsumer.commitSync();
            }
        } finally {
            kafkaConsumer.close();
        }
    }
}