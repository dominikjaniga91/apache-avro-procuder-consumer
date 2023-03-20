package com.example;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Dominik_Janiga
 */
public class KafkaAvroProducer {

    private final static Logger LOGGER = Logger.getLogger(KafkaAvroProducer.class.getName());

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "10");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<>(properties);
        String topic = "customer-avro";
        Customer customer = Customer.newBuilder()
                .setFirstName("Dominik")
                .setLastName("Janiga")
                .setAge(32)
                .setWeight(65)
                .setHeight(176)
                .setAutomatedEmail(false)
                .build();
        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(topic, customer);

        try {
            kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {

                if (exception == null) {
                    LOGGER.info("Success");
                    LOGGER.info(recordMetadata.toString());
                } else {
                    LOGGER.log(Level.SEVERE, "Error on completion", exception);
                }
            });
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }
}
