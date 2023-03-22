package com.epam;

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
class KafkaAvroProducer {

    private final static Logger LOGGER = Logger.getLogger(KafkaAvroProducer.class.getName());

    static void produce() {
        Properties properties = new Properties();

        //Add producer properties
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "10");

        //Add properties for avro part (serializer and deserializer)
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:8081");

        //Create kafka produced base on properties
        KafkaProducer<String, Customer> kafkaProducer = new KafkaProducer<>(properties);
        String kafkaTopic = "customer-avro";

        //Create instance of customer that should be serialized
        Customer customer = Customer.newBuilder()
                .setFirstName("Dominik")
                .setLastName("Janiga")
                .setAge(32)
                .setWeight(65)
                .setHeight(176)
                .setAutomatedEmail(false)
                .build();

        //Create producer record base on topic and customer data
        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(kafkaTopic, customer);

        try {
            //Send record to kafka
            kafkaProducer.send(producerRecord, (recordMetadata, exception) -> {
                LOGGER.info("Success");
                LOGGER.info(recordMetadata.toString());
            });
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Error on completion", ex);
        }
        finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }
}
