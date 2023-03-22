package com.epam;

/**
 * @author Dominik_Janiga
 */
class Main {
    public static void main(String[] args) {
        KafkaAvroProducer.produce();
        KafkaAvroConsumer.consume();
    }
}
