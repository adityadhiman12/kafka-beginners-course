package com.aditya.kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getName());
    public static void main(String[] args) {

        System.out.println("Hello world!");
        //create producer properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello World");

        //send data - asynchronous
        producer.send(producerRecord);

        //flush - synchronous
        producer.flush(); //This makes sure that the above data is sent and then only proceed further on flushing and closing

        //flush and close the producer
        producer.close();
    }
}
