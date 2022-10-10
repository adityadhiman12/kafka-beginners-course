package com.aditya.kafka;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKey {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKey.class.getName());
    public static void main(String[] args) {

        System.out.println("Hello world!");
        //create producer properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++) {
            //create producer record
            String topic = "demo_java";
            String key = "id_" + i;
            String message = "Hello World" + i;

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, message);

            //send data - asynchronous
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception==null) {
                        log.info("topic: " + metadata.topic() + "\n" +
                                "key: " + producerRecord.key() + "\n" +
                                "partition: " + metadata.partition() + "\n" +
                                "offset: " + metadata.offset()
                        );
                    }
                    else {
                        log.error("producer msg sent failed with exception:" + exception);
                    }
                }
            });
        }

        //flush - synchronous
        producer.flush(); //This makes sure that the above data is sent and then only proceed further on flushing and closing

        //flush and close the producer
        producer.close();
    }
}
