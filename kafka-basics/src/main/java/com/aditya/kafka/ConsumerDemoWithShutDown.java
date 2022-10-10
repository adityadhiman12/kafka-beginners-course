package com.aditya.kafka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutDown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutDown.class.getName());
    public static void main(String[] args) {

        System.out.println("I AM a kafka Consumer!");
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-second-application";
        String topic = "demo_java";
        //create producer properties
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // types are: earliest/none/latest

        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run(){
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup(); //what this does is, it causes consumer.poll to throw an exception
                //to do this, we need to join the main thread
                try {
                    mainThread.join();
                }catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            //subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));

            //poll for new data
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for(ConsumerRecord<String, String> record: records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() +", Offset: " + record.offset());
                }
                log.info("Polling");
            }
        } catch (WakeupException e) {
            log.info("Wake up exception");
            //we ignore this as this is an expected exception when closing a consumer
        } catch (Exception e) {
            log.error("Unexpected exception");
        } finally {
             consumer.close(); //this will also commit the offsets
        }

    }
}
