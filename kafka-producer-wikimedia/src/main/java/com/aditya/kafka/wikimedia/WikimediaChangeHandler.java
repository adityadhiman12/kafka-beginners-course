package com.aditya.kafka.wikimedia;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.EventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WikimediaChangeHandler implements EventHandler {
    private KafkaProducer<String, String> kafkaProducer;
    private String topic;
    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }
    public void onOpen()  {

    }

    public void onClosed() throws Exception {
        kafkaProducer.close();
    }

    public void onMessage(String event, MessageEvent messageEvent) throws Exception{
        log.info(messageEvent.getData());
        //asynchronous
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));

    }

    @Override
    public void onComment(String comment) throws Exception {
        //nothing here
    }

    @Override
    public void onError(Throwable t) {
        log.error("error while stream reading" + t );
    }

}
