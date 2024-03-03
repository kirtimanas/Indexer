package com.kong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleProducer extends AbstractKafkaSimple {

    private KafkaProducer<String, String> kafkaProducer;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Logger log = Logger.getLogger(SimpleProducer.class.getName());
    private final IEventReader eventReader;

    public SimpleProducer(IEventReader eventReader) {
        this.eventReader = eventReader;
    }

    @Override
    public void shutdown() throws Exception {
        closed.set(true);
        log.info("Shutting down producer");
        getKafkaProducer().close();
    }

    @Override
    public void runAlways(OpenSearchBatchMessageProcessorImpl callback) throws Exception {
        while (true) {
            String key = UUID.randomUUID().toString();
            //Read from file here
            String message = this.eventReader.getNextEvent();
            this.send(key, message);
            Thread.sleep(5000);
        }
    }

    private void send(String key, String message) throws Exception {
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("cdc-events", key, message);
        log.info(message);
        System.out.println(message);
        getKafkaProducer().send(producerRecord);
    }

    private synchronized KafkaProducer<String, String> getKafkaProducer() throws Exception {
        if (this.kafkaProducer == null) {
            Properties props = PropertiesHelper.getProperties();
            this.kafkaProducer = new KafkaProducer<>(props);
        }
        return this.kafkaProducer;
    }
}
