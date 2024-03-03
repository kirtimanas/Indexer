package com.kong;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The type KafkaEventProducer is a wrapper class for {@link org.apache.kafka.clients.producer.KafkaProducer}.
 * The object publishes methods that send messages that have reads from {@link com.kong.IEventReader}
 * content onto the Kafka broker defined in {@link /src/resources/config.properties}
 */
public class KafkaEventProducer extends AbstractKafkaSimple {

    private org.apache.kafka.clients.producer.KafkaProducer<String, String> kafkaProducer;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Logger log = Logger.getLogger(KafkaEventProducer.class.getName());
    private final IEventReader eventReader;

    public KafkaEventProducer(IEventReader eventReader) {
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
            Thread.sleep(100);
        }
    }

    private void send(String key, String message) throws Exception {
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("cdc-events", key, message);
        log.info(message);
        System.out.println(message);
        getKafkaProducer().send(producerRecord);
    }

    private synchronized org.apache.kafka.clients.producer.KafkaProducer<String, String> getKafkaProducer() throws Exception {
        if (this.kafkaProducer == null) {
            Properties props = PropertiesHelper.getProperties();
            this.kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
        }
        return this.kafkaProducer;
    }
}
