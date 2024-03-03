package com.kong;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The type KafkaEventConsumer is used to consume messages
 * from a Kafka cluster. The class provides functionality for the
 * {@link org.apache.kafka.clients.consumer.KafkaConsumer}.
 */
public class KafkaEventConsumer extends AbstractKafka {

    private final int TIME_OUT_MS = 5000;
    static Logger log = Logger.getLogger(KafkaEventConsumer.class.getName());
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private KafkaConsumer<String, String> kafkaConsumer = null;

    public KafkaEventConsumer() {
    }

    public KafkaConsumer<String, String> getKafkaConsumer() {
        return kafkaConsumer;
    }

    public void setKafkaConsumer(KafkaConsumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }


    @Override
    public void shutdown() throws Exception {
        closed.set(true);
        getKafkaConsumer().wakeup();
        // This class shouldn't be responsible for closing OpenSearchCLient
    }

    private void close() throws Exception {
        if (this.getKafkaConsumer() == null){
            log.info("The internal consumer is NULL");
            return;
        }
        log.info("Closing consumer");
        if( this.getKafkaConsumer() != null) this.getKafkaConsumer().close();
    }

    /**
     * The runAlways method retrieves a collection of ConsumerRecords continuously.
     * The number of max number of records retrieved in each polling session back to
     * the Kafka broker is defined by the property max.poll.records as published by
     * the class {@link com.kong.PropertiesHelper} object
     *
     * @param callback the callback function that processes messages retrieved
     *                 from Kafka
     * @throws Exception the Exception that will get thrown upon an error
     */
    //TODO topic is hardcoded here, pass it via constructor or make it configurable
    @Override
    public void runAlways(OpenSearchBatchMessageProcessorImpl callback) throws Exception {
        Properties props = PropertiesHelper.getProperties();
        setKafkaConsumer(new KafkaConsumer<>(props));

        try {
            getKafkaConsumer().subscribe(List.of("cdc-events"));
            while (!closed.get()) {
                ConsumerRecords<String, String> records = getKafkaConsumer().poll(Duration.ofMillis(TIME_OUT_MS));
                if (records.count() == 0) {
                    log.info("No records retrieved");
                    System.out.println("No records ..");
                }

//                for (ConsumerRecord<String, String> record : records) {
//                    callback.processMessage(record);
//                }
                else if (records.count() > 0) {
                    callback.processBulkMessage(records);
                }

                Thread.sleep(1000);
            }
        } catch (WakeupException e) {
            if (!closed.get()) throw e;
        } finally {
            close();
        }

    }
}
