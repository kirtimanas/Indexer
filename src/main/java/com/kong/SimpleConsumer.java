package com.kong;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleConsumer extends AbstractKafkaSimple{

    private final int TIME_OUT_MS = 5000;
    static Logger log = Logger.getLogger(SimpleConsumer.class.getName());
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private KafkaConsumer<String, String> kafkaConsumer = null;

    public SimpleConsumer() {
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

    public static void main(String[] args) throws Exception {
        new SimpleConsumer().runAlways(new OpenSearchBatchMessageProcessorImpl());
    }
}
