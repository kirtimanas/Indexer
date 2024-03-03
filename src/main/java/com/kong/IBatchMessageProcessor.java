package com.kong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * The interface IBatchMessageProcessor.
 *
 * This interface is the template callback functions that can
 * be passed to an instance of the {@link com.kong.KafkaEventConsumer}
 */
public interface IBatchMessageProcessor {

    public boolean processMessage(ConsumerRecord<String, String> currentKafkaRecord) throws Exception;

    public boolean onPollEndCallBack() throws Exception;

    public boolean processBulkMessage(ConsumerRecords<String, String> currentKafkaRecords) throws Exception;
}
