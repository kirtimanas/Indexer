package com.kong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface IBatchMessageProcessor {

    public boolean processMessage(ConsumerRecord<String, String> currentKafkaRecord) throws Exception;

    public boolean onPollEndCallBack() throws Exception;

    public boolean processBulkMessage(ConsumerRecords<String, String> currentKafkaRecords) throws Exception;
}
