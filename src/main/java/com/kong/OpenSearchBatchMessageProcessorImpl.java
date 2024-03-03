package com.kong;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.opensearch.action.bulk.BulkRequest;

import java.util.Map;

public class OpenSearchBatchMessageProcessorImpl implements IBatchMessageProcessor {

    private final OpenSearchService openSearchService = new OpenSearchService();
    private final Gson gson = new Gson();
    private static final String KEY_FIELD = "key";

    @Override
    public boolean processMessage(ConsumerRecord<String, String> currentKafkaRecord) throws Exception {
        Map<String, Object> valueMap = processSingleEvent(currentKafkaRecord);
        String id = String.valueOf(valueMap.get(KEY_FIELD));
        System.out.println(id);
        openSearchService.updateRequest(id, valueMap);
        return true;
    }

    @Override
    public boolean onPollEndCallBack() throws Exception {
        return false;
    }

    public boolean processBulkMessage(ConsumerRecords<String, String> currentKafkaRecords) throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        for (ConsumerRecord<String, String> record : currentKafkaRecords) {
            Map<String, Object> valueMap = processSingleEvent(record);
            String id = String.valueOf(valueMap.get(KEY_FIELD));
            System.out.println(id);
            bulkRequest = openSearchService.prepareBulkUpdateRequest(id, valueMap, bulkRequest);
        }

        openSearchService.bulkUpdateRequest(bulkRequest);
        return true;
    }

    private Map<String, Object> processSingleEvent(ConsumerRecord<String, String> currentKafkaRecord) {
        String value = currentKafkaRecord.value();
        System.out.println("Value is" + value);
        Map<String,Object> attributes = gson.fromJson(value,Map.class);
        return (Map<String, Object>) attributes.get("after");
    }

}
