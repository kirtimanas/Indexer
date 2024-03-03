package com.kong;

import org.apache.log4j.Logger;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;

import java.io.IOException;
import java.util.Map;

public class OpenSearchService {

    private static final String INDEX_NAME = "cdc";
    private final Logger log = Logger.getLogger(OpenSearchService.class);

    public OpenSearchService() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Thread(() -> {
            try {
                OpenSearchClientService.INSTANCE.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        })));
    }

    private IndexRequest createIndexRequest(String id, Map<String, Object> doc) {
        IndexRequest request = new IndexRequest(INDEX_NAME);
        request.source(doc);
        request.id(id);
        return request;
    }

    public BulkRequest prepareBulkUpdateRequest(String id, Map<String, Object> doc, final BulkRequest bulkRequest) {
        IndexRequest request = createIndexRequest(id, doc);
        bulkRequest.add(request);
        return bulkRequest;
    }

    public void updateRequest(String id, Map<String, Object> valueMap) {
        try {
            IndexRequest request = createIndexRequest(id, valueMap);
            IndexResponse indexResponse = OpenSearchClientService.INSTANCE.getOpenSearchClient().index(request, RequestOptions.DEFAULT);
            System.out.println("Index Response: " + indexResponse);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void bulkUpdateRequest(BulkRequest bulkRequest) throws IOException {
        try {
            BulkResponse bulkResponses = OpenSearchClientService.INSTANCE.getOpenSearchClient().bulk(bulkRequest, RequestOptions.DEFAULT);
            handleBulkResponse(bulkResponses);
        } catch (IOException e) {
            log.error("Error performing bulk update", e);
            System.out.println("Error performing bulk update");
        }
    }

    private void handleBulkResponse(BulkResponse bulkResponses) {
        BulkItemResponse bulkItemResp;
        if (bulkResponses.hasFailures()) {
            System.out.println("Failures in bulk update!");
            int failedCount = 0;
            for (BulkItemResponse bulkResponse : bulkResponses) {
                bulkItemResp = bulkResponse;
                if (bulkItemResp.isFailed()) {
                    failedCount++;
                    String errorMessage = bulkItemResp.getFailure().getMessage();
                    String restResponse = bulkItemResp.getFailure().getStatus().name();
                    System.out.println("Failed Message# " + failedCount + " , REST response: " + restResponse + "  errorMessage: " + errorMessage);
                }
            }
        }
    }
}
