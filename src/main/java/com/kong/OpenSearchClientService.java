package com.kong;

import org.apache.http.HttpHost;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;

import java.io.Closeable;
import java.io.IOException;

public enum OpenSearchClientService implements Closeable {

    INSTANCE;

    private final RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"))
            .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder);
    private final RestHighLevelClient client = new RestHighLevelClient(builder);


    public RestHighLevelClient getOpenSearchClient() {
        return this.client;
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
