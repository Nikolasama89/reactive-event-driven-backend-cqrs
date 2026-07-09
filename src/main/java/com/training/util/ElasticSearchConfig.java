package com.training.util;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.training.config.AppConfig;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

public class ElasticSearchConfig {
    private ElasticSearchConfig() {}

    private static ElasticsearchAsyncClient client;

    public static void init(AppConfig config) {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        RestClient restClient = RestClient.builder(
                new HttpHost(config.esHost(), config.esPort())).build();
        ElasticsearchTransport transport =
                new RestClientTransport(restClient, new JacksonJsonpMapper(mapper));
        client = new ElasticsearchAsyncClient(transport);
    }

    public static ElasticsearchAsyncClient getClient() {
        if (client == null)
            throw new IllegalStateException("ElasticSearchConfig.init(config) must be called first");
        return client;
    }
}
