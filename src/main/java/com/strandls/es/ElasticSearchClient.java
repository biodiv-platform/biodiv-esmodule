
package com.strandls.es;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;

/**
 * @author Arun
 *
 */
public class ElasticSearchClient {

    private final ElasticsearchClient client;
    private final RestClient restClient;
    private final ElasticsearchTransport transport;

    public ElasticSearchClient(RestClientBuilder restClientBuilder) {
        this.restClient = restClientBuilder.build();
        this.transport = new RestClientTransport(
                restClient,
                new JacksonJsonpMapper());
        this.client = new ElasticsearchClient(transport);
    }

    public ElasticsearchClient getClient() {
        return client;
    }

    public RestClient getLowLevelClient() {
        return restClient;
    }

    public void close() throws java.io.IOException {
        if (transport != null) {
            transport.close();
        }
        if (restClient != null) {
            restClient.close();
        }
    }
}