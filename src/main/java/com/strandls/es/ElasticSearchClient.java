package com.strandls.es;

import java.io.IOException;

import org.apache.http.HttpHost;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

/**
 * @author Abhishek Rudra
 *
 * Elasticsearch client wrapper for ES 9.x using the new Java API Client
 */
public class ElasticSearchClient implements AutoCloseable {

	private final ElasticsearchClient client;
	private final RestClient restClient;
	private final ElasticsearchTransport transport;

	public ElasticSearchClient(RestClientBuilder restClientBuilder) {
		this.restClient = restClientBuilder.build();
		this.transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
		this.client = new ElasticsearchClient(transport);
	}

	public ElasticSearchClient(HttpHost... hosts) {
		this(RestClient.builder(hosts));
	}

	/**
	 * Get the Elasticsearch client instance
	 *
	 * @return ElasticsearchClient instance
	 */
	public ElasticsearchClient getClient() {
		return client;
	}

	/**
	 * Get the underlying REST client
	 *
	 * @return RestClient instance
	 */
	public RestClient getLowLevelClient() {
		return restClient;
	}

	@Override
	public void close() throws IOException {
		if (transport != null) {
			transport.close();
		}
		if (restClient != null) {
			restClient.close();
		}
	}
}
