package com.strandls.es;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import co.elastic.clients.transport.rest5_client.low_level.Rest5ClientBuilder;

/**
 * @author Arun
 *
 */
public class ElasticSearchClient {

	private final ElasticsearchClient client;
	private final Rest5Client rest5Client;
	private final ElasticsearchTransport transport;

	public ElasticSearchClient(Rest5ClientBuilder rest5ClientBuilder) {
		this.rest5Client = rest5ClientBuilder.build();
		this.transport = new Rest5ClientTransport(rest5Client, new JacksonJsonpMapper());
		this.client = new ElasticsearchClient(transport);
	}

	public ElasticsearchClient getClient() {
		return client;
	}

	public Rest5Client getLowLevelClient() {
		return rest5Client;
	}

	public void close() throws java.io.IOException {
		if (transport != null) {
			transport.close();
		}
	}
}