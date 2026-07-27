package com.strandls.esmodule.services.impl;

import java.io.IOException;

import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

import co.elastic.clients.transport.rest5_client.low_level.Request;
import co.elastic.clients.transport.rest5_client.low_level.Response;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.strandls.es.ElasticSearchClient;
import com.strandls.esmodule.models.MapDocument;
import com.strandls.esmodule.models.MapQueryResponse;
import com.strandls.esmodule.models.MapQueryStatus;
import com.strandls.esmodule.services.ElasticAdminSearchService;

import jakarta.inject.Inject;

/**
 * Implementation of {@link ElasticAdminSearchService}
 *
 * @author arun
 *
 */
public class ElasticAdminSearchServiceImpl implements ElasticAdminSearchService {

	private final Rest5Client client;

	private final Logger logger = LoggerFactory.getLogger(ElasticAdminSearchServiceImpl.class);

	@Inject
	public ElasticAdminSearchServiceImpl(ElasticSearchClient client) {
		this.client = client.getLowLevelClient();
	}

	private String getStatusResponse(Response response) {
		int statusCode = response.getStatusCode();
		return String.valueOf(statusCode);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * com.strandls.naksha.es.services.api.ElasticAdminSearchService#postMapping(
	 * java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public MapQueryResponse postMapping(String index, String mapping) throws IOException {
		String indexParam = index.replaceAll("[\n\r\t]", "_");
		logger.info("Trying to add mapping to index: {}", indexParam);

		Request request = new Request("PUT", "/" + index + "/_mapping");

		if (mapping != null && !mapping.isEmpty()) {
			request.setEntity(new StringEntity(mapping, ContentType.APPLICATION_JSON));
		}

		Response response = client.performRequest(request);
		String status = getStatusResponse(response);

		logger.info("Added mapping to index: {} with status: {}", indexParam, status);
		return new MapQueryResponse(MapQueryStatus.UNKNOWN, status);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * com.strandls.naksha.es.services.api.ElasticAdminSearchService#getMapping(java
	 * .lang.String)
	 */
	@Override
	public MapDocument getMapping(String index) throws IOException {
		String indexParam = index.replaceAll("[\n\r\t]", "_");
		logger.info("Trying to get mapping for index: {}", indexParam);

		Request request = new Request("GET", "/" + index + "/_mapping");
		Response response = client.performRequest(request);

		try {
			// This now requires catching ParseException in Apache 5
			String content = EntityUtils.toString(response.getEntity());
			logger.info("Retrieved mapping for index: {} with status: {}", indexParam, response.getStatusCode());
			return new MapDocument(content);

		} catch (org.apache.hc.core5.http.ParseException e) {
			logger.error("Failed to parse Elasticsearch response entity", e);
			throw new IOException("Error parsing ES response", e);
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * com.strandls.naksha.es.services.api.ElasticAdminSearchService#createIndex(
	 * java.lang.String, java.lang.String)
	 */
	@Override
	public MapQueryResponse createIndex(String index, String type) throws IOException {
		String indexParam = index.replaceAll("[\n\r\t]", "_");
		logger.info("Trying to create index: {}", indexParam);

		Request request = new Request("PUT", "/" + index);
		Response response = client.performRequest(request);
		String status = getStatusResponse(response);

		logger.info("Created index: {} with status: {}", indexParam, status);
		return new MapQueryResponse(MapQueryStatus.UNKNOWN, status);
	}

	@Override
	public MapQueryResponse esPostMapping(String index, String mapping) throws IOException {
		logger.info("Trying to add mapping to index: {}", index);

		// Note: ensure the path matches your intended logic (index create vs mapping
		// update)
		Request request = new Request("PUT", "/" + index);
		if (mapping != null && !mapping.isEmpty()) {
			request.setEntity(new StringEntity(mapping, ContentType.APPLICATION_JSON));
		}

		Response response = client.performRequest(request);
		String status = getStatusResponse(response);

		logger.info("Added mapping to index: {} with status: {}", index, status);
		return new MapQueryResponse(MapQueryStatus.UNKNOWN, status);
	}
}
