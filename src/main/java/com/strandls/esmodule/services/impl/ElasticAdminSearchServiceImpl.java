package com.strandls.esmodule.services.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import javax.inject.Inject;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.strandls.es.ElasticSearchClient;
import com.strandls.esmodule.models.MapDocument;
import com.strandls.esmodule.models.MapQueryResponse;
import com.strandls.esmodule.models.MapQueryStatus;
import com.strandls.esmodule.services.ElasticAdminSearchService;

/**
 * Implementation of {@link ElasticAdminSearchService}
 * 
 * @author mukund
 *
 */
public class ElasticAdminSearchServiceImpl implements ElasticAdminSearchService {

	private final RestClient client;

	private final Logger logger = LoggerFactory.getLogger(ElasticAdminSearchServiceImpl.class);

	@Inject
	public ElasticAdminSearchServiceImpl(ElasticSearchClient client) {
		this.client = client.getLowLevelClient();
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
		String indexParam=index.replaceAll("[\n\r\t]", "_");
		logger.info("Trying to add mapping to index: {}", indexParam);

		StringEntity entity = null;
		if (!Strings.isNullOrEmpty(mapping)) {
			entity = new StringEntity(mapping, ContentType.APPLICATION_JSON);
		}

		Request request = new Request("PUT", index + "/_mapping");
		request.setEntity(entity);
		Response response = client.performRequest(request);
		String status = response.getStatusLine().getReasonPhrase();

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
		String indexParam=index.replaceAll("[\n\r\t]", "_");
		logger.info("Trying to get mapping for index: {}", indexParam);
		
		Request request = new Request("GET", index + "/_mapping");
		Response response = client.performRequest(request);
		String status = response.getStatusLine().getReasonPhrase();

		logger.info("Retrieved mapping for index: {} with status: {}", indexParam, status);

		return new MapDocument(EntityUtils.toString(response.getEntity()));
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
		String indexParam=index.replaceAll("[\n\r\t]", "_");
		logger.info("Trying to create index: {}", indexParam);

		Request request = new Request("PUT", "/" + index);
		Response response = client.performRequest(request);
		String status = response.getStatusLine().getReasonPhrase();

		logger.info("Created index: {} with status: {}", indexParam, status);

		return new MapQueryResponse(MapQueryStatus.UNKNOWN, status);
	}

	@Override
	public MapQueryResponse esPostMapping(String index, String mapping) throws IOException {
		logger.info("Trying to add mapping to index: {}", index);

		StringEntity entity = null;
		if (!Strings.isNullOrEmpty(mapping)) {
			entity = new StringEntity(mapping, ContentType.APPLICATION_JSON);
		}
		Request request = new Request("PUT", index + "/");
		request.setEntity(entity);
		Response response = client.performRequest(request);

		String status = response.getStatusLine().getReasonPhrase();

		logger.info("Added mapping to index: {} with status: {}", index, status);
		return new MapQueryResponse(MapQueryStatus.UNKNOWN, status);
	}

	@Override
	public MapQueryResponse reIndex(String index, String mapping) throws IOException, InterruptedException {
		String filePath = "/app/configurations/scripts/";
		String script = null;
		Response response = deleteIndex(index);
		if (response != null) {
			MapQueryResponse mapQueryResponse = esPostMapping(index, mapping);
			String status = mapQueryResponse.getMessage();
			if (index.equalsIgnoreCase("extended_taxon_definition")) {
				script = "runTaxonElasticMigration.sh";
				if (status.equalsIgnoreCase("ok") && startShellScriptProcess(script, filePath) == 0) {
					return new MapQueryResponse(MapQueryStatus.UPDATED, "re-indexing successful!");
				}
			} else if (index.equalsIgnoreCase("extended_observation")) {
				script = "refreshObservationMV.sh";
				if (startShellScriptProcess(script, filePath) == 0) {
					script = "runObservationElasticMigration.sh";
					if (status.equalsIgnoreCase("ok") && startShellScriptProcess(script, filePath) == 0) {
						return new MapQueryResponse(MapQueryStatus.UPDATED, "re-indexing successful!");
					}
				}
			} else if (index.equalsIgnoreCase("extended_observation_test")) {
				script = "refreshObservationMVTest.sh";
				System.out.println("\n\nrefreshObservationMVTest.sh\n\n");
				if (startShellScriptProcess(script, filePath) == 0) {
					script = "runObservationElasticMigrationTest.sh";
					System.out.println("\n\runObservationElasticMigrationTest.sh\n\n");
					if (status.equalsIgnoreCase("ok") && startShellScriptProcessPB(script, filePath) == 0) {
						System.out.println("\n\nInside\n\n");
						return new MapQueryResponse(MapQueryStatus.UPDATED, "re-indexing successful!");
					}
				}
			}
		}
		return new MapQueryResponse(MapQueryStatus.UNKNOWN, "re-indexing failure");
	}

	private Response deleteIndex(String index) {
		Request request = new Request("DELETE", index);
		Response response = null;
		try {
			response = client.performRequest(request);
			return response;
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return response;

	}

	private Integer startShellScriptProcess(String script, String filePath) throws InterruptedException {
		Process process;
		try {
			process = Runtime.getRuntime().exec("sh " + script, null, new File(filePath));
			return process.waitFor();
		} catch (IOException e) {
			logger.error(e.getMessage());
		} catch (InterruptedException e) {
			logger.error(e.getMessage());
			Thread.currentThread().interrupt();
		}
		return -1;
	}

	private Integer startShellScriptProcessPB(String script, String filePath) {
		Process process;
		try {

			ProcessBuilder pb = new ProcessBuilder(Arrays.asList("nohup", "sh", script));
			pb.directory(new File(filePath));
			pb.redirectErrorStream(false);
			process = pb.start();

//			int processStatus = process.waitFor();
			String line = null;
			final BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
			while ((line = reader.readLine()) != null) {
				System.out.println(line);// Ignore line, or do something with it
			}
			return 0;
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return -1;
	}
}
