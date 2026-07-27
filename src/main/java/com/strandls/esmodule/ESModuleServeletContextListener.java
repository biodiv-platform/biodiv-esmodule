package com.strandls.esmodule;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hc.core5.http.HttpHost;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.strandls.es.ElasticSearchClient;
import com.strandls.esmodule.binning.servicesImpl.BinningModule;
import com.strandls.esmodule.controllers.ESControllerModule;
import com.strandls.esmodule.services.impl.ESServiceImplModule;
import com.strandls.esmodule.utils.UtilityMethods;

import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import jakarta.servlet.ServletContextEvent;

/**
 * @author Arun
 *
 */
public class ESModuleServeletContextListener extends GuiceServletContextListener {

	private final Logger logger = LoggerFactory.getLogger(ESModuleServeletContextListener.class);

	@Override
	protected Injector getInjector() {
		Injector injector = Guice.createInjector(new ServletModule() {
			@Override
			protected void configureServlets() {

				try {
					String esUrl = ESmoduleConfig.getString("es.url");

					ElasticSearchClient esClient = new ElasticSearchClient(Rest5Client.builder(HttpHost.create(esUrl)));

					bind(ElasticSearchClient.class).toInstance(esClient);

				} catch (Exception e) { // Catches URISyntaxException
					logger.error("Failed to initialize Elasticsearch Client: Invalid URL", e);
					throw new RuntimeException("Elasticsearch configuration failed", e);
				}

				// 2. Standard Jackson Mapping
				ObjectMapper objectMapper = new ObjectMapper();
				bind(ObjectMapper.class).toInstance(objectMapper);

				bind(UtilityMethods.class).in(Scopes.SINGLETON);

				// 3. JAX-RS / Jersey Config (Jakarta EE 10 compatible)
				Map<String, String> props = new HashMap<String, String>();
				props.put("jakarta.ws.rs.Application", ApplicationConfig.class.getName());
				props.put("jersey.config.server.provider.packages", "com");
				props.put("jersey.config.server.wadl.disableWadl", "true");

				bind(ServletContainer.class).in(Scopes.SINGLETON);

				serve("/api/*").with(ServletContainer.class, props);
			}
		}, new ESControllerModule(), new ESServiceImplModule(), new BinningModule());

		return injector;

	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
		// Proper cleanup of the Singleton ES Client
		Injector injector = (Injector) sce.getServletContext().getAttribute(Injector.class.getName());

		if (injector != null) {
			ElasticSearchClient elasticSearchClient = injector.getInstance(ElasticSearchClient.class);
			if (elasticSearchClient != null) {
				try {
					logger.info("Closing Elasticsearch 9 Client...");
					elasticSearchClient.close();
				} catch (IOException e) {
					logger.error("Error closing elasticsearch client. ", e);
				}
			}
		}

		super.contextDestroyed(sce);
	}
}