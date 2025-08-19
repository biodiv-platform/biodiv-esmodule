package com.strandls.esmodule;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
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

import jakarta.servlet.ServletContextEvent;

/**
 * @author Abhishek Rudra
 *
 */

public class ESModuleServeletContextListener extends GuiceServletContextListener {

	private final Logger logger = LoggerFactory.getLogger(ESModuleServeletContextListener.class);

	@Override
	protected Injector getInjector() {
		Injector injector = Guice.createInjector(new ServletModule() {
			@Override
			protected void configureServlets() {

				ElasticSearchClient esClient = new ElasticSearchClient(
						RestClient.builder(HttpHost.create(ESmoduleConfig.getString("es.url"))));
				bind(ElasticSearchClient.class).toInstance(esClient);

				ObjectMapper objectMapper = new ObjectMapper();
				bind(ObjectMapper.class).toInstance(objectMapper);

				bind(UtilityMethods.class).in(Scopes.SINGLETON);

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
		Injector injector = (Injector) sce.getServletContext().getAttribute(Injector.class.getName());

		ElasticSearchClient elasticSearchClient = injector.getInstance(ElasticSearchClient.class);
		if (elasticSearchClient != null) {
			try {
				elasticSearchClient.close();
			} catch (IOException e) {
				logger.error("Error closing elasticsearch client. ", e);
			}
		}

		super.contextDestroyed(sce);
	}

}
