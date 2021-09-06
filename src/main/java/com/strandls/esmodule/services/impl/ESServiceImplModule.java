package com.strandls.esmodule.services.impl;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.strandls.esmodule.services.ElasticAdminSearchService;
import com.strandls.esmodule.services.ElasticSearchGeoService;
import com.strandls.esmodule.services.ElasticSearchService;

/**
 * Guice binding module
 * 
 * @author mukund
 *
 */
public class ESServiceImplModule extends AbstractModule {

	@Override
	protected void configure() {

		bind(ElasticAdminSearchService.class).to(ElasticAdminSearchServiceImpl.class).in(Scopes.SINGLETON);
		bind(ElasticSearchService.class).to(ElasticSearchServiceImpl.class).in(Scopes.SINGLETON);
		bind(ElasticSearchGeoService.class).to(ElasticSearchGeoServiceImpl.class).in(Scopes.SINGLETON);
		bind(ElasticSearchServiceHelper.class).in(Scopes.SINGLETON);
	}
}