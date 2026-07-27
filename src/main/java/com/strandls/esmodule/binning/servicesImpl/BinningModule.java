package com.strandls.esmodule.binning.servicesImpl;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

/**
 * Guice binding module
 *
 * @author mukund
 *
 */
public class BinningModule extends AbstractModule {

	@Override
	protected void configure() {

		bind(BinningServiceImpl.class).in(Scopes.SINGLETON);
		bind(GeojsonServiceImpl.class).in(Scopes.SINGLETON);
	}
}