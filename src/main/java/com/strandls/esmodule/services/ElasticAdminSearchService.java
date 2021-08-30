package com.strandls.esmodule.services;

import java.io.IOException;


import com.strandls.esmodule.models.MapDocument;
import com.strandls.esmodule.models.MapQueryResponse;

/**
 * All the admin serviecs supported by map app
 * 
 * @author mukund
 */
public interface ElasticAdminSearchService {

	/**
	 * Define a mapping for an index and type
	 * 
	 * @param index
	 *            the index on which mapping needs to be defined
	 * @param mapping
	 *            the mapping
	 * @return {@link MapQueryResponse}
	 * @throws IOException throws {@link IOException}
	 */
	MapQueryResponse postMapping(String index, String mapping) throws IOException;
	
	/**
	 * Define a mapping for an index and type
	 * 
	 * @param index
	 *            the index on which mapping needs to be defined
	 * @param type
	 * 			  elastic search accepts a single type per index           
	 * @param mapping
	 *            the mapping
	 * @return {@link MapQueryResponse}
	 * @throws IOException throws {@link IOException}
	 */
	
	MapQueryResponse esPostMapping(String index,String mapping) throws IOException;

	/**
	 * Get the mapping for an index
	 * 
	 * @param index
	 *            the index for which mapping is needed
	 * @return {@link MapDocument}
	 * @throws IOException throws {@link IOException}
	 */
	MapDocument getMapping(String index) throws IOException;

	/**
	 * Create an index type
	 * 
	 * @param index
	 *            the index to be created
	 * @param type
	 *            the type to be created
	 * @return {@link MapQueryResponse}
	 * @throws IOException throws {@link IOException}
	 */
	MapQueryResponse createIndex(String index, String type) throws IOException;

	
	/**
	 * @param index
	 * @param mapping
	 * @return
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	MapQueryResponse reIndex(String index, String mapping) throws IOException, InterruptedException;
	
	
	

}
