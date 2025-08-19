package com.strandls.esmodule.binning.servicesImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.strandls.es.ElasticSearchClient;
import com.strandls.esmodule.binning.models.Feature;
import com.strandls.esmodule.binning.models.Geojson;
import com.strandls.esmodule.binning.models.GeojsonData;
import com.strandls.esmodule.binning.models.Geometry;
import com.strandls.esmodule.binning.services.GeojsonService;

import jakarta.inject.Inject;

/**
 * Services for {@link GeojsonData}
 *
 * @author mukund
 *
 */
public class GeojsonServiceImpl implements GeojsonService {

	@Inject
	private ElasticSearchClient client;

	public GeojsonData getGeojsonData(String index, String type, String geoField, double[][][] coordinatesList)
			throws IOException {

		Collection<Feature> features = new ArrayList<>(coordinatesList.length);

		long maxCount = 0;
		long minCount = 0;

		for (int i = 0; i < coordinatesList.length; i++) {

			// geometry
			double[][][] coordinates = new double[1][5][2];
			coordinates[0] = coordinatesList[i];
			Geometry geometry = new Geometry("Polygon", coordinates);

			// properties
			Map<String, Object> properties = new HashMap<>();
			GeoBoundingBoxQueryBuilder query = QueryBuilders.geoBoundingBoxQuery(geoField)
					.setCorners(coordinates[0][1][1], coordinates[0][0][0], coordinates[0][0][1], coordinates[0][2][0]);
			long count = querySearch(index, type, query);
			properties.put("doc_count", count);

			features.add(new Feature(geometry, properties));

			maxCount = Math.max(maxCount, count);
			minCount = Math.min(minCount, count);
		}

		Geojson geojson = new Geojson(features);
		return new GeojsonData(geojson, maxCount, minCount);
	}

	private long querySearch(String index, String type, QueryBuilder query) throws IOException {

		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

		sourceBuilder.query(query);

		SearchRequest searchRequest = new SearchRequest(index);
		searchRequest.source(sourceBuilder);

		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

		return searchResponse.getHits().getTotalHits().value;
	}

}
