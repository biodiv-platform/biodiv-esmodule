package com.strandls.esmodule.binning.servicesImpl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import co.elastic.clients.elasticsearch._types.GeoLocation;
import co.elastic.clients.elasticsearch._types.LatLonGeoLocation;
import co.elastic.clients.elasticsearch._types.query_dsl.GeoBoundingBoxQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchResponse;

import com.strandls.es.ElasticSearchClient;
import com.strandls.esmodule.binning.models.Feature;
import com.strandls.esmodule.binning.models.Geojson;
import com.strandls.esmodule.binning.models.GeojsonData;
import com.strandls.esmodule.binning.models.Geometry;
import com.strandls.esmodule.binning.services.GeojsonService;

import jakarta.inject.Inject;

/**
 * Services for {@link GeojsonData} Migrated to Elasticsearch 9.x Java API
 * Client
 *
 * @author mukund
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

			// Create GeoHashBox query using new ES 9 API
			final double top = coordinates[0][1][1];
			final double left = coordinates[0][0][0];
			final double bottom = coordinates[0][0][1];
			final double right = coordinates[0][2][0];

			Query query = GeoBoundingBoxQuery
					.of(g -> g.field(geoField).boundingBox(b -> b.tlbr(tlbr -> tlbr
							.topLeft(GeoLocation.of(gl -> gl.latlon(LatLonGeoLocation.of(ll -> ll.lat(top).lon(left)))))
							.bottomRight(GeoLocation
									.of(gl -> gl.latlon(LatLonGeoLocation.of(ll -> ll.lat(bottom).lon(right))))))))
					._toQuery();

			long count = querySearch(index, type, query);
			properties.put("doc_count", count);

			features.add(new Feature(geometry, properties));

			maxCount = Math.max(maxCount, count);
			minCount = Math.min(minCount, count);
		}

		Geojson geojson = new Geojson(features);
		return new GeojsonData(geojson, maxCount, minCount);
	}

	private long querySearch(String index, String type, Query query) throws IOException {

		SearchResponse<Void> searchResponse = client.getClient().search(s -> s.index(index).query(query), Void.class);

		return searchResponse.hits().total() != null ? searchResponse.hits().total().value() : 0;
	}

}
