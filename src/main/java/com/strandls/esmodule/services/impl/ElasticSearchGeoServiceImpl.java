package com.strandls.esmodule.services.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch._types.GeoLocation;
import co.elastic.clients.elasticsearch._types.LatLonGeoLocation;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.GeoBoundsAggregate;
import co.elastic.clients.elasticsearch._types.aggregations.GeoHashGridAggregate;
import co.elastic.clients.elasticsearch._types.aggregations.GeoHashGridBucket;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.GeoBoundingBoxQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch._types.GeoHashPrecision;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.strandls.es.ElasticSearchClient;
import com.strandls.esmodule.ErrorConstants;
import com.strandls.esmodule.models.MapDocument;
import com.strandls.esmodule.models.MapResponse;
import com.strandls.esmodule.services.ElasticSearchGeoService;

import jakarta.inject.Inject;

/**
 * Implementation of {@link ElasticSearchGeoService} Migrated to Elasticsearch
 * 9.x Java API Client
 *
 * @author mukund
 */
public class ElasticSearchGeoServiceImpl implements ElasticSearchGeoService {

	private final Logger logger = LoggerFactory.getLogger(ElasticSearchGeoServiceImpl.class);

	@Inject
	private ElasticSearchClient client;

	@Inject
	private ObjectMapper objectMapper;

	@SuppressWarnings("rawtypes")
	private MapResponse querySearch(String index, Query query) throws IOException {

		SearchResponse<Map> searchResponse = client.getClient()
				.search(s -> s.index(index).query(query).from(0).size(500), Map.class);

		List<MapDocument> result = new ArrayList<>();
		long totalHits = searchResponse.hits().total() != null ? searchResponse.hits().total().value() : 0;

		for (Hit<Map> hit : searchResponse.hits().hits()) {
			if (hit.source() != null) {
				String jsonString = objectMapper.writeValueAsString(hit.source());
				result.add(new MapDocument(jsonString));
			}
		}

		logger.info("Search completed with total hits: {}", totalHits);

		return new MapResponse(result, totalHits, null);
	}

	@Override
	public MapResponse getGeoWithinDocuments(String index, String type, String geoField, double top, double left,
			double bottom, double right) throws IOException {

		logger.info(ErrorConstants.GEO_WITH_SEARCH, top, left, bottom, right);

		Query query = GeoBoundingBoxQuery
				.of(g -> g.field(geoField).boundingBox(b -> b.tlbr(tlbr -> tlbr
						.topLeft(GeoLocation.of(gl -> gl.latlon(LatLonGeoLocation.of(ll -> ll.lat(top).lon(left)))))
						.bottomRight(GeoLocation
								.of(gl -> gl.latlon(LatLonGeoLocation.of(ll -> ll.lat(bottom).lon(right))))))))
				._toQuery();

		return querySearch(index, query);
	}

	@Override
	public Map<String, Long> getGeoAggregation(String index, String type, String geoField, Integer precision,
			Double top, Double left, Double bottom, Double right, Long speciesId) throws IOException {

		logger.info(ErrorConstants.GEO_WITH_SEARCH, top, left, bottom, right);

		SearchRequest.Builder searchBuilder = new SearchRequest.Builder();
		searchBuilder.index(index);

		BoolQuery.Builder boolQuery = new BoolQuery.Builder();

		if (top != null && left != null && bottom != null && right != null) {
			top = top < LAT_MIN || top > LAT_MAX ? Math.copySign(LAT_MAX, top) : top;
			bottom = bottom < LAT_MIN || bottom > LAT_MAX ? Math.copySign(LAT_MAX, bottom) : bottom;
			left = left < LON_MIN || left > LON_MAX ? Math.copySign(LON_MAX, left) : left;
			right = right < LON_MIN || right > LON_MAX ? Math.copySign(LON_MAX, right) : right;

			final double finalTop = top;
			final double finalLeft = left;
			final double finalBottom = bottom;
			final double finalRight = right;

			// FIXED: Proper GeoBoundingBox construction with GeoLocation wrapper
			Query geoQuery = GeoBoundingBoxQuery.of(g -> g.field(geoField).boundingBox(b -> b.tlbr(tlbr -> tlbr
					.topLeft(GeoLocation
							.of(gl -> gl.latlon(LatLonGeoLocation.of(ll -> ll.lat(finalTop).lon(finalLeft)))))
					.bottomRight(GeoLocation
							.of(gl -> gl.latlon(LatLonGeoLocation.of(ll -> ll.lat(finalBottom).lon(finalRight))))))))
					._toQuery();

			boolQuery.filter(geoQuery);
		}

		if (speciesId != null) {
			Query termQuery = TermQuery.of(t -> t.field("all_reco_vote.scientific_name.taxon_detail.species_id")
					.value(v -> v.longValue(speciesId)))._toQuery();
			boolQuery.filter(termQuery);
		}

		searchBuilder.query(boolQuery.build()._toQuery());

		searchBuilder.size(0);

		// Convert Integer precision to GeoHashPrecision
		GeoHashPrecision geoHashPrecision = GeoHashPrecision.of(g -> g.geohashLength(precision));
		searchBuilder.aggregations("agg", a -> a.geohashGrid(g -> g.field(geoField).precision(geoHashPrecision)));

		try {
			SearchResponse<Void> searchResponse = client.getClient().search(searchBuilder.build(), Void.class);

			if (searchResponse.aggregations() != null && searchResponse.aggregations().get("agg") != null) {
				Aggregate agg = searchResponse.aggregations().get("agg");

				if (agg.isGeohashGrid()) {
					GeoHashGridAggregate geoHashGrid = agg.geohashGrid();
					Map<String, Long> hashToCount = new HashMap<>();

					for (GeoHashGridBucket bucket : geoHashGrid.buckets().array()) {
						hashToCount.put(bucket.key(), bucket.docCount());
					}
					return hashToCount;
				}
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return null;
	}

	@Override
	public List<List<Double>> getGeoBounds(String jsonString) throws IOException {
		JSONObject jsonObject = new JSONObject(jsonString);
		String index = jsonObject.getString("index");

		BoolQuery.Builder boolQueryBuilder = getBooleanSearchQuery(jsonString);

		SearchResponse<Void> searchResponse = client.getClient()
				.search(s -> s.index(index).query(boolQueryBuilder.build()._toQuery()).size(0).aggregations("aggs",
						a -> a.geoBounds(g -> g.field("location"))), Void.class);

		List<List<Double>> result = new ArrayList<>();

		if (searchResponse.aggregations() != null && searchResponse.aggregations().get("aggs") != null) {
			Aggregate agg = searchResponse.aggregations().get("aggs");

			if (agg.isGeoBounds()) {
				GeoBoundsAggregate geoBounds = agg.geoBounds();

				if (geoBounds.bounds().isTlbr()) {
					var bounds = geoBounds.bounds().tlbr();

					var topLeftGeo = bounds.topLeft().latlon();
					var bottomRightGeo = bounds.bottomRight().latlon();

					List<Double> topLeft = new ArrayList<>();
					topLeft.add(topLeftGeo.lat());
					topLeft.add(topLeftGeo.lon());

					List<Double> bottomRight = new ArrayList<>();
					bottomRight.add(bottomRightGeo.lat());
					bottomRight.add(bottomRightGeo.lon());

					result.add(topLeft);
					result.add(bottomRight);
				}
			}
		}

		return result;
	}

	@Override
	public Map<String, Long> getGeoAggregation(String jsonString) throws IOException {
		JSONObject jsonObject = new JSONObject(jsonString);
		String index = jsonObject.getString("index");
		String geoField = jsonObject.getString("geoField");
		Integer precision = jsonObject.getInt("precision");

		BoolQuery.Builder boolQueryBuilder = getBooleanSearchQuery(jsonString);

		// Convert Integer to GeoHashPrecision
		GeoHashPrecision geoHashPrecision = GeoHashPrecision.of(g -> g.geohashLength(precision));

		SearchResponse<Void> searchResponse = client.getClient()
				.search(s -> s.index(index).query(boolQueryBuilder.build()._toQuery()).size(0).aggregations("agg",
						a -> a.geohashGrid(g -> g.field(geoField).precision(geoHashPrecision)

						)), Void.class);

		Map<String, Long> hashToCount = new HashMap<>();

		if (searchResponse.aggregations() != null && searchResponse.aggregations().get("agg") != null) {
			Aggregate agg = searchResponse.aggregations().get("agg");

			if (agg.isGeohashGrid()) {
				GeoHashGridAggregate geoHashGrid = agg.geohashGrid();
				for (GeoHashGridBucket bucket : geoHashGrid.buckets().array()) {
					hashToCount.put(bucket.key(), bucket.docCount());
				}
			}
		}

		return hashToCount;
	}

	private BoolQuery.Builder getBooleanSearchQuery(String jsonString) {
		JSONObject jsonObject = new JSONObject(jsonString);
		String geoField = jsonObject.getString("geoField");

		BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

		if (jsonObject.has("top") && jsonObject.has("left") && jsonObject.has("bottom") && jsonObject.has("right")) {

			Double top = jsonObject.getDouble("top");
			Double left = jsonObject.getDouble("left");
			Double bottom = jsonObject.getDouble("bottom");
			Double right = jsonObject.getDouble("right");

			logger.info(ErrorConstants.GEO_WITH_SEARCH, top, left, bottom, right);

			top = top < LAT_MIN || top > LAT_MAX ? Math.copySign(LAT_MAX, top) : top;
			bottom = bottom < LAT_MIN || bottom > LAT_MAX ? Math.copySign(LAT_MAX, bottom) : bottom;
			left = left < LON_MIN || left > LON_MAX ? Math.copySign(LON_MAX, left) : left;
			right = right < LON_MIN || right > LON_MAX ? Math.copySign(LON_MAX, right) : right;

			final double finalTop = top;
			final double finalLeft = left;
			final double finalBottom = bottom;
			final double finalRight = right;

			Query geoQuery = GeoBoundingBoxQuery.of(g -> g.field(geoField).boundingBox(b -> b.tlbr(tlbr -> tlbr
					.topLeft(GeoLocation
							.of(gl -> gl.latlon(LatLonGeoLocation.of(ll -> ll.lat(finalTop).lon(finalLeft)))))
					.bottomRight(GeoLocation
							.of(gl -> gl.latlon(LatLonGeoLocation.of(ll -> ll.lat(finalBottom).lon(finalRight))))))))
					._toQuery();

			boolQueryBuilder.filter(geoQuery);
		}

		if (jsonObject.has("speciesId")) {
			Long speciesId = jsonObject.getLong("speciesId");
			Query termQuery = TermQuery.of(t -> t.field("max_voted_reco.species_id").value(v -> v.longValue(speciesId)))
					._toQuery();
			boolQueryBuilder.must(termQuery);
		}

		if (jsonObject.has("groupId")) {
			Long groupId = jsonObject.getLong("groupId");
			Query termQuery = TermQuery.of(t -> t.field("group_id").value(v -> v.longValue(groupId)))._toQuery();
			boolQueryBuilder.must(termQuery);
		}

		if (jsonObject.has("userGroupId")) {
			Long userGroupId = jsonObject.getLong("userGroupId");
			Query termQuery = TermQuery
					.of(t -> t.field("user_group_observations.id").value(v -> v.longValue(userGroupId)))._toQuery();
			boolQueryBuilder.must(termQuery);
		}

		if (jsonObject.has("authorId")) {
			Long authorId = jsonObject.getLong("authorId");
			Query termQuery = TermQuery.of(t -> t.field("author_id").value(v -> v.longValue(authorId)))._toQuery();
			boolQueryBuilder.must(termQuery);
		}

		Query flagQuery = TermQuery.of(t -> t.field("flag_count").value(v -> v.longValue(0)))._toQuery();
		boolQueryBuilder.must(flagQuery);

		return boolQueryBuilder;
	}
}
