package com.strandls.esmodule.services.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.ParsedGeoHashGrid;
import org.elasticsearch.search.aggregations.metrics.GeoBounds;
import org.elasticsearch.search.aggregations.metrics.GeoBoundsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.strandls.es.ElasticSearchClient;
import com.strandls.esmodule.ErrorConstants;
import com.strandls.esmodule.models.MapDocument;
import com.strandls.esmodule.models.MapResponse;
import com.strandls.esmodule.services.ElasticSearchGeoService;

import jakarta.inject.Inject;

/**
 * Implementation of {@link ElasticSearchGeoService}
 *
 * @author mukund
 *
 */
public class ElasticSearchGeoServiceImpl implements ElasticSearchGeoService {

	private final Logger logger = LoggerFactory.getLogger(ElasticSearchGeoServiceImpl.class);

	@Inject
	private ElasticSearchClient client;

	private MapResponse querySearch(String index, QueryBuilder query) throws IOException {

		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

		sourceBuilder.query(query);
		sourceBuilder.from(0);
		sourceBuilder.size(500);

		SearchRequest searchRequest = new SearchRequest(index);
		searchRequest.source(sourceBuilder);

		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		List<MapDocument> result = new ArrayList<>();

		long totalHits = searchResponse.getHits().getTotalHits().value;

		for (SearchHit hit : searchResponse.getHits().getHits())
			result.add(new MapDocument(hit.getSourceAsString()));

		logger.info("Search completed with total hits: {}", totalHits);

		return new MapResponse(result, totalHits, null);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see com.strandls.naksha.es.services.api.ElasticSearchGeoService#
	 * getGeoWithinDocuments(java.lang.String, java.lang.String, java.lang.String,
	 * double, double, double, double)
	 */
	@Override
	public MapResponse getGeoWithinDocuments(String index, String type, String geoField, double top, double left,
			double bottom, double right) throws IOException {

		logger.info(ErrorConstants.GEO_WITH_SEARCH, top, left, bottom, right);
		GeoBoundingBoxQueryBuilder query = QueryBuilders.geoBoundingBoxQuery(geoField).setCorners(top, left, bottom,
				right);

		return querySearch(index, query);
	}

	@Override

	public Map<String, Long> getGeoAggregation(String index, String type, String geoField, Integer precision,
			Double top, Double left, Double bottom, Double right, Long speciesId) throws IOException {

		logger.info(ErrorConstants.GEO_WITH_SEARCH, top, left, bottom, right);

		AggregationBuilder aggregationBuilder = AggregationBuilders.geohashGrid("agg").field(geoField)
				.precision(precision);
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		if (top != null && left != null && bottom != null && right != null) {

			top = top < LAT_MIN || top > LAT_MAX ? Math.copySign(LAT_MAX, top) : top;
			bottom = bottom < LAT_MIN || bottom > LAT_MAX ? Math.copySign(LAT_MAX, bottom) : bottom;

			left = left < LON_MIN || left > LON_MAX ? Math.copySign(LON_MAX, left) : left;
			right = right < LON_MIN || right > LON_MAX ? Math.copySign(LON_MAX, right) : right;

			searchSourceBuilder.query(QueryBuilders.geoBoundingBoxQuery(geoField).setCorners(top, left, bottom, right));
		}

		if (speciesId != null) {
			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
			TermQueryBuilder termQuery = QueryBuilders
					.termQuery("all_reco_vote.scientific_name.taxon_detail.species_id", speciesId);
			boolQuery.filter(termQuery);
			searchSourceBuilder.query(boolQuery);
		}

		searchSourceBuilder.aggregation(aggregationBuilder);

		SearchRequest searchRequest = new SearchRequest(index);
		searchRequest.source(searchSourceBuilder);

		try {
			SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
			Aggregations aggregations = searchResponse.getAggregations();
			ParsedGeoHashGrid geoHashGrid = aggregations.get("agg");

			Map<String, Long> hashToCount = new HashMap<String, Long>();
			for (GeoGrid.Bucket b : geoHashGrid.getBuckets()) {
				hashToCount.put(b.getKeyAsString(), b.getDocCount());
			}
			return hashToCount;
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
		return null;
	}

	@Override
	public List<List<Double>> getGeoBounds(String jsonString) throws IOException {
		JSONObject jsonObject = new JSONObject(jsonString);
		String index = jsonObject.getString("index");

		BoolQueryBuilder boolqueryBuilder = getBooleanSearchQuery(jsonString);

		GeoBoundsAggregationBuilder aggregation = new GeoBoundsAggregationBuilder("aggs").field("location");

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(boolqueryBuilder);
		searchSourceBuilder.aggregation(aggregation);

		SearchRequest searchRequest = new SearchRequest(index);
		searchRequest.source(searchSourceBuilder);

		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		Aggregations aggregations = searchResponse.getAggregations();
		GeoBounds geoBounds = aggregations.get("aggs");

		List<List<Double>> result = new ArrayList<List<Double>>();
		List<Double> topLeft = new ArrayList<Double>();
		topLeft.add(geoBounds.topLeft().getLat());
		topLeft.add(geoBounds.topLeft().getLon());

		List<Double> bottomRight = new ArrayList<Double>();
		bottomRight.add(geoBounds.bottomRight().getLat());
		bottomRight.add(geoBounds.bottomRight().getLon());

		result.add(topLeft);
		result.add(bottomRight);

		return result;
	}

	@Override
	public Map<String, Long> getGeoAggregation(String jsonString) throws IOException {
		JSONObject jsonObject = new JSONObject(jsonString);
		String index = jsonObject.getString("index");
		String geoField = jsonObject.getString("geoField");

		Integer precision = jsonObject.getInt("precision");

		AggregationBuilder aggregationBuilder = AggregationBuilders.geohashGrid("agg").field(geoField)
				.precision(precision);
		BoolQueryBuilder boolqueryBuilder = getBooleanSearchQuery(jsonString);

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(boolqueryBuilder);
		searchSourceBuilder.aggregation(aggregationBuilder);

		SearchRequest searchRequest = new SearchRequest(index);
		searchRequest.source(searchSourceBuilder);

		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		Aggregations aggregations = searchResponse.getAggregations();
		ParsedGeoHashGrid geoHashGrid = aggregations.get("agg");

		Map<String, Long> hashToCount = new HashMap<String, Long>();
		for (GeoGrid.Bucket b : geoHashGrid.getBuckets()) {
			hashToCount.put(b.getKeyAsString(), b.getDocCount());
		}
		return hashToCount;
	}

	private BoolQueryBuilder getBooleanSearchQuery(String jsonString) {
		JSONObject jsonObject = new JSONObject(jsonString);
		String geoField = jsonObject.getString("geoField");

		BoolQueryBuilder boolqueryBuilder = QueryBuilders.boolQuery();
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
			boolqueryBuilder.filter(QueryBuilders.geoBoundingBoxQuery(geoField).setCorners(top, left, bottom, right));
		}

		if (jsonObject.has("speciesId")) {
			Long speciesId = jsonObject.getLong("speciesId");
			TermQueryBuilder termQuery = QueryBuilders.termQuery("max_voted_reco.species_id", speciesId);
			boolqueryBuilder.must(termQuery);
		}
		if (jsonObject.has("groupId")) {
			Long groupId = jsonObject.getLong("groupId");
			TermQueryBuilder termQuery = QueryBuilders.termQuery("group_id", groupId);
			boolqueryBuilder.must(termQuery);
		}
		if (jsonObject.has("userGroupId")) {
			Long userGroupId = jsonObject.getLong("userGroupId");
			TermQueryBuilder termQuery = QueryBuilders.termQuery("user_group_observations.id", userGroupId);
			boolqueryBuilder.must(termQuery);
		}

		if (jsonObject.has("authorId")) {
			Long userGroupId = jsonObject.getLong("authorId");
			TermQueryBuilder termQuery = QueryBuilders.termQuery("author_id", userGroupId);
			boolqueryBuilder.must(termQuery);
		}

		boolqueryBuilder.must(QueryBuilders.termQuery("flag_count", 0));
		return boolqueryBuilder;
	}
}