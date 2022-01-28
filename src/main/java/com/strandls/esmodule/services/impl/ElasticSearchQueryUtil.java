package com.strandls.esmodule.services.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.builders.CoordinatesBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.GeoBoundingBoxQueryBuilder;
import org.elasticsearch.index.query.GeoPolygonQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.strandls.esmodule.models.MapBoundParams;
import com.strandls.esmodule.models.MapBounds;
import com.strandls.esmodule.models.MapGeoPoint;
import com.strandls.esmodule.models.MapSearchParams;
import com.strandls.esmodule.models.query.MapAndBoolQuery;
import com.strandls.esmodule.models.query.MapAndMatchPhraseQuery;
import com.strandls.esmodule.models.query.MapAndRangeQuery;
import com.strandls.esmodule.models.query.MapBoolQuery;
import com.strandls.esmodule.models.query.MapExistQuery;
import com.strandls.esmodule.models.query.MapMatchPhraseQuery;
import com.strandls.esmodule.models.query.MapOrBoolQuery;
import com.strandls.esmodule.models.query.MapOrMatchPhraseQuery;
import com.strandls.esmodule.models.query.MapOrRangeQuery;
import com.strandls.esmodule.models.query.MapQuery;
import com.strandls.esmodule.models.query.MapRangeQuery;
import com.strandls.esmodule.models.query.MapSearchQuery;

public class ElasticSearchQueryUtil {

	private static final int SHARD_SIZE = 100;

	private final Logger logger = LoggerFactory.getLogger(ElasticSearchQueryUtil.class);

	private QueryBuilder getNestedQueryBuilder(MapQuery query, QueryBuilder queryBuilder) {
		if (query.getPath() == null)
			return queryBuilder;
		return QueryBuilders.nestedQuery(query.getPath(), queryBuilder, ScoreMode.None);
	}

	private QueryBuilder getTermsQueryBuilder(MapBoolQuery query) {
		TermsQueryBuilder queryBuilder = QueryBuilders.termsQuery(query.getKey(), query.getValues());
		return query.getPath() != null ? getNestedQueryBuilder(query, queryBuilder) : queryBuilder;
	}

	private QueryBuilder getExistsQueryBuilder(MapQuery query) {
		ExistsQueryBuilder queryBuilder = QueryBuilders.existsQuery(query.getKey());
		return query.getPath() != null ? getNestedQueryBuilder(query, queryBuilder) : queryBuilder;
	}

	private QueryBuilder getRangeQueryBuilder(MapRangeQuery query) {
		RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery(query.getKey()).gte(query.getStart())
				.lte(query.getEnd());
		return query.getPath() != null ? getNestedQueryBuilder(query, queryBuilder) : queryBuilder;
	}

	private QueryBuilder getMatchPhraseQueryBuilder(MapMatchPhraseQuery query) {
		MatchPhraseQueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery(query.getKey(), query.getValue());
		return query.getPath() != null ? getNestedQueryBuilder(query, queryBuilder) : queryBuilder;
	}

	private void buildBoolQueries(List<MapAndBoolQuery> andQueries, List<MapOrBoolQuery> orQueries,
			BoolQueryBuilder masterBoolQuery) {

		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

		List<MapAndBoolQuery> nonNestedAnd = andQueries.stream()
				.filter(p -> (p.getPath() == null || p.getPath().isEmpty())).collect(Collectors.toList());

		List<MapAndBoolQuery> nestedAnd = andQueries.stream()
				.filter(p -> (p.getPath() != null && !p.getPath().isEmpty())).collect(Collectors.toList());

		buildNestedBoolAndQuery(nestedAnd, masterBoolQuery);

		if (andQueries != null) {
			boolQuery = QueryBuilders.boolQuery();
			for (MapBoolQuery query : nonNestedAnd) {
				if (query.getValues() != null)
					boolQuery.must(getTermsQueryBuilder(query));
				else
					boolQuery.mustNot(getExistsQueryBuilder(query));
			}
			masterBoolQuery.must(boolQuery);
		}

		List<MapOrBoolQuery> nonNestedOrList = orQueries.stream()
				.filter(p -> (p.getPath() == null || p.getPath().isEmpty())).collect(Collectors.toList());

		List<MapOrBoolQuery> nestedOrList = orQueries.stream()
				.filter(p -> (p.getPath() != null && !p.getPath().isEmpty())).collect(Collectors.toList());

		buildNestedBoolOrQuery(nestedOrList, masterBoolQuery);

		if (orQueries != null) {
			boolQuery = QueryBuilders.boolQuery();
			for (MapBoolQuery query : nonNestedOrList) {
				if (query.getValues() != null)
					boolQuery.should(getTermsQueryBuilder(query));
				else
					boolQuery.mustNot(getExistsQueryBuilder(query));
			}
			masterBoolQuery.must(boolQuery);
		}
	}

	private void combinationNestedQuery(BoolQueryBuilder masterBoolQery, BoolQueryBuilder nestedBoolQuery,
			String nestedPath) {
		String regex = "(.)*(\\d)(.)*";
		Pattern pattern = Pattern.compile(regex);//NOSONAR
		if (StringUtils.isNumeric(nestedPath)) {
			masterBoolQery.must(nestedBoolQuery);
		} else {
			// case nested combination query convert "fieldData.108"->"fieldData"
			if (pattern.matcher(nestedPath).matches()) {
				List<String> list = Arrays.asList(nestedPath.split("\\.")).subList(0,
						(nestedPath.split("\\.").length - 1));
				nestedPath = String.join(".", list);
			}
			masterBoolQery.must(QueryBuilders.nestedQuery(nestedPath, nestedBoolQuery, ScoreMode.None));
		}
	}

	private void buildNestedBoolAndQuery(List<MapAndBoolQuery> nestedAnd, BoolQueryBuilder masterBoolQery) {

		Map<String, List<MapAndBoolQuery>> nestedGroupAndByList = nestedAnd.stream()
				.collect(Collectors.groupingBy(w -> w.getPath()));

		for (Entry<String, List<MapAndBoolQuery>> item : nestedGroupAndByList.entrySet()) {
			BoolQueryBuilder nestedBoolQuery = QueryBuilders.boolQuery();

			// for combination and nested combination queies
			String nestedPath = item.getKey();

			item.getValue().forEach(qry -> {
				qry.setPath(null);
				if (qry.getValues() != null)
					nestedBoolQuery.must(getTermsQueryBuilder(qry));
				else
					nestedBoolQuery.mustNot(getExistsQueryBuilder(qry));
			});

			combinationNestedQuery(masterBoolQery, nestedBoolQuery, nestedPath);

		}
	}

	private void buildNestedBoolOrQuery(List<MapOrBoolQuery> nestedOr, BoolQueryBuilder masterBoolQery) {

		Map<String, List<MapOrBoolQuery>> nestedGroupAndByList = nestedOr.stream()
				.collect(Collectors.groupingBy(w -> w.getPath()));

		for (Entry<String, List<MapOrBoolQuery>> item : nestedGroupAndByList.entrySet()) {
			BoolQueryBuilder nestedBoolQuery = QueryBuilders.boolQuery();

			// for combination and nested combination queies
			String nestedPath = item.getKey();

			item.getValue().forEach(qry -> {
				qry.setPath(null);
				if (qry.getValues() != null)
					nestedBoolQuery.should(getTermsQueryBuilder(qry));
				else
					nestedBoolQuery.mustNot(getExistsQueryBuilder(qry));
			});

			combinationNestedQuery(masterBoolQery, nestedBoolQuery, nestedPath);
		}
	}

	// builds nested, combination , nested combination queries based on the path
	// type
	// eg. path = "108"[combination query],
	// eg. path = "fieldData"[nested],
	// eg. path = "fieldData.108"[nested combination]
	private void buildNestedMatchPhraseAndQuery(List<MapAndMatchPhraseQuery> nestedAnd,
			BoolQueryBuilder masterBoolQery) {

		// group by path parameter in nestedAdd query
		Map<String, List<MapAndMatchPhraseQuery>> nestedGroupAndByList = nestedAnd.stream()
				.collect(Collectors.groupingBy(w -> w.getPath()));

		// for each path create a combination or nested or nested-combination query
		for (Entry<String, List<MapAndMatchPhraseQuery>> item : nestedGroupAndByList.entrySet()) {
			BoolQueryBuilder nestedBoolQuery = QueryBuilders.boolQuery();
			// for combination and nested combination queies
			String nestedPath = item.getKey();

			item.getValue().forEach(qry -> {
				qry.setPath(null);
				if (qry.getValue() != null)
					nestedBoolQuery.must(getMatchPhraseQueryBuilder(qry));
				else
					nestedBoolQuery.mustNot(getExistsQueryBuilder(qry));
			});

			combinationNestedQuery(masterBoolQery, nestedBoolQuery, nestedPath);
		}
	}

	private void buildNestedMatchPhraseOrQuery(List<MapOrMatchPhraseQuery> nestedor, BoolQueryBuilder masterBoolQery) {

		Map<String, List<MapOrMatchPhraseQuery>> nestedGroupAndByList = nestedor.stream()
				.collect(Collectors.groupingBy(w -> w.getPath()));

		for (Entry<String, List<MapOrMatchPhraseQuery>> item : nestedGroupAndByList.entrySet()) {
			BoolQueryBuilder nestedBoolQuery = QueryBuilders.boolQuery();

			// for combination and nested combination queies
			String nestedPath = item.getKey();

			item.getValue().forEach(qry -> {
				qry.setPath(null);
				if (qry.getValue() != null)
					nestedBoolQuery.should(getMatchPhraseQueryBuilder(qry));
				else
					nestedBoolQuery.mustNot(getExistsQueryBuilder(qry));
			});

			combinationNestedQuery(masterBoolQery, nestedBoolQuery, nestedPath);
		}
	}

	private void buildRangeQueries(List<MapAndRangeQuery> andQueries, List<MapOrRangeQuery> orQueries,
			BoolQueryBuilder masterBoolQuery) {

		BoolQueryBuilder boolQuery;

		if (andQueries != null) {
			boolQuery = QueryBuilders.boolQuery();
			for (MapAndRangeQuery query : andQueries) {
				boolQuery.must(getRangeQueryBuilder(query));
			}
			masterBoolQuery.must(boolQuery);
		}

		if (orQueries != null) {
			boolQuery = QueryBuilders.boolQuery();
			for (MapOrRangeQuery query : orQueries) {
				boolQuery.should(getRangeQueryBuilder(query));
			}
			masterBoolQuery.must(boolQuery);
		}
	}

	private void buildExistsQueries(List<MapExistQuery> andExistQueries, BoolQueryBuilder masterBoolQuery) {

		if (andExistQueries != null) {
			BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
			for (MapExistQuery query : andExistQueries) {
				if (query.isExists())
					boolQuery.must(getExistsQueryBuilder(query));
				else
					boolQuery.mustNot(getExistsQueryBuilder(query));
			}
			masterBoolQuery.must(boolQuery);
		}
	}

	private void buildMatchPhraseQueries(List<MapAndMatchPhraseQuery> andQueries, List<MapOrMatchPhraseQuery> orQueries,
			BoolQueryBuilder masterBoolQuery) {
		BoolQueryBuilder boolQuery;

		if (andQueries != null) {

			List<MapAndMatchPhraseQuery> nonNestedOrList = andQueries.stream()
					.filter(p -> (p.getPath() == null || p.getPath().isEmpty())).collect(Collectors.toList());

			List<MapAndMatchPhraseQuery> nestedOrList = andQueries.stream()
					.filter(p -> (p.getPath() != null && !p.getPath().isEmpty())).collect(Collectors.toList());

			buildNestedMatchPhraseAndQuery(nestedOrList, masterBoolQuery);

			boolQuery = QueryBuilders.boolQuery();
			for (MapAndMatchPhraseQuery query : nonNestedOrList) {
				if (query.getValue() != null)
					boolQuery.must(getMatchPhraseQueryBuilder(query));
				else
					boolQuery.mustNot(getExistsQueryBuilder(query));
			}
			masterBoolQuery.must(boolQuery);
		}

		if (orQueries != null) {

			List<MapOrMatchPhraseQuery> nonNestedOrList = orQueries.stream()
					.filter(p -> (p.getPath() == null || p.getPath().isEmpty())).collect(Collectors.toList());

			List<MapOrMatchPhraseQuery> nestedOrList = orQueries.stream()
					.filter(p -> (p.getPath() != null && !p.getPath().isEmpty())).collect(Collectors.toList());

			buildNestedMatchPhraseOrQuery(nestedOrList, masterBoolQuery);

			boolQuery = QueryBuilders.boolQuery();
			for (MapOrMatchPhraseQuery query : nonNestedOrList) {
				if (query.getValue() != null)
					boolQuery.should(getMatchPhraseQueryBuilder(query));
				else
					boolQuery.mustNot(getExistsQueryBuilder(query));
			}
			masterBoolQuery.must(boolQuery);
		}

	}

	protected BoolQueryBuilder getBoolQueryBuilder(MapSearchQuery searchQuery) {

		BoolQueryBuilder masterBoolQuery = QueryBuilders.boolQuery();

		if (searchQuery == null)
			return masterBoolQuery;

		buildBoolQueries(searchQuery.getAndBoolQueries(), searchQuery.getOrBoolQueries(), masterBoolQuery);
		buildRangeQueries(searchQuery.getAndRangeQueries(), searchQuery.getOrRangeQueries(), masterBoolQuery);
		buildExistsQueries(searchQuery.getAndExistQueries(), masterBoolQuery);
		buildMatchPhraseQueries(searchQuery.getAndMatchPhraseQueries(), searchQuery.getOrMatchPhraseQueries(),
				masterBoolQuery);
		return masterBoolQuery;
	}

	public MatchPhraseQueryBuilder getBoolQueryBuilderObservationPan(String id, Boolean isMaxVotedRecoId) {

		MatchPhraseQueryBuilder masterBoolQueryBuilder = null;
		if (isMaxVotedRecoId)
			masterBoolQueryBuilder = QueryBuilders.matchPhraseQuery("max_voted_reco.id", id);
		else
//			taxonomyId
			masterBoolQueryBuilder = QueryBuilders.matchPhraseQuery("max_voted_reco.hierarchy.taxon_id", id);
		return masterBoolQueryBuilder;
	}

	protected GeoGridAggregationBuilder getGeoGridAggregationBuilder(String field, Integer precision) {
		if (field == null)
			return null;

		precision = precision != null ? precision : 1;
		GeoGridAggregationBuilder geohashGrid = AggregationBuilders.geohashGrid(field + "-" + precision);
		geohashGrid.field(field);
		geohashGrid.precision(precision);
		return geohashGrid;
	}

	protected TermsAggregationBuilder getTermsAggregationBuilder(String field, String subField, Integer size) {
		TermsAggregationBuilder builder = AggregationBuilders.terms(field);
		builder.field(field);

		if (subField != null)
			builder.subAggregation(AggregationBuilders.terms(subField).field(subField));

		builder.size(size);
		builder.shardSize(SHARD_SIZE);
		return builder;
	}

	protected void applyMapBounds(MapSearchParams searchParams, BoolQueryBuilder masterBoolQuery,
			String geoAggregationField) {

		MapBoundParams mapBoundParams = searchParams.getMapBoundParams();
		if (mapBoundParams == null)
			return;

		MapBounds bounds = mapBoundParams.getBounds();
		if (bounds != null) {
			applyMapBounds(bounds, masterBoolQuery, geoAggregationField);
		}

		List<MapGeoPoint> polygon = mapBoundParams.getPolygon();
		if (polygon != null && !polygon.isEmpty() && geoAggregationField != null) {
			List<GeoPoint> geoPoints = new ArrayList<>();
			for (MapGeoPoint point : polygon)
				geoPoints.add(new GeoPoint(point.getLat(), point.getLon()));

			GeoPolygonQueryBuilder setPolygon = QueryBuilders.geoPolygonQuery(geoAggregationField, geoPoints);
			masterBoolQuery.filter(setPolygon);
		}
	}

	protected void applyShapeFilter(MapSearchParams searchParams, BoolQueryBuilder masterBoolQuery,
			String geoShapeFilterField) throws IOException {

		MapBoundParams mapBoundParams = searchParams.getMapBoundParams();
		if (mapBoundParams == null)
			return;

		List<MapGeoPoint> polygon = mapBoundParams.getPolygon();
		List<List<MapGeoPoint>> multipolygon = mapBoundParams.getMultipolygon();

		if (polygon != null && !polygon.isEmpty()) {
			applyGeoPolygonQuery(polygon, masterBoolQuery, geoShapeFilterField);
		} else if (multipolygon != null && !multipolygon.isEmpty()) {
			applyMultiPolygonQuery(multipolygon, masterBoolQuery, geoShapeFilterField);
		}
	}

	protected void applyGeoPolygonQuery(List<MapGeoPoint> polygon, BoolQueryBuilder masterBoolQuery,
			String geoShapeFilterField) throws IOException {
		CoordinatesBuilder cb = new CoordinatesBuilder();

		polygon.forEach(i -> {
			cb.coordinate(i.getLon(), i.getLat());
		});
		Geometry polygonSet = new PolygonBuilder(cb).buildGeometry();
		GeoShapeQueryBuilder qb = QueryBuilders.geoShapeQuery(geoShapeFilterField, polygonSet);
		masterBoolQuery.minimumShouldMatch(1);
		masterBoolQuery.should(qb);

	}

	protected void applyMultiPolygonQuery(List<List<MapGeoPoint>> multipolygon, BoolQueryBuilder masterBoolQuery,
			String geoShapeFilterField) throws IOException {

		multipolygon.forEach(item -> {
			try {
				applyGeoPolygonQuery(item, masterBoolQuery, geoShapeFilterField);

			} catch (IOException e) {
				logger.error(e.getMessage());
			}
		});
	}

	protected void applyMapBounds(MapBounds bounds, BoolQueryBuilder masterBoolQuery, String geoAggregationField) {

		if (bounds != null) {
			GeoBoundingBoxQueryBuilder setCorners = QueryBuilders.geoBoundingBoxQuery(geoAggregationField)
					.setCorners(bounds.getTop(), bounds.getLeft(), bounds.getBottom(), bounds.getRight());
			masterBoolQuery.filter(setCorners);
		}
	}

}
