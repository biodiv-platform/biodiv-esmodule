package com.strandls.esmodule.services.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.GeoLocation;
import co.elastic.clients.elasticsearch._types.GeoHashPrecision;
import co.elastic.clients.elasticsearch._types.LatLonGeoLocation;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.ExistsQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.GeoBoundingBoxQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.GeoShapeQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchPhraseQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.NestedQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TermsQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TermsQueryField;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation;
import co.elastic.clients.elasticsearch._types.aggregations.GeoHashGridAggregation;
import co.elastic.clients.elasticsearch._types.aggregations.TermsAggregation;
import co.elastic.clients.elasticsearch._types.query_dsl.GeoShapeFieldQuery;

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

/**
 * Elasticsearch Query Utility for ES 9.x Migrated from High Level REST Client
 * to Java API Client
 */
public class ElasticSearchQueryUtil {

	private static final int SHARD_SIZE = 100;

	private final Logger logger = LoggerFactory.getLogger(ElasticSearchQueryUtil.class);

	private Query getNestedQuery(MapQuery query, Query innerQuery) {
		if (query.getPath() == null)
			return innerQuery;
		return NestedQuery.of(n -> n.path(query.getPath()).query(innerQuery)
				.scoreMode(co.elastic.clients.elasticsearch._types.query_dsl.ChildScoreMode.None))._toQuery();
	}

	private Query getTermsQuery(MapBoolQuery query) {
		List<FieldValue> values = query.getValues().stream().map(v -> FieldValue.of(v.toString()))
				.collect(Collectors.toList());

		Query termsQuery = TermsQuery.of(t -> t.field(query.getKey()).terms(TermsQueryField.of(tf -> tf.value(values))))
				._toQuery();

		return query.getPath() != null ? getNestedQuery(query, termsQuery) : termsQuery;
	}

	private Query getExistsQuery(MapQuery query) {
		Query existsQuery = ExistsQuery.of(e -> e.field(query.getKey()))._toQuery();
		return query.getPath() != null ? getNestedQuery(query, existsQuery) : existsQuery;
	}

	private Query getRangeQuery(MapRangeQuery query) {
		if (query.getStart() == null && query.getEnd() == null) {
			return null;
		}

		RangeQuery.Builder builder = new RangeQuery.Builder();

		// Try to determine if it's a number or date
		try {
			// Attempt as number range
			Double startValue = query.getStart() != null ? Double.parseDouble(query.getStart().toString()) : null;
			Double endValue = query.getEnd() != null ? Double.parseDouble(query.getEnd().toString()) : null;

			builder.number(n -> {
				n.field(query.getKey());
				if (startValue != null)
					n.gte(startValue);
				if (endValue != null)
					n.lte(endValue);
				return n;
			});
		} catch (NumberFormatException e) {
			// If not a number, treat as date range
			builder.date(d -> {
				d.field(query.getKey());
				if (query.getStart() != null)
					d.gte(query.getStart().toString());
				if (query.getEnd() != null)
					d.lte(query.getEnd().toString());
				return d;
			});
		}

		Query rangeQuery = builder.build()._toQuery();
		return query.getPath() != null ? getNestedQuery(query, rangeQuery) : rangeQuery;
	}

	private Query getMatchPhraseQuery(MapMatchPhraseQuery query) {
		Query matchQuery = MatchPhraseQuery.of(m -> m.field(query.getKey()).query(query.getValue().toString()))
				._toQuery();

		return query.getPath() != null ? getNestedQuery(query, matchQuery) : matchQuery;
	}

	private void buildBoolQueries(List<MapAndBoolQuery> andQueries, List<MapOrBoolQuery> orQueries,
			BoolQuery.Builder masterBoolQuery) {

		List<MapAndBoolQuery> nonNestedAnd = andQueries.stream()
				.filter(p -> (p.getPath() == null || p.getPath().isEmpty())).collect(Collectors.toList());

		List<MapAndBoolQuery> nestedAnd = andQueries.stream()
				.filter(p -> (p.getPath() != null && !p.getPath().isEmpty())).collect(Collectors.toList());

		buildNestedBoolAndQuery(nestedAnd, masterBoolQuery);

		if (andQueries != null && !nonNestedAnd.isEmpty()) {
			BoolQuery.Builder boolQuery = new BoolQuery.Builder();
			for (MapBoolQuery query : nonNestedAnd) {
				if (query.getValues() != null)
					boolQuery.must(getTermsQuery(query));
				else
					boolQuery.mustNot(getExistsQuery(query));
			}
			masterBoolQuery.must(boolQuery.build()._toQuery());
		}

		List<MapOrBoolQuery> nonNestedOrList = orQueries.stream()
				.filter(p -> (p.getPath() == null || p.getPath().isEmpty())).collect(Collectors.toList());

		List<MapOrBoolQuery> nestedOrList = orQueries.stream()
				.filter(p -> (p.getPath() != null && !p.getPath().isEmpty())).collect(Collectors.toList());

		buildNestedBoolOrQuery(nestedOrList, masterBoolQuery);

		if (orQueries != null && !nonNestedOrList.isEmpty()) {
			BoolQuery.Builder boolQuery = new BoolQuery.Builder();
			for (MapBoolQuery query : nonNestedOrList) {
				if (query.getValues() != null)
					boolQuery.should(getTermsQuery(query));
				else
					boolQuery.mustNot(getExistsQuery(query));
			}
			masterBoolQuery.must(boolQuery.build()._toQuery());
		}
	}

	private void combinationNestedQuery(BoolQuery.Builder masterBoolQuery, BoolQuery.Builder nestedBoolQuery,
			String nestedPath) {
		String regex = "(.)*(\\d)(.)*";
		Pattern pattern = Pattern.compile(regex);
		if (StringUtils.isNumeric(nestedPath)) {
			masterBoolQuery.must(nestedBoolQuery.build()._toQuery());
		} else {
			if (pattern.matcher(nestedPath).matches()) {
				List<String> list = Arrays.asList(nestedPath.split("\\.")).subList(0,
						(nestedPath.split("\\.").length - 1));
				nestedPath = String.join(".", list);
			}
			final String path = nestedPath;
			masterBoolQuery.must(NestedQuery
					.of(n -> n.path(path).query(nestedBoolQuery.build()._toQuery())
							.scoreMode(co.elastic.clients.elasticsearch._types.query_dsl.ChildScoreMode.None))
					._toQuery());
		}
	}

	private void buildNestedBoolAndQuery(List<MapAndBoolQuery> nestedAnd, BoolQuery.Builder masterBoolQuery) {

		Map<String, List<MapAndBoolQuery>> nestedGroupAndByList = nestedAnd.stream()
				.collect(Collectors.groupingBy(w -> w.getPath()));

		for (Entry<String, List<MapAndBoolQuery>> item : nestedGroupAndByList.entrySet()) {
			BoolQuery.Builder nestedBoolQuery = new BoolQuery.Builder();
			String nestedPath = item.getKey();

			item.getValue().forEach(qry -> {
				qry.setPath(null);
				if (qry.getValues() != null)
					nestedBoolQuery.must(getTermsQuery(qry));
				else
					nestedBoolQuery.mustNot(getExistsQuery(qry));
			});

			combinationNestedQuery(masterBoolQuery, nestedBoolQuery, nestedPath);
		}
	}

	private void buildNestedBoolOrQuery(List<MapOrBoolQuery> nestedOr, BoolQuery.Builder masterBoolQuery) {

		Map<String, List<MapOrBoolQuery>> nestedGroupAndByList = nestedOr.stream()
				.collect(Collectors.groupingBy(w -> w.getPath()));

		for (Entry<String, List<MapOrBoolQuery>> item : nestedGroupAndByList.entrySet()) {
			BoolQuery.Builder nestedBoolQuery = new BoolQuery.Builder();
			String nestedPath = item.getKey();

			item.getValue().forEach(qry -> {
				qry.setPath(null);
				if (qry.getValues() != null)
					nestedBoolQuery.should(getTermsQuery(qry));
				else
					nestedBoolQuery.mustNot(getExistsQuery(qry));
			});

			combinationNestedQuery(masterBoolQuery, nestedBoolQuery, nestedPath);
		}
	}

	private void buildNestedRangeAndQuery(List<MapAndRangeQuery> nestedRangeand, BoolQuery.Builder masterBoolQuery) {

		Map<String, List<MapAndRangeQuery>> nestedGroupAndByList = nestedRangeand.stream()
				.collect(Collectors.groupingBy(w -> w.getPath()));

		for (Entry<String, List<MapAndRangeQuery>> item : nestedGroupAndByList.entrySet()) {
			BoolQuery.Builder nestedBoolQuery = new BoolQuery.Builder();
			String nestedPath = item.getKey();

			item.getValue().forEach(qry -> {
				qry.setPath(null);
				if (qry.getEnd() == null && qry.getStart() != null) {
					MapAndBoolQuery boolqry = new MapAndBoolQuery();
					List<Object> list = new ArrayList<>();
					list.add(qry.getStart());
					boolqry.setKey(qry.getKey());
					boolqry.setValues(list);
					nestedBoolQuery.must(getTermsQuery(boolqry));
				} else {
					nestedBoolQuery.must(getRangeQuery(qry));
				}
			});

			combinationNestedQuery(masterBoolQuery, nestedBoolQuery, nestedPath);
		}
	}

	private void buildNestedRangeOrQuery(List<MapOrRangeQuery> nestedRangeOr, BoolQuery.Builder masterBoolQuery) {

		Map<String, List<MapOrRangeQuery>> nestedGroupOrByList = nestedRangeOr.stream()
				.collect(Collectors.groupingBy(w -> w.getPath()));

		for (Entry<String, List<MapOrRangeQuery>> item : nestedGroupOrByList.entrySet()) {
			BoolQuery.Builder nestedBoolQuery = new BoolQuery.Builder();
			String nestedPath = item.getKey();

			item.getValue().forEach(qry -> {
				qry.setPath(null);
				if (qry.getEnd() == null && qry.getStart() != null) {
					MapAndBoolQuery boolqry = new MapAndBoolQuery();
					List<Object> list = new ArrayList<>();
					list.add(qry.getStart());
					boolqry.setKey(qry.getKey());
					boolqry.setValues(list);
					nestedBoolQuery.should(getTermsQuery(boolqry));
				} else {
					nestedBoolQuery.should(getRangeQuery(qry));
				}
			});

			combinationNestedQuery(masterBoolQuery, nestedBoolQuery, nestedPath);
		}
	}

	private void buildNestedMatchPhraseAndQuery(List<MapAndMatchPhraseQuery> nestedAnd,
			BoolQuery.Builder masterBoolQuery) {

		Map<String, List<MapAndMatchPhraseQuery>> nestedGroupAndByList = nestedAnd.stream()
				.collect(Collectors.groupingBy(w -> w.getPath()));

		for (Entry<String, List<MapAndMatchPhraseQuery>> item : nestedGroupAndByList.entrySet()) {
			BoolQuery.Builder nestedBoolQuery = new BoolQuery.Builder();
			String nestedPath = item.getKey();

			item.getValue().forEach(qry -> {
				qry.setPath(null);
				if (qry.getValue() != null)
					nestedBoolQuery.must(getMatchPhraseQuery(qry));
				else
					nestedBoolQuery.mustNot(getExistsQuery(qry));
			});

			combinationNestedQuery(masterBoolQuery, nestedBoolQuery, nestedPath);
		}
	}

	private void buildNestedMatchPhraseOrQuery(List<MapOrMatchPhraseQuery> nestedor,
			BoolQuery.Builder masterBoolQuery) {

		Map<String, List<MapOrMatchPhraseQuery>> nestedGroupAndByList = nestedor.stream()
				.collect(Collectors.groupingBy(w -> w.getPath()));

		for (Entry<String, List<MapOrMatchPhraseQuery>> item : nestedGroupAndByList.entrySet()) {
			BoolQuery.Builder nestedBoolQuery = new BoolQuery.Builder();
			String nestedPath = item.getKey();

			item.getValue().forEach(qry -> {
				qry.setPath(null);
				if (qry.getValue() != null)
					nestedBoolQuery.should(getMatchPhraseQuery(qry));
				else
					nestedBoolQuery.mustNot(getExistsQuery(qry));
			});

			combinationNestedQuery(masterBoolQuery, nestedBoolQuery, nestedPath);
		}
	}

	private void buildRangeQueries(List<MapAndRangeQuery> andQueries, List<MapOrRangeQuery> orQueries,
			BoolQuery.Builder masterBoolQuery) {

		if (andQueries != null) {
			List<MapAndRangeQuery> nonNestedOrList = andQueries.stream()
					.filter(p -> (p.getPath() == null || p.getPath().isEmpty())).collect(Collectors.toList());

			List<MapAndRangeQuery> nestedOrList = andQueries.stream()
					.filter(p -> (p.getPath() != null && !p.getPath().isEmpty())).collect(Collectors.toList());

			buildNestedRangeAndQuery(nestedOrList, masterBoolQuery);

			BoolQuery.Builder boolQuery = new BoolQuery.Builder();
			for (MapAndRangeQuery query : nonNestedOrList) {
				if (query.getStart() != null && query.getEnd() != null)
					boolQuery.must(getRangeQuery(query));
			}
			if (!nonNestedOrList.isEmpty())
				masterBoolQuery.must(boolQuery.build()._toQuery());
		}

		if (orQueries != null) {
			List<MapOrRangeQuery> nonNestedOrList = orQueries.stream()
					.filter(p -> (p.getPath() == null || p.getPath().isEmpty())).collect(Collectors.toList());

			List<MapOrRangeQuery> nestedOrList = orQueries.stream()
					.filter(p -> (p.getPath() != null && !p.getPath().isEmpty())).collect(Collectors.toList());

			buildNestedRangeOrQuery(nestedOrList, masterBoolQuery);

			BoolQuery.Builder boolQuery = new BoolQuery.Builder();
			for (MapOrRangeQuery query : nonNestedOrList) {
				if (query.getStart() != null && query.getEnd() != null)
					boolQuery.should(getRangeQuery(query));
			}
			if (!nonNestedOrList.isEmpty())
				masterBoolQuery.must(boolQuery.build()._toQuery());
		}
	}

	private void buildExistsQueries(List<MapExistQuery> andExistQueries, BoolQuery.Builder masterBoolQuery) {

		if (andExistQueries != null) {
			BoolQuery.Builder boolQuery = new BoolQuery.Builder();
			for (MapExistQuery query : andExistQueries) {
				if (query.isExists())
					boolQuery.must(getExistsQuery(query));
				else
					boolQuery.mustNot(getExistsQuery(query));
			}
			masterBoolQuery.must(boolQuery.build()._toQuery());
		}
	}

	private void buildMatchPhraseQueries(List<MapAndMatchPhraseQuery> andQueries, List<MapOrMatchPhraseQuery> orQueries,
			BoolQuery.Builder masterBoolQuery) {

		if (andQueries != null) {
			List<MapAndMatchPhraseQuery> nonNestedOrList = andQueries.stream()
					.filter(p -> (p.getPath() == null || p.getPath().isEmpty())).collect(Collectors.toList());

			List<MapAndMatchPhraseQuery> nestedOrList = andQueries.stream()
					.filter(p -> (p.getPath() != null && !p.getPath().isEmpty())).collect(Collectors.toList());

			buildNestedMatchPhraseAndQuery(nestedOrList, masterBoolQuery);

			BoolQuery.Builder boolQuery = new BoolQuery.Builder();
			for (MapAndMatchPhraseQuery query : nonNestedOrList) {
				if (query.getValue() != null)
					boolQuery.must(getMatchPhraseQuery(query));
				else
					boolQuery.mustNot(getExistsQuery(query));
			}
			if (!nonNestedOrList.isEmpty())
				masterBoolQuery.must(boolQuery.build()._toQuery());
		}

		if (orQueries != null) {
			List<MapOrMatchPhraseQuery> nonNestedOrList = orQueries.stream()
					.filter(p -> (p.getPath() == null || p.getPath().isEmpty())).collect(Collectors.toList());

			List<MapOrMatchPhraseQuery> nestedOrList = orQueries.stream()
					.filter(p -> (p.getPath() != null && !p.getPath().isEmpty())).collect(Collectors.toList());

			buildNestedMatchPhraseOrQuery(nestedOrList, masterBoolQuery);

			BoolQuery.Builder boolQuery = new BoolQuery.Builder();
			for (MapOrMatchPhraseQuery query : nonNestedOrList) {
				if (query.getValue() != null)
					boolQuery.should(getMatchPhraseQuery(query));
				else
					boolQuery.mustNot(getExistsQuery(query));
			}
			if (!nonNestedOrList.isEmpty())
				masterBoolQuery.must(boolQuery.build()._toQuery());
		}
	}

	protected Query getBoolQuery(MapSearchQuery searchQuery) {

		BoolQuery.Builder masterBoolQuery = new BoolQuery.Builder();

		if (searchQuery == null)
			return masterBoolQuery.build()._toQuery();

		buildBoolQueries(searchQuery.getAndBoolQueries(), searchQuery.getOrBoolQueries(), masterBoolQuery);
		buildRangeQueries(searchQuery.getAndRangeQueries(), searchQuery.getOrRangeQueries(), masterBoolQuery);
		buildExistsQueries(searchQuery.getAndExistQueries(), masterBoolQuery);
		buildMatchPhraseQueries(searchQuery.getAndMatchPhraseQueries(), searchQuery.getOrMatchPhraseQueries(),
				masterBoolQuery);
		return masterBoolQuery.build()._toQuery();
	}

	public Query getBoolQueryBuilderObservationPan(String id, Boolean isMaxVotedRecoId) {

		if (isMaxVotedRecoId)
			return MatchPhraseQuery.of(m -> m.field("max_voted_reco.id").query(id))._toQuery();
		else
			return MatchPhraseQuery.of(m -> m.field("max_voted_reco.hierarchy.taxon_id").query(id))._toQuery();
	}

	protected Aggregation getGeoGridAggregation(String field, Integer precision) {
		if (field == null)
			return null;

		final int precisionValue = precision != null ? precision : 1;

		GeoHashPrecision geoHashPrecision = GeoHashPrecision.of(g -> g.geohashLength(precisionValue));

		return GeoHashGridAggregation.of(g -> g.field(field).precision(geoHashPrecision))._toAggregation();
	}

	protected Aggregation getTermsAggregation(String field, String subField, Integer size) {
		TermsAggregation.Builder termsBuilder = new TermsAggregation.Builder().field(field).size(size)
				.shardSize(SHARD_SIZE);

		if (subField != null && !subField.isEmpty()) {
			return Aggregation.of(a -> a.terms(termsBuilder.build())
					.aggregations(Map.of(subField, Aggregation.of(sub -> sub.terms(t -> t.field(subField))))));
		} else {
			return Aggregation.of(a -> a.terms(termsBuilder.build()));
		}
	}

	protected void applyMapBounds(MapSearchParams searchParams, BoolQuery.Builder masterBoolQuery,
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
			// GeoPolygonQuery is deprecated - use GeoShape with polygon instead
			logger.warn("GeoPolygon query converted to GeoShape query for ES 9 compatibility");
			try {
				applyGeoShapePolygonQuery(polygon, masterBoolQuery, geoAggregationField);
			} catch (IOException e) {
				logger.error("Error applying geo shape polygon query", e);
			}
		}
	}

	private void applyGeoShapePolygonQuery(List<MapGeoPoint> polygon, BoolQuery.Builder masterBoolQuery, String field)
			throws IOException {

		List<List<Double>> coordinates = new ArrayList<>();
		for (MapGeoPoint point : polygon) {
			coordinates.add(Arrays.asList(point.getLon(), point.getLat()));
		}

		// Close polygon
		if (!polygon.isEmpty()) {
			MapGeoPoint first = polygon.get(0);
			coordinates.add(Arrays.asList(first.getLon(), first.getLat()));
		}

		List<List<List<Double>>> wrappedCoordinates = new ArrayList<>();
		wrappedCoordinates.add(coordinates);

		// ✅ Build GeoJSON
		Map<String, Object> geoJson = new HashMap<>();
		geoJson.put("type", "Polygon");
		geoJson.put("coordinates", wrappedCoordinates);

		// ✅ Wrap into GeoShapeFieldQuery
		GeoShapeFieldQuery fieldQuery = GeoShapeFieldQuery.of(f -> f.shape(JsonData.of(geoJson)));

		masterBoolQuery.filter(GeoShapeQuery.of(g -> g.field(field).shape(fieldQuery) // ✅ correct type
		)._toQuery());
	}

	protected void applyShapeFilter(MapSearchParams searchParams, BoolQuery.Builder masterBoolQuery,
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

	protected void applyGeoPolygonQuery(List<MapGeoPoint> polygon, BoolQuery.Builder masterBoolQuery,
			String geoShapeFilterField) throws IOException {

		// Convert to coordinate list
		List<List<Double>> coordinates = new ArrayList<>();
		for (MapGeoPoint point : polygon) {
			coordinates.add(Arrays.asList(point.getLon(), point.getLat()));
		}

		// Close polygon
		if (!polygon.isEmpty()) {
			MapGeoPoint first = polygon.get(0);
			coordinates.add(Arrays.asList(first.getLon(), first.getLat()));
		}

		// Wrap coordinates
		List<List<List<Double>>> wrappedCoordinates = new ArrayList<>();
		wrappedCoordinates.add(coordinates);

		// Build GeoJSON
		Map<String, Object> geoJson = new HashMap<>();
		geoJson.put("type", "Polygon");
		geoJson.put("coordinates", wrappedCoordinates);

		// Wrap into GeoShapeFieldQuery
		GeoShapeFieldQuery shapeQuery = GeoShapeFieldQuery.of(f -> f.shape(JsonData.of(geoJson)));

		masterBoolQuery.minimumShouldMatch("1");

		masterBoolQuery.should(GeoShapeQuery.of(g -> g.field(geoShapeFilterField).shape(shapeQuery))._toQuery());
	}

	protected void applyMultiPolygonQuery(List<List<MapGeoPoint>> multipolygon, BoolQuery.Builder masterBoolQuery,
			String geoShapeFilterField) throws IOException {

		multipolygon.forEach(item -> {
			try {
				applyGeoPolygonQuery(item, masterBoolQuery, geoShapeFilterField);
			} catch (IOException e) {
				logger.error(e.getMessage());
			}
		});
	}

	// FIXED: Corrected GeoBoundingBoxQuery with GeoLocation wrapper
	protected void applyMapBounds(MapBounds bounds, BoolQuery.Builder masterBoolQuery, String geoAggregationField) {

		if (bounds != null) {
			masterBoolQuery
					.filter(GeoBoundingBoxQuery
							.of(g -> g.field(geoAggregationField).boundingBox(b -> b.tlbr(tlbr -> tlbr
									.topLeft(GeoLocation.of(gl -> gl.latlon(
											LatLonGeoLocation.of(ll -> ll.lat(bounds.getTop()).lon(bounds.getLeft())))))
									.bottomRight(GeoLocation.of(gl -> gl.latlon(LatLonGeoLocation
											.of(ll -> ll.lat(bounds.getBottom()).lon(bounds.getRight()))))))))
							._toQuery());
		}
	}

}