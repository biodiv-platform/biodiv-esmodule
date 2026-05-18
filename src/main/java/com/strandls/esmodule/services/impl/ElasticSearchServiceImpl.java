package com.strandls.esmodule.services.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.logging.log4j.core.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

// ES 9 Client and Core
import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.*;
import co.elastic.clients.elasticsearch.core.search.*;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.util.NamedValue;
import co.elastic.clients.elasticsearch._types.Script;

import com.strandls.es.ElasticSearchClient;
import com.strandls.esmodule.Constants;
import com.strandls.esmodule.ESmoduleConfig;
import com.strandls.esmodule.indexes.pojo.ExtendedTaxonDefinition;
import com.strandls.esmodule.models.AggregationResponse;
import com.strandls.esmodule.models.AuthorUploadedObservationInfo;
import com.strandls.esmodule.models.CustomFieldValues;
import com.strandls.esmodule.models.CustomFields;
import com.strandls.esmodule.models.DayAggregation;
import com.strandls.esmodule.models.FilterPanelData;
import com.strandls.esmodule.models.GeoHashAggregationData;
import com.strandls.esmodule.models.IdentifiersInfo;
import com.strandls.esmodule.models.Location;
import com.strandls.esmodule.models.MapDocument;
import com.strandls.esmodule.models.MapQueryResponse;
import com.strandls.esmodule.models.MapQueryStatus;
import com.strandls.esmodule.models.MapResponse;
import com.strandls.esmodule.models.MapSearchParams;
import com.strandls.esmodule.models.MapSortType;
import com.strandls.esmodule.models.MaxVotedReco;
import com.strandls.esmodule.models.MaxVotedRecoFreq;
import com.strandls.esmodule.models.MonthAggregation;
import com.strandls.esmodule.models.ObservationInfo;
import com.strandls.esmodule.models.ObservationLatLon;
import com.strandls.esmodule.models.ObservationMapInfo;
import com.strandls.esmodule.models.ObservationNearBy;
import com.strandls.esmodule.models.SimilarObservation;
import com.strandls.esmodule.models.SpeciesGroup;
import com.strandls.esmodule.models.TaxonomyUpdateData;
import com.strandls.esmodule.models.TraitValue;
import com.strandls.esmodule.models.Traits;
import com.strandls.esmodule.models.UploadersInfo;
import com.strandls.esmodule.models.UserGroup;
import com.strandls.esmodule.models.query.MapBoolQuery;
import com.strandls.esmodule.models.query.MapRangeQuery;
import com.strandls.esmodule.models.query.MapSearchQuery;
import com.strandls.esmodule.services.ElasticSearchService;

import jakarta.inject.Inject;
import jakarta.json.stream.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;

/**
 * Implementation of {@link ElasticSearchService}
 *
 * @author mukund
 *
 */
public class ElasticSearchServiceImpl extends ElasticSearchQueryUtil implements ElasticSearchService {

	@Inject
	private ElasticSearchClient client;

	@Inject
	private ObjectMapper objectMapper;

	private final Logger logger = LoggerFactory.getLogger(ElasticSearchServiceImpl.class);

	private static final Integer TOTAL_USER_UPPER_BOUND = 20000;

	private static final String USERGROUP = "userGroup";

	List<String> months = Arrays.asList("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov",
			"Dec");

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * com.strandls.naksha.es.services.api.ElasticSearchService#create(java.lang.
	 * String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public MapQueryResponse create(String index, String type, String documentId, String document) throws IOException {
		String indexParam = index.replaceAll("[\n\r\t]", "_");
		String typeParam = type.replaceAll("[\n\r\t]", "_");
		String documentIdParam = documentId.replaceAll("[\n\r\t]", "_");
		logger.info("Trying to create index: {}, type: {} & id: {}", indexParam, typeParam, documentIdParam);

		Map<String, Object> docMap = objectMapper.readValue(document, Map.class);

		co.elastic.clients.elasticsearch.core.IndexResponse response = client.getClient()
				.index(i -> i.index(index).id(documentId).document(docMap));

		MapQueryStatus queryStatus = MapQueryStatus.valueOf(response.result().name());

		logger.info("Created index: {}, type: {} & id: {} with status {}", indexParam, typeParam, documentIdParam,
				queryStatus);

		return new MapQueryResponse(queryStatus, "");
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * com.strandls.naksha.es.services.api.ElasticSearchService#fetch(java.lang.
	 * String, java.lang.String, java.lang.String)
	 */
	@Override
	public MapDocument fetch(String index, String type, String documentId) throws IOException {
		String indexParam = index.replaceAll("[\n\r\t]", "_");
		String typeParam = type.replaceAll("[\n\r\t]", "_");
		String documentIdParam = documentId.replaceAll("[\n\r\t]", "_");

		logger.info("Trying to fetch index: {}, type: {} & id: {}", indexParam, typeParam, documentIdParam);

		GetResponse<Map> response = client.getClient().get(g -> g.index(index).id(documentId), Map.class);

		logger.info("Fetched index: {}, type: {} & id: {} with status {}", indexParam, typeParam, documentIdParam,
				response.found());

		if (response.found() && response.source() != null) {
			String jsonString = objectMapper.writeValueAsString(response.source());
			return new MapDocument(jsonString);
		}
		return new MapDocument(null);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * com.strandls.naksha.es.services.api.ElasticSearchService#update(java.lang.
	 * String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public MapQueryResponse update(String index, String type, String documentId, Map<String, Object> document)
			throws IOException {
		String indexParam = index.replaceAll("[\n\r\t]", "_");
		String typeParam = type.replaceAll("[\n\r\t]", "_");
		String documentIdParam = documentId.replaceAll("[\n\r\t]", "_");
		logger.info("Trying to update index: {}, type: {} & id: {}", indexParam, typeParam, documentIdParam);

		co.elastic.clients.elasticsearch.core.UpdateResponse<Map> updateResponse = client.getClient()
				.update(u -> u.index(index).id(documentId).doc(document), Map.class);
		MapQueryStatus queryStatus = MapQueryStatus.valueOf(updateResponse.result().name());

		logger.info("Updated index: {}, type: {} & id: {} with status {}", indexParam, typeParam, documentIdParam,
				queryStatus);

		return new MapQueryResponse(queryStatus, "");
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * com.strandls.naksha.es.services.api.ElasticSearchService#delete(java.lang.
	 * String, java.lang.String, java.lang.String)
	 */
	@Override
	public MapQueryResponse delete(String index, String type, String documentId) throws IOException {
		String indexParam = index.replaceAll("[\n\r\t]", "_");
		String typeParam = type.replaceAll("[\n\r\t]", "_");
		String documentIdParam = documentId.replaceAll("[\n\r\t]", "_");

		logger.info("Trying to delete index: {}, type: {} & id: {}", indexParam, typeParam, documentIdParam);

		DeleteResponse deleteResponse = client.getClient().delete(d -> d.index(index).id(documentId));
		MapQueryStatus queryStatus = MapQueryStatus.valueOf(deleteResponse.result().name());

		logger.info("Deleted index: {}, type: {} & id: {} with status {}", indexParam, typeParam, documentIdParam,
				queryStatus);

		return new MapQueryResponse(queryStatus, "");
	}

	private JsonNode[] parseJson(String jsonArray, List<MapQueryResponse> responses) throws IOException {
		ObjectMapper mapper = new ObjectMapper();

		logger.info("DEBUG parseJson: About to parse JSON, input type: {}",
				jsonArray != null ? jsonArray.getClass().getName() : "null");
		logger.info("DEBUG parseJson: Input starts with: {}",
				jsonArray != null && jsonArray.length() > 100 ? jsonArray.substring(0, 100) : jsonArray);

		JsonNode[] jsons = null;
		try {
			jsons = mapper.readValue(jsonArray, JsonNode[].class);
		} catch (JsonParseException e) {
			String detailedError = "Json Parsing Exception: " + e.getMessage();
			logger.error("JSON Parsing Exception during bulk upload parsing:", e);
			responses.add(new MapQueryResponse(MapQueryStatus.JSON_EXCEPTION, detailedError));
		} catch (JsonMappingException e) {
			String detailedError = "Json Mapping Exception: " + e.getMessage();
			logger.error("JSON Mapping Exception during bulk upload parsing:", e);
			responses.add(new MapQueryResponse(MapQueryStatus.JSON_EXCEPTION, detailedError));
		}

		if (jsons != null && !jsons[0].has("id")) {
			responses.add(new MapQueryResponse(MapQueryStatus.NO_ID, "No id field specified"));
		}

		return jsons;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * com.strandls.naksha.es.services.api.ElasticSearchService#bulkUpload(java.lang
	 * .String, java.lang.String, java.lang.String)
	 */
	@Override
	public List<MapQueryResponse> bulkUpload(String index, String type, String jsonArray) throws IOException {
		List<MapQueryResponse> responses = new ArrayList<>();
		String indexParam = index.replaceAll("[\n\r\t]", "_");
		String typeParam = type.replaceAll("[\n\r\t]", "_");
		logger.info("Trying to bulk upload index: {}, type: {}", indexParam, typeParam);

		// DEBUG: Log what we're receiving
		logger.info("DEBUG: jsonArray parameter type: {}", jsonArray != null ? jsonArray.getClass().getName() : "null");
		logger.info("DEBUG: jsonArray parameter value (first 500 chars): {}",
				jsonArray != null && jsonArray.length() > 500 ? jsonArray.substring(0, 500) : jsonArray);
		logger.info("DEBUG: jsonArray starts with '[': {}", jsonArray != null && jsonArray.startsWith("["));
		logger.info("DEBUG: jsonArray length: {}", jsonArray != null ? jsonArray.length() : 0);

		JsonNode[] jsons = parseJson(jsonArray, responses);
		if (!responses.isEmpty()) {
			logger.error("Json exception-{}, while trying to bulk upload for index:{}, type: {}",
					responses.get(0).getMessage(), indexParam, typeParam);
			return responses;
		}

		List<BulkOperation> operations = new ArrayList<>();

		for (JsonNode json : jsons) {
			Map<String, Object> docMap = objectMapper.convertValue(json, Map.class);
			String id = json.get("id").asText();

			operations
					.add(BulkOperation.of(b -> b.index(idx -> idx.index(index).id(id).document(JsonData.of(docMap)))));
		}

		BulkResponse bulkResponse = client.getClient().bulk(br -> br.index(index).operations(operations));

		for (BulkResponseItem item : bulkResponse.items()) {

			StringBuilder failureReason = new StringBuilder();
			MapQueryStatus queryStatus;

			if (item.error() != null) {
				failureReason.append(item.error().reason());
				queryStatus = MapQueryStatus.ERROR;
			} else {
				queryStatus = MapQueryStatus.valueOf(item.result().toUpperCase());
			}

			logger.info(" For index: {}, type: {}, bulk upload id: {}, the status is {}", indexParam, typeParam,
					item.id(), queryStatus);

			responses.add(new MapQueryResponse(queryStatus, failureReason.toString()));
		}

		return responses;
	}

	@Override
	public List<MapQueryResponse> bulkUpdate(String index, String type, List<Map<String, Object>> updateDocs)
			throws IOException {
		String indexParam = index.replaceAll("[\n\r\t]", "_");
		String typeParam = type.replaceAll("[\n\r\t]", "_");
		logger.info("Trying to bulk update index: {}, type: {}", indexParam, typeParam);

		List<BulkOperation> operations = new ArrayList<>();

		for (Map<String, Object> doc : updateDocs) {
			String id = doc.get("id").toString();
			operations.add(BulkOperation.of(b -> b.update(u -> u.index(index).id(id).action(a -> a.doc(doc)))));
		}

		BulkResponse bulkResponse = client.getClient().bulk(br -> br.index(index).operations(operations));

		List<MapQueryResponse> responses = new ArrayList<>();

		for (BulkResponseItem item : bulkResponse.items()) {

			StringBuilder failureReason = new StringBuilder();
			MapQueryStatus queryStatus;

			if (item.error() != null) {
				failureReason.append(item.error().reason());
				queryStatus = MapQueryStatus.ERROR;
			} else {
				queryStatus = MapQueryStatus.valueOf(item.result());
			}

			logger.info(" For index: {}, type: {}, bulk update id: {}, the status is {}", indexParam, typeParam,
					item.id(), queryStatus);

			responses.add(new MapQueryResponse(queryStatus, failureReason.toString()));
		}

		return responses;

	}

	private MapResponse querySearch(String index, Query query, MapSearchParams searchParams, String geoAggregationField,
			Integer geoAggegationPrecision) throws IOException {

		SearchRequest searchRequest = SearchRequest.of(s -> {
			s.index(index).trackTotalHits(t -> t.enabled(true));

			if (query != null) {
				s.query(query);
			}

			if (searchParams.getFrom() != null)
				s.from(searchParams.getFrom());
			if (searchParams.getLimit() != null)
				s.size(searchParams.getLimit());

			if (searchParams.getSortOn() != null) {
				SortOrder order = (searchParams.getSortType() != null && MapSortType.ASC == searchParams.getSortType())
						? SortOrder.Asc
						: SortOrder.Desc;
				s.sort(so -> so.field(f -> f.field(searchParams.getSortOn()).order(order)));
			}

			if (searchParams.getSearchAfter() != null) {
				s.searchAfter(FieldValue.of(searchParams.getSearchAfter()));
			}

			if (geoAggregationField != null) {
				s.aggregations("geo_agg", getGeoGridAggregationBuilder(geoAggregationField, geoAggegationPrecision));
			}

			return s;
		});

		logger.info("ES Request: {}", searchRequest.toString());

		SearchResponse<ObjectNode> searchResponse = client.getClient().search(searchRequest, ObjectNode.class);

		List<MapDocument> result = new ArrayList<>();
		long totalHits = (searchResponse.hits().total() != null) ? searchResponse.hits().total().value() : 0;

		for (Hit<ObjectNode> hit : searchResponse.hits().hits()) {
			if (hit.source() != null) {
				result.add(new MapDocument(hit.source().toString()));
			}
		}

		String aggregationString = null;
		if (geoAggregationField != null && searchResponse.aggregations().containsKey("geo_agg")) {
			Aggregate aggregate = searchResponse.aggregations().get("geo_agg");
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			JsonpMapper mapper = client.getClient()._jsonpMapper();
			try (JsonGenerator generator = mapper.jsonProvider().createGenerator(baos)) {
				mapper.serialize(aggregate, generator);
			}
			aggregationString = baos.toString();
		}

		return new MapResponse(result, totalHits, aggregationString);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * com.strandls.naksha.es.services.api.ElasticSearchService#termSearch(java.lang
	 * .String, java.lang.String, java.lang.String, java.lang.String,
	 * com.strandls.naksha.es.models.MapSearchParams, java.lang.String,
	 * java.lang.Integer)
	 */
	@Override
	public MapResponse termSearch(String index, String type, String key, String value, MapSearchParams searchParams,
			String geoAggregationField, Integer geoAggegationPrecision) throws IOException {
		String indexParam = index.replaceAll("[\n\r\t]", "_");
		String typeParam = type.replaceAll("[\n\r\t]", "_");
		String keyParam = key.replaceAll("[\n\r\t]", "_");
		String valueParam = value != null ? value.replaceAll("[\n\r\t]", "_") : null;

		logger.info("Term search for index: {}, type: {}, key: {}, value: {}", indexParam, typeParam, keyParam,
				valueParam);
		Query query;
		if (value != null) {
			query = TermQuery.of(t -> t.field(key).value(FieldValue.of(value)))._toQuery();
		} else {
			query = BoolQuery.of(b -> b.mustNot(ExistsQuery.of(e -> e.field(key))._toQuery()))._toQuery();
		}

		return querySearch(index, query, searchParams, geoAggregationField, geoAggegationPrecision);

	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * com.strandls.naksha.es.services.api.ElasticSearchService#boolSearch(java.lang
	 * .String, java.lang.String, java.util.List,
	 * com.strandls.naksha.es.models.MapSearchParams, java.lang.String,
	 * java.lang.Integer)
	 */
	@Override
	public MapResponse boolSearch(String index, String type, List<MapBoolQuery> queries, MapSearchParams searchParams,
			String geoAggregationField, Integer geoAggegationPrecision) throws IOException {
		String indexParam = index.replaceAll("[\n\r\t]", "_");
		String typeParam = type.replaceAll("[\n\r\t]", "_");
		logger.info("Bool search for index: {}, type: {}", indexParam, typeParam);
		List<Query> mustQueries = new ArrayList<>();
		for (MapBoolQuery query : queries) {
			if (query.getValues() != null) {
				List<FieldValue> values = query.getValues().stream().map(v -> FieldValue.of(v.toString()))
						.collect(Collectors.toList());
				mustQueries.add(TermsQuery.of(t -> t.field(query.getKey()).terms(tf -> tf.value(values)))._toQuery());
			} else {
				mustQueries.add(BoolQuery.of(b -> b.mustNot(ExistsQuery.of(e -> e.field(query.getKey()))._toQuery()))
						._toQuery());
			}
		}

		Query boolQuery = BoolQuery.of(b -> b.must(mustQueries))._toQuery();

		return querySearch(index, boolQuery, searchParams, geoAggregationField, geoAggegationPrecision);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * com.strandls.naksha.es.services.api.ElasticSearchService#rangeSearch(java.
	 * lang.String, java.lang.String, java.util.List,
	 * com.strandls.naksha.es.models.MapSearchParams, java.lang.String,
	 * java.lang.Integer)
	 */
	@Override
	public MapResponse rangeSearch(String index, String type, List<MapRangeQuery> queries, MapSearchParams searchParams,
			String geoAggregationField, Integer geoAggegationPrecision) throws IOException {

		String indexParam = index.replaceAll("[\n\r\t]", "_");
		String typeParam = type.replaceAll("[\n\r\t]", "_");

		logger.info("Range search for index: {}, type: {}", indexParam, typeParam);

		List<Query> mustQueries = new ArrayList<>();

		for (MapRangeQuery query : queries) {

			String field = query.getKey();
			Object start = query.getStart();
			Object end = query.getEnd();

			mustQueries.add(RangeQuery.of(r -> {

				// 🔹 NUMBER RANGE
				if ((start instanceof Number) || (end instanceof Number)) {
					return r.number(n -> {
						n.field(field);

						if (start != null) {
							n.gte(((Number) start).doubleValue());
						}
						if (end != null) {
							n.lte(((Number) end).doubleValue());
						}

						return n;
					});
				}

				// 🔹 DATE / STRING RANGE (fallback like old ES)
				return r.date(d -> {
					d.field(field);

					if (start != null) {
						d.gte(start.toString());
					}
					if (end != null) {
						d.lte(end.toString());
					}

					return d;
				});

			})._toQuery());
		}

		Query boolQuery = BoolQuery.of(b -> b.must(mustQueries))._toQuery();

		return querySearch(index, boolQuery, searchParams, geoAggregationField, geoAggegationPrecision);
	}

	@Override
	public Map<String, List<DayAggregation>> aggregationByDay(String index, String user) throws IOException {

		Query authorQuery = TermQuery.of(t -> t.field("author_id").value(FieldValue.of(user)))._toQuery();

		SearchResponse<Void> response = client
				.getClient().search(
						s -> s.index(index).query(authorQuery).size(0)
								.aggregations(Constants.TEMPORAL_AGG,
										a -> a.dateHistogram(d -> d.field("created_on")
												.calendarInterval(CalendarInterval.Day).format("yyyy-MM-dd"))),
						Void.class);
		Map<String, List<DayAggregation>> groupbyday = new LinkedHashMap<>();

		if (response.aggregations() != null) {
			Aggregate agg = response.aggregations().get(Constants.TEMPORAL_AGG);
			if (agg != null && agg.isDateHistogram()) {
				DateHistogramAggregate dateHistogram = agg.dateHistogram();

				for (DateHistogramBucket entry : dateHistogram.buckets().array()) {
					String dateStr = entry.keyAsString();
					String year = dateStr.substring(0, 4);
					List<DayAggregation> yeardata = groupbyday.computeIfAbsent(year, k -> new ArrayList<>());

					DayAggregation data = new DayAggregation(dateStr, entry.docCount());
					yeardata.add(data);
				}
			}
		}

		return groupbyday;
	}

	public List<IdentifiersInfo> identifierInfo(String index, String userIds) {
		List<String> l = Arrays.asList(userIds.split(","));
		List<IdentifiersInfo> result = new ArrayList<>();

		for (int i = 0; i < l.size(); i++) {
			String id = l.get(i);
			Query query = TermQuery.of(t -> t.field("all_reco_vote.authors_voted.id").value(FieldValue.of(id)))
					._toQuery();

			try {
				SearchResponse<Map> response = client.getClient().search(s -> s.index(index).query(query).size(1),
						Map.class);
				for (Hit<Map> hit : response.hits().hits()) {
					Map<String, Object> sourceMap = hit.source();
					List<Object> allRecoVote = new ArrayList<>((List<Object>) sourceMap.get("all_reco_vote"));

					for (int n = 0; n < allRecoVote.size(); n++) {
						Map<String, Object> identificationObject = new HashMap<>(
								(Map<String, Object>) allRecoVote.get(n));
						List<Object> authorsVoted = new ArrayList<>(
								(List<Object>) identificationObject.get("authors_voted"));

						for (int k = 0; k < authorsVoted.size(); k++) {
							Map<String, Object> identifier = new HashMap<>((Map<String, Object>) authorsVoted.get(k));
							String authorId = String.valueOf(identifier.get("id"));
							if (authorId.equals(id)) {
								String name = String.valueOf(identifier.get("name"));
								String pic = String.valueOf(identifier.get("profile_pic"));
								Long identifierId = Long.parseLong(String.valueOf(identifier.get("id")));
								IdentifiersInfo identifierInfo = new IdentifiersInfo(name, pic, identifierId);
								result.add(identifierInfo);
								break;
							}
						}
					}
				}
			} catch (Exception e) {
				logger.error(e.getMessage());
			}
		}
		return (result);
	}

	public List<UploadersInfo> uploaderInfo(String index, String userIds) {
		List<String> l = Arrays.asList(userIds.split(","));
		List<UploadersInfo> result = new ArrayList<>();
		String authorIdConstant = "author_id";
		for (int i = 0; i < l.size(); i++) {
			String id = l.get(i);
			Query query = TermQuery.of(t -> t.field(authorIdConstant).value(FieldValue.of(id)))._toQuery();

			try {
				SearchResponse<Map> response = client.getClient().search(s -> s.index(index).query(query).size(1),
						Map.class);
				for (Hit<Map> hit : response.hits().hits()) {
					Map<String, Object> sourceMap = hit.source();
					String name = String.valueOf(sourceMap.get("created_by"));
					String pic = String.valueOf(sourceMap.get("profile_pic"));
					Long authorId = Long.parseLong(String.valueOf(sourceMap.get(authorIdConstant)));
					UploadersInfo uploaderInfo = new UploadersInfo(name, pic, authorId);
					result.add(uploaderInfo);
				}
			} catch (Exception e) {
				logger.error(e.getMessage());
			}
		}
		return (result);
	}

	private MapDocument aggregateSearchGeo(String index, String field, Integer precision, Query query)
			throws IOException {

		if (field == null)
			return null;

		// Convert Integer precision to GeoHashPrecision using the builder pattern
		GeoHashPrecision prec = precision != null ? GeoHashPrecision.of(g -> g.geohashLength(precision))
				: GeoHashPrecision.of(g -> g.geohashLength(1));

		SearchResponse<Void> searchResponse = client.getClient().search(s -> s.index(index).size(0).query(query)
				.aggregations("geohash", a -> a.geohashGrid(g -> g.field(field).precision(prec))), Void.class);

		if (searchResponse.aggregations() != null) {
			Aggregate agg = searchResponse.aggregations().get("geohash");
			// Fix: Use isGeohashGrid() directly without _get()
			if (agg != null && agg.isGeohashGrid()) {
				String result = objectMapper.writeValueAsString(agg.geohashGrid());
				logger.info("Aggregation search: geohash completed");
				return new MapDocument(result);
			}
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * com.strandls.naksha.es.services.api.ElasticSearchService#geohashAggregation(
	 * java.lang.String, java.lang.String, java.lang.String, java.lang.Integer)
	 */
	@Override
	public MapDocument geohashAggregation(String index, String type, String field, Integer precision)
			throws IOException {
		String indexParam = index.replaceAll("[\n\r\t]", "_");
		String typeParam = type.replaceAll("[\n\r\t]", "_");
		String fieldParam = field.replaceAll("[\n\r\t]", "_");
		logger.info("GeoHash aggregation for index: {}, type: {} on field: {} with precision: {}", indexParam,
				typeParam, fieldParam, precision);

		return aggregateSearchGeo(index, field, precision, null);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * com.strandls.naksha.es.services.api.ElasticSearchService#termsAggregation(
	 * java.lang.String, java.lang.String, java.lang.String, java.lang.String,
	 * java.lang.Integer, java.lang.String,
	 * com.strandls.naksha.es.models.query.MapSearchQuery)
	 */
	@Override
	public MapDocument termsAggregation(String index, String type, String field, String subField, Integer size,
			String locationField, MapSearchQuery query) throws IOException {

		if (size == null)
			size = 500;

		// Log parameters (sanitized)
		logger.info("Terms aggregation for index: {}, type: {} on field: {} and sub field: {} with size: {}",
				index.replaceAll("[\n\r\t]", "_"), type.replaceAll("[\n\r\t]", "_"), field.replaceAll("[\n\r\t]", "_"),
				subField.replaceAll("[\n\r\t]", "_"), size.toString().replaceAll("[\n\r\t]", "_"));

		// Build the query using builders
		BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

		// Add search conditions from MapSearchQuery
		addSearchConditions(boolQueryBuilder, query);

		// Apply map bounds if present
		if (query.getSearchParams() != null) {
			applyMapBounds(query.getSearchParams(), boolQueryBuilder, locationField);
		}

		// Build the final query
		Query boolQuery = boolQueryBuilder.build()._toQuery();

		return aggregateSearchTerms(index, field, subField, size, boolQuery);
	}

	private void addSearchConditions(BoolQuery.Builder builder, MapSearchQuery query) {
		Query searchQuery = getBoolQuery(query);
		if (searchQuery != null) {
			builder.must(searchQuery);
		}
	}

	public Double distanceCalculate(Double lat1, Double lon1, Double lat2, Double lon2) {
		Double dist = 0.0;
		if ((lat1.equals(lat2)) && (lon1.equals(lon2))) {
			return dist;
		} else {
			double theta = lon1 - lon2;
			dist = Math.sin(Math.toRadians(lat1)) * Math.sin(Math.toRadians(lat2))
					+ Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.cos(Math.toRadians(theta));
			dist = Math.acos(dist);
			dist = Math.toDegrees(dist);
			dist = dist * 60 * 1.1515; // distance in miles
			dist = dist * 1.609344; // distnace in KM
		}
		return (dist);
	}

	private MapDocument aggregateSearchTerms(String index, String field, String subField, Integer size, Query query)
			throws IOException {

		if (field == null)
			return null;

		SearchResponse<Void> searchResponse;

		if (subField != null) {
			searchResponse = client.getClient().search(s -> {
				var builder = s.index(index).size(0);
				if (query != null) {
					builder = builder.query(query);
				}
				return builder.aggregations(field, a -> a.terms(t -> t.field(field).size(size)).aggregations(subField,
						sa -> sa.terms(t -> t.field(subField).size(10))));
			}, Void.class);
		} else {
			searchResponse = client.getClient().search(s -> {
				var builder = s.index(index).size(0);
				if (query != null) {
					builder = builder.query(query);
				}
				return builder.aggregations(field, a -> a.terms(t -> t.field(field).size(size)));
			}, Void.class);
		}

		if (searchResponse.aggregations() != null) {
			Aggregate agg = searchResponse.aggregations().get(field);
			if (agg != null) {
				String result = objectMapper.writeValueAsString(agg);
				logger.info("Aggregation search: {} completed", field);
				return new MapDocument(result);
			}
		}

		return null;
	}

	@Override
	public AggregationResponse aggregation(String index, String type, MapSearchQuery searchQuery,
			String geoAggregationField, String filter, String geoShapeFilterField) throws IOException {

		String indexParam = sanitize(index);
		String typeParam = sanitize(type);
		logger.info("SEARCH for index: {}, type: {}", indexParam, typeParam);

		MapSearchParams searchParams = searchQuery.getSearchParams();

		// Build bool query
		BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();
		Query searchDsl = getBoolQuery(searchQuery);
		if (searchDsl != null) {
			boolQueryBuilder.must(searchDsl);
		}

		applyMapBounds(searchParams, boolQueryBuilder, geoAggregationField);

		if (geoShapeFilterField != null) {
			applyShapeFilter(searchParams, boolQueryBuilder, geoShapeFilterField);
		}

		Query finalQuery = boolQueryBuilder.build()._toQuery();

		Aggregation aggregation = buildAggregation(filter);

		AggregationResponse aggregationResponse = new AggregationResponse();

		if (filter.equals(Constants.MAX_VOTED_RECO) || filter.equals(Constants.MVR_TAXON_STATUS)) {

			AggregationResponse temp;
			HashMap<Object, Long> merged = new HashMap<>();

			// exists / available
			Aggregation existsAgg = new Aggregation.Builder().filter(f -> f.exists(e -> e.field(filter))).build();

			temp = groupAggregation(index, existsAgg, finalQuery, filter);
			if (temp != null && temp.getGroupAggregation() != null) {
				merged.putAll(temp.getGroupAggregation());
			}

			// missing
			String missingField = filter.equals(Constants.MAX_VOTED_RECO) ? filter + ".id" : filter + ".keyword";

			Aggregation missingAgg = new Aggregation.Builder().missing(m -> m.field(missingField)).build();

			temp = groupAggregation(index, missingAgg, finalQuery, filter);
			if (temp != null && temp.getGroupAggregation() != null) {
				merged.putAll(temp.getGroupAggregation());
			}

			aggregationResponse.setGroupAggregation(merged);

		} else {
			aggregationResponse = groupAggregation(index, aggregation, finalQuery, filter);
		}

		return aggregationResponse;
	}

	private Aggregation buildAggregation(String filter) {

		if (filter.equals(Constants.MVR_SCIENTIFIC_NAME)) {
			return new Aggregation.Builder().terms(t -> t.field(filter).size(50000)).build();

		} else if (filter.equals(Constants.AUTHOR_ID) || filter.equals(Constants.IDENTIFIER_ID)) {
			return new Aggregation.Builder().terms(t -> t.field(filter).size(20000).order(getCountDescOrder())).build();

		} else if (filter.split("\\|")[0].equals("uploaders")) {
			String[] parts = filter.split("\\|");
			boolean speciesSort = parts.length > 1 && "species".equals(parts[1]);
			Map<String, Aggregation> subAggs = new HashMap<>();
			subAggs.put("exact_count", buildExactCountAggregation());

			if (speciesSort) {
				subAggs.put("species_cardinality",
						new Aggregation.Builder().cardinality(
								c -> c.field("max_voted_reco.scientific_name.keyword").precisionThreshold(40000))
								.build());
				return new Aggregation.Builder().terms(
						t -> t.field(Constants.AUTHOR_ID).size(20000).order(getAggDescOrder("species_cardinality")))
						.aggregations(subAggs).build();
			}
			return new Aggregation.Builder()
					.terms(t -> t.field(Constants.AUTHOR_ID).size(20000).order(getCountDescOrder()))
					.aggregations(subAggs).build();

		} else if (filter.split("\\|")[0].equals("identifiers")) {
			String[] parts = filter.split("\\|");
			boolean speciesSort = parts.length > 1 && "species".equals(parts[1]);
			Map<String, Aggregation> subAggs = new HashMap<>();
			subAggs.put("exact_count", buildExactCountAggregation());

			if (speciesSort) {
				subAggs.put("species_cardinality",
						new Aggregation.Builder().cardinality(
								c -> c.field("max_voted_reco.scientific_name.keyword").precisionThreshold(40000))
								.build());
				return new Aggregation.Builder().terms(
						t -> t.field(Constants.IDENTIFIER_ID).size(20000).order(getAggDescOrder("species_cardinality")))
						.aggregations(subAggs).build();
			}
			return new Aggregation.Builder()
					.terms(t -> t.field(Constants.IDENTIFIER_ID).size(20000).order(getCountDescOrder()))
					.aggregations(subAggs).build();

		} else if (filter.contains("nested")) {
			String nestedField = filter.split("\\.")[1];
			String nestedFilter = filter.replace("nested.", "");
			Map<String, Aggregation> subAggs = new HashMap<>();
			subAggs.put(nestedFilter, new Aggregation.Builder().terms(t -> t.field(nestedFilter).size(1000)).build());
			return new Aggregation.Builder().nested(n -> n.path(nestedField)).aggregations(subAggs).build();

		} else if (filter.equals(Constants.GROUP_BY_DAY)) {
			return new Aggregation.Builder()
					.dateHistogram(
							d -> d.field("created_on").calendarInterval(CalendarInterval.Day).format("yyyy-MM-dd"))
					.build();

		} else if (filter.split("\\|")[0].equals("min")) {

			return new Aggregation.Builder().stats(s -> s.field(filter.split("\\|")[1])).build();

		} else if (filter.equals(Constants.GROUP_BY_OBSERVED))

		{
			return new Aggregation.Builder()
					.dateHistogram(
							d -> d.field("from_date").calendarInterval(CalendarInterval.Month).format("yyyy-MMM"))
					.build();

		} else if (filter.equals(Constants.GROUP_BY_TRAITS)) {
			Map<String, Aggregation> subAggs = new HashMap<>();
			subAggs.put(Constants.TEMPORAL_AGG, new Aggregation.Builder().dateHistogram(d -> d.field("from_date")
					.calendarInterval(CalendarInterval.Month).format("yyyy-MMM").minDocCount(1)).build());
			return new Aggregation.Builder().terms(t -> t.field("facts.trait_value.trait_aggregation.raw").size(1000))
					.aggregations(subAggs).build();

		} else if (filter.equals(Constants.GROUP_BY_TAXON)) {
			return new Aggregation.Builder().terms(t -> t.field("max_voted_reco.hierarchy.taxon_id").size(500000))
					.build();

		} else if (filter.split("\\|")[0].equals("taxon_path")) {
			Map<String, Aggregation> subAggs = new HashMap<>();
			subAggs.put("raw_name",
					new Aggregation.Builder().terms(t -> t.field("italicised_form.keyword").size(10)).build());
			return new Aggregation.Builder().terms(t -> t.field("path.keyword").size(200)).aggregations(subAggs)
					.build();

		} else {
			return new Aggregation.Builder().terms(t -> t.field(filter).size(1000)).build();
		}
	}

	private Aggregation buildExactCountAggregation() {
		return new Aggregation.Builder().scriptedMetric(sm -> sm
				.initScript(buildInlineScript("state.unique = new HashSet()"))
				.mapScript(buildInlineScript(
						"if (doc['max_voted_reco.scientific_name.keyword'].size() > 0) {state.unique.add(doc['max_voted_reco.scientific_name.keyword'].value)}"))
				.combineScript(buildInlineScript("return state.unique.size()"))
				.reduceScript(buildInlineScript(
						"long total = 0; for (int i = 0; i < states.length; i++) { total += states[i] } return total;")))
				.build();
	}

	private Script buildInlineScript(String source) {
		return new Script.Builder().lang(ScriptLanguage.Painless)
				.source(new ScriptSource.Builder().scriptString(source).build()).build();
	}

	private List<NamedValue<SortOrder>> getCountDescOrder() {
		return Collections.singletonList(new NamedValue<>("_count", SortOrder.Desc));
	}

	private List<NamedValue<SortOrder>> getAggDescOrder(String aggName) {
		return Collections.singletonList(new NamedValue<>(aggName, SortOrder.Desc));
	}

	private AggregationResponse groupAggregation(String index, Aggregation aggregation, Query query, String filter)
			throws IOException {

		String targetIndex = filter.split("\\|")[0].equals("taxon_path") ? "extended_taxon_definition" : index;

		SearchResponse<Void> response = client.getClient().search(s -> {
			var search = s.index(targetIndex).size(0);

			if (query != null) {
				if (filter.split("\\|")[0].equals("taxon_path")) {
					String[] parts = filter.split("\\|");
					String taxonPathRegex = (parts.length > 1 && !parts[1].isEmpty()) ? parts[1] + "\\.[0-9]+"
							: "[0-9]+(\\.[0-9]+)?";
					search = search.query(q -> q.regexp(r -> r.field("path.keyword").value(taxonPathRegex)));
				} else {
					search = search.query(query);
				}
			}

			return search.aggregations("agg_result", aggregation);
		}, Void.class);

		AggregationResponse result = new AggregationResponse();

		// FIX 1: Change to LinkedHashMap to PRESERVE CHRONOLOGICAL ORDER
		LinkedHashMap<Object, Long> groupAggregation = new LinkedHashMap<>();

		if (response.aggregations() == null) {
			result.setGroupAggregation(groupAggregation);
			return result;
		}

		for (Map.Entry<String, Aggregate> entry : response.aggregations().entrySet()) {
			Aggregate agg = entry.getValue();

			if (filter.split("\\|")[0].equals("taxon_path")) {
				if (agg.isSterms()) {
					for (StringTermsBucket bucket : agg.sterms().buckets().array()) {
						Aggregate subAgg = bucket.aggregations().get("raw_name");
						if (subAgg != null && subAgg.isSterms()) {
							for (StringTermsBucket subBucket : subAgg.sterms().buckets().array()) {
								groupAggregation.put(subBucket.key().stringValue() + '|' + bucket.key().stringValue(),
										(long) 0);
							}
						}
					}
				}
			}

			// FIX 2: Check for Traits FIRST to avoid falling into generic isSterms()
			else if (filter.equals(Constants.GROUP_BY_TRAITS) && agg.isSterms()) {
				for (StringTermsBucket bucket : agg.sterms().buckets().array()) {
					String traitKey = bucket.key().stringValue();
					Aggregate subAgg = bucket.aggregations().get(Constants.TEMPORAL_AGG);

					if (subAgg != null && subAgg.isDateHistogram()) {
						Map<String, Long> monthSumDays = new HashMap<>();
						for (DateHistogramBucket dateBucket : subAgg.dateHistogram().buckets().array()) {
							String dateStr = dateBucket.keyAsString();
							// Extracts 'Jan' from '2026-Jan'
							String monthName = dateStr.contains("-") ? dateStr.split("-")[1] : dateStr;
							monthSumDays.put(monthName,
									monthSumDays.getOrDefault(monthName, 0L) + dateBucket.docCount());
						}

						// Force insertion in Jan, Feb, Mar... order
						for (String month : months) {
							String compositeKey = traitKey + "_" + month;
							groupAggregation.put(compositeKey, monthSumDays.getOrDefault(month, 0L));
						}
					}
				}
			}
			// 3. Handle Min/Max Date (via Stats)
			else if (agg.isStats()) {
				StatsAggregate stats = agg.stats();
				if (stats.min() != null && stats.min() > 0) {
					String minYear = java.time.Instant.ofEpochMilli(stats.min().longValue())
							.atZone(java.time.ZoneId.of("UTC")).getYear() + "";
					groupAggregation.put(minYear, 0L);
				}
				if (stats.max() != null && stats.max() > 0) {
					String maxYear = java.time.Instant.ofEpochMilli(stats.max().longValue())
							.atZone(java.time.ZoneId.of("UTC")).getYear() + "";
					groupAggregation.put(maxYear, 0L);
				}
			}
			// 4. Standard Terms
			else if (agg.isSterms()) {
				for (StringTermsBucket bucket : agg.sterms().buckets().array()) {
					groupAggregation.put(bucket.key().stringValue(), bucket.docCount());
				}
			}
			// 5. Standard Long Terms
			else if (agg.isLterms()) {
				for (LongTermsBucket bucket : agg.lterms().buckets().array()) {
					groupAggregation.put(bucket.key(), bucket.docCount());
				}
			}
			// 6. Standard Date Histogram
			else if (agg.isDateHistogram()) {
				for (DateHistogramBucket bucket : agg.dateHistogram().buckets().array()) {
					groupAggregation.put(bucket.keyAsString(), bucket.docCount());
				}
			}
			// 7. Filter/Missing
			else if (agg.isFilter()) {
				groupAggregation.put(Constants.AVAILABLE, agg.filter().docCount());
			} else if (agg.isMissing()) {
				groupAggregation.put("missing", agg.missing().docCount());
			}
		}

		result.setGroupAggregation(groupAggregation);
		return result;
	}

	@Override
	public MapResponse autocompleteSpeciesContributors(String index, String type, String name) throws IOException {

		SearchResponse<Object> searchResponse = client
				.getClient().search(
						s -> s.index(index).size(10)
								.query(q -> q.bool(b -> b
										.must(mustQueries -> mustQueries.nested(n -> n.path("taxonomy")
												.query(nq -> nq.term(t -> t.field("taxonomy.role.keyword")
														.value("SPECIES CONTRIBUTOR")))
												.scoreMode(ChildScoreMode.None)))
										.must(mustQueries -> mustQueries
												.matchPhrasePrefix(mpp -> mpp.field("user.name").query(name))))),
						Object.class);

		List<MapDocument> result = new ArrayList<>();

		for (Hit<Object> hit : searchResponse.hits().hits()) {
			Object source = hit.source();
			if (source != null) {
				String sourceAsString = objectMapper.writeValueAsString(source);
				result.add(new MapDocument(sourceAsString));
			}
		}

		long totalHits = searchResponse.hits().total() != null ? searchResponse.hits().total().value() : 0;

		return new MapResponse(result, totalHits, null);
	}

	@Override
	public Map<String, List<MonthAggregation>> aggregationByMonth(String index, String user) throws IOException {

		// Build term query for author_id
		Query authorQuery = TermQuery.of(t -> t.field("author_id").value(user)) // FieldValue.of() is not needed in ES9,
																				// just pass the value directly
				._toQuery();

		SearchResponse<Void> response = client
				.getClient().search(
						s -> s.index(index).query(authorQuery).size(0)
								.aggregations(Constants.TEMPORAL_AGG,
										a -> a.dateHistogram(d -> d.field("from_date")
												.calendarInterval(CalendarInterval.Month).format("yyyy-MMM"))),
						Void.class);

		Map<String, List<MonthAggregation>> groupByMonth = new LinkedHashMap<>();

		if (response.aggregations() != null) {
			Aggregate agg = response.aggregations().get(Constants.TEMPORAL_AGG);
			if (agg != null && agg.isDateHistogram()) {
				DateHistogramAggregate dateHistogram = agg.dateHistogram();

				// Safely get buckets - handle both array and keyed buckets
				List<DateHistogramBucket> buckets;
				if (dateHistogram.buckets().isArray()) {
					buckets = dateHistogram.buckets().array();
				} else {
					buckets = new ArrayList<>(dateHistogram.buckets().keyed().values());
				}

				if (!buckets.isEmpty()) {
					DateHistogramBucket lastBucket = buckets.get(buckets.size() - 1);
					Integer yearInterval = 50;
					String currentYear = lastBucket.keyAsString().substring(0, 4);

					for (DateHistogramBucket entry : buckets) {
						String year = entry.keyAsString().substring(0, 4);
						Integer intervaldiff = Integer.parseInt(currentYear) - Integer.parseInt(year);
						Integer intervalId = intervaldiff / yearInterval;
						String intervalKey = String.format("%04d",
								Math.max(Integer.parseInt(currentYear) - ((intervalId + 1) * yearInterval), 0)) + "-"
								+ String.format("%04d", Integer.parseInt(currentYear) - (intervalId * yearInterval));

						List<MonthAggregation> intervaldata = groupByMonth.computeIfAbsent(intervalKey,
								k -> new ArrayList<>());

						String month = entry.keyAsString().substring(5, 8);
						MonthAggregation data = new MonthAggregation(month, year, entry.docCount());
						intervaldata.add(data);
					}
				}
			}
		}

		return groupByMonth;
	}

	@Override
	public MapResponse search(String index, String type, MapSearchQuery searchQuery, String geoAggregationField,
			Integer geoAggegationPrecision, Boolean onlyFilteredAggregation, String termsAggregationField,
			String geoShapeFilterField) throws IOException {

		String indexParam = index.replaceAll("[\n\r\t]", "_");
		logger.info("SEARCH for index: {}", indexParam);

		MapSearchParams searchParams = searchQuery.getSearchParams();

		// 1. Create the base builder
		BoolQuery.Builder masterBoolQueryBuilder = getBoolQueryBuilder(searchQuery);

		// 2. Build the initial immutable Query object
		Query currentQuery = masterBoolQueryBuilder.build()._toQuery();

		Aggregation geoGridAggregation = getGeoGridAggregationBuilder(geoAggregationField, geoAggegationPrecision);

		// 3. Perform initial aggregate search using the immutable Query
		MapDocument aggregateResult = aggregateSearch(indexParam, geoGridAggregation, currentQuery);
		String geohashAggregation = (aggregateResult != null) ? aggregateResult.getDocument().toString() : null;

		String termsAggregation = null;
		if (termsAggregationField != null) {
			termsAggregation = termsAggregation(indexParam, type, termsAggregationField, null, null,
					geoAggregationField, searchQuery).getDocument().toString();
		}

		if (onlyFilteredAggregation != null && onlyFilteredAggregation) {
			BoolQuery.Builder filteredBuilder = new BoolQuery.Builder().must(currentQuery);
			applyMapBounds(searchParams, filteredBuilder, geoAggregationField);

			Query filteredQuery = filteredBuilder.build()._toQuery();

			aggregateResult = aggregateSearch(indexParam, geoGridAggregation, filteredQuery);
			if (aggregateResult != null)
				geohashAggregation = aggregateResult.getDocument().toString();

			return new MapResponse(new ArrayList<>(), 0, geohashAggregation, geohashAggregation, termsAggregation);
		}

		if (geoShapeFilterField != null) {
			BoolQuery.Builder finalBuilder = new BoolQuery.Builder().must(currentQuery);
			applyShapeFilter(searchParams, finalBuilder, geoShapeFilterField);
			currentQuery = finalBuilder.build()._toQuery();
		}

		MapResponse mapResponse = querySearch(indexParam, currentQuery, searchParams, geoAggregationField,
				geoAggegationPrecision);

		mapResponse.setViewFilteredGeohashAggregation(mapResponse.getGeohashAggregation());
		mapResponse.setGeohashAggregation(geohashAggregation);
		mapResponse.setTermsAggregation(termsAggregation);

		return mapResponse;
	}

	private MapDocument aggregateSearch(String index, Aggregation aggQuery, Query query) throws IOException {
		if (aggQuery == null) {
			return null;
		}

		SearchResponse<ObjectNode> response = client.getClient().search(s -> s.index(index).query(query) // Reusing the
				.aggregations("agg_result", aggQuery).size(0), ObjectNode.class);

		Aggregate aggregate = response.aggregations().get("agg_result");
		if (aggregate == null)
			return null;

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		JsonpMapper mapper = client.getClient()._jsonpMapper();
		try (JsonGenerator generator = mapper.jsonProvider().createGenerator(baos)) {
			mapper.serialize(aggregate, generator);
		}

		logger.info("Aggregation search completed for index: {}", index);
		return new MapDocument(baos.toString());
	}

	@Override
	public ObservationInfo getObservationRightPan(String index, String type, String id, Boolean isMaxVotedRecoId)
			throws IOException {

		Query finalQuery = getBoolQueryBuilderObservationPan(id, isMaxVotedRecoId);

		SearchResponse<Map> response = client.getClient().search(
				s -> s.index(index).size(1000)
						.source(src -> src.filter(f -> f.includes(Constants.OBSERVATION_ID, Constants.REPR_IMAGE_URL,
								Constants.MAX_VOTED_RECO, Constants.LOCATION)))
						.query(finalQuery)
						.sort(so -> so.field(f -> f.field("created_on")
								.order(co.elastic.clients.elasticsearch._types.SortOrder.Desc)))
						.aggregations("observed_in_month",
								a -> a.terms(t -> t.field("observed_in_month.keyword").size(1000))),
				Map.class);

		List<SimilarObservation> similarObservation = new ArrayList<>();
		List<ObservationMapInfo> latlon = new ArrayList<>();
		HashMap<Object, Long> groupMonth = new HashMap<>();

		Aggregate monthAgg = response.aggregations().get("observed_in_month");
		if (monthAgg != null && monthAgg.sterms() != null) {
			for (StringTermsBucket bucket : monthAgg.sterms().buckets().array()) {
				groupMonth.put(bucket.key().stringValue(), bucket.docCount());
			}
		}

		for (Hit<Map> hit : response.hits().hits()) {
			if (hit.source() == null) {
				continue;
			}

			Location loc = objectMapper.readValue(objectMapper.writeValueAsString(hit.source().get(Constants.LOCATION)),
					Location.class);

			MaxVotedReco maxVotedReco = objectMapper.readValue(
					objectMapper.writeValueAsString(hit.source().get(Constants.MAX_VOTED_RECO)), MaxVotedReco.class);

			if (maxVotedReco == null) {
				maxVotedReco = new MaxVotedReco();
			}

			Long observationId = Long.parseLong(hit.source().get(Constants.OBSERVATION_ID).toString());

			latlon.add(new ObservationMapInfo(observationId, maxVotedReco.getScientific_name(), loc.getLat(),
					loc.getLon()));

			similarObservation.add(new SimilarObservation(observationId, maxVotedReco.getScientific_name(),
					String.valueOf(hit.source().get(Constants.REPR_IMAGE_URL))));
		}

		return new ObservationInfo(groupMonth, similarObservation, latlon);
	}

	@Override
	public List<ObservationNearBy> observationNearBy(String index, String type, Double lat, Double lon)
			throws IOException {

		logger.info("Observation nearby search for index: {}, type: {}, lat: {}, lon: {}", sanitize(index),
				sanitize(type), lat, lon);

		String[] includes = { Constants.OBSERVATION_ID, Constants.REPR_IMAGE_URL, Constants.MAX_VOTED_RECO,
				Constants.LOCATION, "group_name" };

		SearchResponse<Map> response = client
				.getClient().search(
						s -> s.index(index).size(15)
								.sort(so -> so.geoDistance(g -> g.field(Constants.LOCATION)
										.location(l -> l.latlon(ll -> ll.lat(lat).lon(lon)))
										.order(co.elastic.clients.elasticsearch._types.SortOrder.Asc)
										.unit(co.elastic.clients.elasticsearch._types.DistanceUnit.Kilometers)
										.distanceType(co.elastic.clients.elasticsearch._types.GeoDistanceType.Plane)
										.ignoreUnmapped(true)))
								.source(src -> src.filter(f -> f.includes(Arrays.asList(includes)))),
						Map.class);

		List<ObservationNearBy> nearBy = new ArrayList<>();

		Double distance;
		Double lat2;
		Double lon2;

		for (Hit<Map> hit : response.hits().hits()) {

			if (hit.source() == null) {
				continue;
			}

			Map sourceMap = hit.source();

			Location loc = objectMapper.readValue(objectMapper.writeValueAsString(sourceMap.get(Constants.LOCATION)),
					Location.class);

			MaxVotedReco maxVotedReco = objectMapper.readValue(
					objectMapper.writeValueAsString(sourceMap.get(Constants.MAX_VOTED_RECO)), MaxVotedReco.class);

			if (maxVotedReco == null) {
				maxVotedReco = new MaxVotedReco();
			}

			lat2 = loc.getLat();
			lon2 = loc.getLon();
			distance = distanceCalculate(lat, lon, lat2, lon2);

			nearBy.add(new ObservationNearBy(Long.parseLong(sourceMap.get(Constants.OBSERVATION_ID).toString()),
					maxVotedReco.getScientific_name(), String.valueOf(sourceMap.get(Constants.REPR_IMAGE_URL)),
					distance, sourceMap.get("group_name").toString()));
		}

		Collections.sort(nearBy, (obv1, obv2) -> obv1.getDistance().compareTo(obv2.getDistance()));

		logger.info("Observation nearby search completed. Total results: {}", nearBy.size());

		return nearBy;
	}

	private String normalizeAutocompleteField(String field) {
		if ("common_name".equals(field)) {
			return "common_names.name";
		} else if ("scientific_name".equals(field)) {
			return "name";
		}
		return field;
	}

	private <T> List<T> mapSearchHits(SearchResponse<Map> searchResponse, Class<T> classMapped) {
		List<T> matchedResults = new ArrayList<>();

		for (Hit<Map> hit : searchResponse.hits().hits()) {
			try {
				if (hit.source() != null) {
					matchedResults
							.add(objectMapper.readValue(objectMapper.writeValueAsString(hit.source()), classMapped));
				}
			} catch (Exception e) {
				logger.error("Error mapping search hit: {}", e.getMessage(), e);
			}
		}

		return matchedResults;
	}

	@Override
	public <T> List<T> autoCompletion(String index, String type, String field, String text, Class<T> classMapped) {
		logger.info("inside auto completion method");

		String normalizedField = normalizeAutocompleteField(field);

		try {
			SearchResponse<Map> searchResponse = client.getClient()
					.search(s -> s.index(index).size(100).source(
							src -> src.filter(f -> f.excludes(Arrays.asList(Constants.TIMESTAMP, Constants.VERSION))))
							.query(q -> q.matchPhrase(m -> m.field(normalizedField).query(text))), Map.class);

			return mapSearchHits(searchResponse, classMapped);

		} catch (Exception e) {
			logger.error("Error in autoCompletion: {}", e.getMessage(), e);
		}

		return new ArrayList<>();
	}

	@Override
	public <T> List<T> autoCompletion(String index, String type, String field, String text, String filterField,
			Integer filter, Class<T> classMapped) {

		String normalizedField = normalizeAutocompleteField(field);

		try {
			SearchResponse<Map> searchResponse = client
					.getClient().search(
							s -> s.index(index).size(10000)
									.source(src -> src.filter(
											f -> f.excludes(Arrays.asList(Constants.TIMESTAMP, Constants.VERSION))))
									.query(q -> q.bool(
											b -> b.must(m -> m.matchPhrase(mp -> mp.field(normalizedField).query(text)))
													.filter(f -> f.term(t -> t.field(filterField).value(filter))))),
							Map.class);

			return mapSearchHits(searchResponse, classMapped);

		} catch (Exception e) {
			logger.error("Error in filtered autoCompletion: {}", e.getMessage(), e);
		}

		return new ArrayList<>();
	}

	private List<ExtendedTaxonDefinition> processElasticResponse(SearchResponse<Map> searchResponse) {
		List<ExtendedTaxonDefinition> matchedResults = new ArrayList<>();

		if (searchResponse == null) {
			return matchedResults;
		}

		for (Hit<Map> hit : searchResponse.hits().hits()) {
			try {
				if (hit.source() != null) {
					matchedResults.add(objectMapper.readValue(objectMapper.writeValueAsString(hit.source()),
							ExtendedTaxonDefinition.class));
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}

		return matchedResults;
	}

	@Override
	public List<ExtendedTaxonDefinition> matchPhrase(String index, String type, String scientificName,
			String scientificText, String canonicalName, String canonicalText, Boolean checkOnAllParam) {

		String scientificFieldName = "name.raw";
		String canonicalFieldName = "canonical_form.keyword";

		try {
			BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

			if (Boolean.TRUE.equals(checkOnAllParam)) {
				boolQueryBuilder.must(
						m -> m.match(mm -> mm.field(scientificFieldName).query(scientificText).operator(Operator.And)));
			}

			boolQueryBuilder.must(m -> m.matchPhrase(mp -> mp.field(canonicalFieldName).query(canonicalText)));

			SearchResponse<Map> searchResponse = client.getClient()
					.search(s -> s.index(index).size(10000).source(
							src -> src.filter(f -> f.excludes(Arrays.asList(Constants.TIMESTAMP, Constants.VERSION))))
							.query(boolQueryBuilder.build()._toQuery()), Map.class);

			long totalHits = searchResponse.hits().total() != null ? searchResponse.hits().total().value() : 0L;

			// fallback query if no results
			if (totalHits == 0) {
				searchResponse = client.getClient().search(
						s -> s.index(index).size(10000)
								.source(src -> src
										.filter(f -> f.excludes(Arrays.asList(Constants.TIMESTAMP, Constants.VERSION))))
								.query(q -> q.matchPhrase(mp -> mp.field(canonicalFieldName).query(canonicalText))),
						Map.class);
			}

			totalHits = searchResponse.hits().total() != null ? searchResponse.hits().total().value() : 0L;

			if (totalHits == 0) {
				return new ArrayList<>();
			}

			return processElasticResponse(searchResponse);

		} catch (Exception e) {
			logger.error("Error in matchPhrase: {}", e.getMessage(), e);
		}

		return new ArrayList<>();
	}

	@Override
	public List<LinkedHashMap<String, LinkedHashMap<String, String>>> getTopUsers(String index, String type,
			String sortingValue, Integer topUser, String timeFilter) {

		try {
			// Step 1: Get top user IDs
			SearchResponse<Void> firstResponse = client.getClient().search(s -> {
				var search = s.index(index).size(0);

				if (timeFilter != null) {
					search = search.query(q -> q.bool(
							b -> b.filter(f -> f.range(r -> r.date(d -> d.field("created_on").gte(timeFilter))))));
				}

				return search.aggregations(Constants.GROUP_BY_AUTHOR, buildSortingAggregation(sortingValue, topUser));
			}, Void.class);

			List<Integer> topUserIds = getUserIds(firstResponse);

			if (topUserIds.isEmpty()) {
				return new ArrayList<>();
			}

			// Step 2: Fetch detailed aggregation for those top users
			SearchResponse<Void> secondResponse = client.getClient().search(s -> {
				var search = s.index(index).size(0);

				search = search.query(q -> q.bool(b -> {
					b.filter(f -> f.terms(t -> t.field(Constants.AUTHOR_ID).terms(
							tv -> tv.value(topUserIds.stream().map(FieldValue::of).collect(Collectors.toList())))));

					if (timeFilter != null) {
						b.must(m -> m.bool(bb -> bb
								.filter(f -> f.range(r -> r.date(d -> d.field("created_on").gte(timeFilter))))));
					}

					return b;
				}));

				return search.aggregations(Constants.GROUP_BY_AUTHOR,
						buildDetailedTopUserAggregation(sortingValue, topUser));
			}, Void.class);

			return processAggregationResponse(secondResponse);

		} catch (Exception e) {
			logger.error("Error in getTopUsers: {}", e.getMessage(), e);
		}

		return new ArrayList<>();
	}

	private Aggregation buildSortingAggregation(String sortingValue, Integer topUser) {
		String sortingField = null;

		var groupByAuthor = new Aggregation.Builder()
				.terms(t -> t.field(Constants.AUTHOR_ID).size(TOTAL_USER_UPPER_BOUND));

		Map<String, Aggregation> subAggs = new HashMap<>();

		subAggs.put("group_by_score_category_engagement",
				buildFilterAggregation("score_category.keyword", "Engagement"));

		subAggs.put("group_by_score_category_content", buildFilterAggregation("score_category.keyword", "Content"));

		if (sortingValue != null) {
			if (sortingValue.contains(".")) {
				sortingField = "module_activity_category.keyword";
			} else {
				sortingField = "module.keyword";
			}

			subAggs.put("group_by_module", buildFilterAggregation(sortingField, sortingValue));
		}

		subAggs.put(Constants.ACTIVITY_SCORE, getBucketScriptAggregation());
		subAggs.put("bucket_sorting", getBucketSortAggregation(sortingValue, topUser));

		groupByAuthor.aggregations(subAggs);

		return groupByAuthor.build();
	}

	private Aggregation buildDetailedTopUserAggregation(String sortingValue, Integer topUser) {
		String sortingField = null;

		var groupByAuthor = new Aggregation.Builder()
				.terms(t -> t.field(Constants.AUTHOR_ID).size(TOTAL_USER_UPPER_BOUND));

		Map<String, Aggregation> subAggs = new HashMap<>();

		subAggs.put("group_by_score_category_engagement",
				buildFilterAggregation("score_category.keyword", "Engagement"));

		subAggs.put("group_by_score_category_content", buildFilterAggregation("score_category.keyword", "Content"));

		if (sortingValue != null) {
			if (sortingValue.contains(".")) {
				sortingField = "module_activity_category.keyword";
			} else {
				sortingField = "module.keyword";
			}

			subAggs.put("group_by_module", buildFilterAggregation(sortingField, sortingValue));
		}

		subAggs.put("bucket_by_module", populateDataAggregation());
		subAggs.put(Constants.PROFILE_PIC, buildTermsAggregation("profile_pic.keyword", 100));
		subAggs.put(Constants.AUTHOR_NAME, buildTermsAggregation("name.keyword", 100));
		subAggs.put(Constants.ACTIVITY_SCORE, getBucketScriptAggregation());
		subAggs.put("bucket_sorting", getBucketSortAggregation(sortingValue, topUser));

		groupByAuthor.aggregations(subAggs);

		return groupByAuthor.build();
	}

	private Aggregation buildTermsAggregation(String field, Integer totalBucket) {
		return new Aggregation.Builder().terms(t -> t.field(field).size(totalBucket)).build();
	}

	private Aggregation buildFilterAggregation(String field, String fieldValue) {
		return new Aggregation.Builder().filter(f -> f.term(t -> t.field(field).value(fieldValue))).build();
	}

	private Aggregation populateDataAggregation() {
		Map<String, Aggregation> subAggs = new HashMap<>();
		subAggs.put("bucket_by_activity_category", buildTermsAggregation("activity_category.keyword", 100));

		return new Aggregation.Builder().terms(t -> t.field("module.keyword").size(100)).aggregations(subAggs).build();
	}

	private Aggregation getBucketScriptAggregation() {
		Map<String, String> bucketsPathsMap = new HashMap<>();
		bucketsPathsMap.put("engagement", "group_by_score_category_engagement>_count");
		bucketsPathsMap.put("content", "group_by_score_category_content>_count");

		String scriptText = "double content = params.content > 0 ? params.content : 1; "
				+ "double engagement = params.engagement > 0 ? params.engagement : 1; "
				+ "Math.round(10 * (Math.log10(content) + Math.log10(engagement)))";

		return new Aggregation.Builder().bucketScript(bs -> bs.bucketsPath(bp -> bp.dict(bucketsPathsMap))
				.script(s -> s.source(ss -> ss.scriptString(scriptText)).lang("painless"))).build();
	}

	private Aggregation getBucketSortAggregation(String sortingValue, int topUsers) {
		String sortOnAggregation;

		if (sortingValue == null) {
			sortOnAggregation = Constants.ACTIVITY_SCORE;
		} else {
			sortOnAggregation = "group_by_module>_count";
		}

		return new Aggregation.Builder().bucketSort(bs -> bs
				.sort(so -> so.field(
						f -> f.field(sortOnAggregation).order(co.elastic.clients.elasticsearch._types.SortOrder.Desc)))
				.size(topUsers)).build();
	}

	private List<Integer> getUserIds(SearchResponse<Void> searchResponse) {
		List<Integer> topAuthors = new ArrayList<>();

		if (searchResponse.aggregations() == null) {
			return topAuthors;
		}

		Aggregate authorAgg = searchResponse.aggregations().get(Constants.GROUP_BY_AUTHOR);
		if (authorAgg == null) {
			return topAuthors;
		}

		// Check if the aggregation result is Long Terms (lterms)
		if (authorAgg.isLterms()) {
			for (LongTermsBucket bucket : authorAgg.lterms().buckets().array()) {
				try {
					// For lterms, bucket.key() returns a long
					topAuthors.add((int) bucket.key());
				} catch (Exception e) {
					logger.error("Error parsing Long user ID bucket: {}", e.getMessage(), e);
				}
			}
		}
		// Fallback if it's String Terms (sterms)
		else if (authorAgg.isSterms()) {
			for (StringTermsBucket bucket : authorAgg.sterms().buckets().array()) {
				try {
					topAuthors.add(Integer.parseInt(bucket.key().stringValue()));
				} catch (Exception e) {
					logger.error("Error parsing String user ID bucket: {}", e.getMessage(), e);
				}
			}
		}

		return topAuthors;
	}

	private List<LinkedHashMap<String, LinkedHashMap<String, String>>> processAggregationResponse(
			SearchResponse<Void> searchResponse) {

		List<LinkedHashMap<String, LinkedHashMap<String, String>>> records = new ArrayList<>();

		if (searchResponse.aggregations() == null) {
			return records;
		}

		Aggregate authorAgg = searchResponse.aggregations().get(Constants.GROUP_BY_AUTHOR);
		if (authorAgg == null) {
			return records;
		}

		// Handle Long variant (lterms)
		if (authorAgg.isLterms()) {
			for (LongTermsBucket authorBucket : authorAgg.lterms().buckets().array()) {
				// Pass the aggregations map and the stringified ID
				records.add(processBucketLogic(authorBucket.aggregations(), String.valueOf(authorBucket.key())));
			}
		}
		// Handle String variant (sterms)
		else if (authorAgg.isSterms()) {
			for (StringTermsBucket authorBucket : authorAgg.sterms().buckets().array()) {
				// Pass the aggregations map and the stringified ID
				records.add(processBucketLogic(authorBucket.aggregations(), authorBucket.key().stringValue()));
			}
		}

		return records;
	}

	/**
	 * Helper method that takes the Map of sub-aggregations. This avoids needing a
	 * shared Bucket base class.
	 */
	private LinkedHashMap<String, LinkedHashMap<String, String>> processBucketLogic(Map<String, Aggregate> aggs,
			String authorId) {
		LinkedHashMap<String, LinkedHashMap<String, String>> moduleRecords = new LinkedHashMap<>();

		// 1. Process modules
		Aggregate moduleAgg = aggs.get("bucket_by_module");
		if (moduleAgg != null && moduleAgg.isSterms()) {
			for (StringTermsBucket moduleBucket : moduleAgg.sterms().buckets().array()) {
				LinkedHashMap<String, String> activities = new LinkedHashMap<>();

				Aggregate activityAgg = moduleBucket.aggregations().get("bucket_by_activity_category");
				if (activityAgg != null && activityAgg.isSterms()) {
					for (StringTermsBucket activityBucket : activityAgg.sterms().buckets().array()) {
						activities.put(activityBucket.key().stringValue().toLowerCase(),
								String.valueOf(activityBucket.docCount()));
					}
				}
				moduleRecords.put(moduleBucket.key().stringValue().toLowerCase(), activities);
			}
		}

		// 2. Process User Details
		LinkedHashMap<String, String> userDetails = new LinkedHashMap<>();

		// Author Name
		Aggregate nameAgg = aggs.get(Constants.AUTHOR_NAME);
		if (nameAgg != null && nameAgg.isSterms()) {
			for (StringTermsBucket bucket : nameAgg.sterms().buckets().array()) {
				userDetails.put("authorName", bucket.key().stringValue());
			}
		}

		userDetails.put(Constants.AUTHOR_ID, authorId);

		// Profile Pic
		Aggregate picAgg = aggs.get(Constants.PROFILE_PIC);
		if (picAgg != null && picAgg.isSterms()) {
			for (StringTermsBucket bucket : picAgg.sterms().buckets().array()) {
				userDetails.put("profilePic", bucket.key().stringValue());
			}
		}

		// Activity Score
		Aggregate activityScoreAgg = aggs.get(Constants.ACTIVITY_SCORE);
		if (activityScoreAgg != null && activityScoreAgg.isSimpleValue()) {
			double score = activityScoreAgg.simpleValue().value();
			userDetails.put(Constants.ACTIVITY_SCORE, String.valueOf(Math.max(score, 0.0d)));
		}

		moduleRecords.put("details", userDetails);
		return moduleRecords;
	}

	@Override
	public List<LinkedHashMap<String, LinkedHashMap<String, String>>> getUserScore(String index, String type,
			Integer authorId, String timeFilter) {

		try {
			SearchResponse<Void> searchResponse = client.getClient().search(s -> {
				var search = s.index(index).size(0);

				search = search.query(q -> q.bool(b -> {
					b.filter(f -> f.term(t -> t.field(Constants.AUTHOR_ID).value(authorId)));

					if (timeFilter != null) {
						b.must(m -> m.bool(bb -> bb
								.filter(f -> f.range(r -> r.date(d -> d.field("created_on").gte(timeFilter))))));
					}

					return b;
				}));

				return search.aggregations(Constants.GROUP_BY_AUTHOR, buildDetailedTopUserAggregation(null, 1));
			}, Void.class);

			return processAggregationResponse(searchResponse);

		} catch (Exception e) {
			logger.error("Error in getUserScore: {}", e.getMessage(), e);
		}

		return new ArrayList<>();
	}

	private String cleanAutoCompleteResponse(String[] resRegex, String text) {
		String resp = text;
		for (String filterString : resRegex) {
			resp = resp.replace(filterString + "=", "");
		}
		return resp.replaceAll("[\\[\\]{}]", "");

	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public List<String> getListPageFilterValue(String index, String type, String filterOn, String text) {

		List<String> results = new ArrayList<>();

		try {
			if (filterOn.equalsIgnoreCase("district") || filterOn.equalsIgnoreCase("tahsil")
					|| filterOn.equalsIgnoreCase("tags")) {

				String prefixPath;
				String actualField = filterOn;
				String finalSuggestionName;

				if (filterOn.equalsIgnoreCase("tags")) {
					prefixPath = "tags.";
					actualField = "name";
					finalSuggestionName = "tags";
				} else {
					prefixPath = "location_information.";
					finalSuggestionName = filterOn;
				}

				String suggestField = prefixPath + actualField;

				SearchResponse<Void> searchResponse = client.getClient().search(
						s -> s.index(index).source(src -> src.filter(f -> f.includes(Collections.emptyList())))
								.suggest(sg -> sg.suggesters(finalSuggestionName,
										sug -> sug.prefix(text).completion(
												c -> c.field(suggestField).skipDuplicates(true).size(100)))),
						Void.class);

				if (searchResponse.suggest() != null && searchResponse.suggest().get(finalSuggestionName) != null
						&& !searchResponse.suggest().get(finalSuggestionName).isEmpty()) {

					List<Suggestion<Void>> suggestions = searchResponse.suggest().get(finalSuggestionName);

					for (Suggestion<Void> suggestion : suggestions) {
						if (suggestion.completion() != null) {
							for (CompletionSuggestOption<Void> option : suggestion.completion().options()) {
								if (option.text() != null) {
									results.add(option.text());
								}
							}
						}
					}
				}

			} else if (filterOn.equalsIgnoreCase("reconame")) {

				String field = "all_reco_vote.scientific_name.name";

				SearchResponse<Map> searchResponse = client.getClient()
						.search(s -> s.index(index).size(100).source(src -> src.filter(f -> f.includes(field)))
								.query(q -> q.matchPhrase(m -> m.field(field).query(text))), Map.class);

				for (Hit<Map> hit : searchResponse.hits().hits()) {
					if (hit.source() != null) {
						Collection<Object> values = hit.source().values();
						results.add(values.toString().replaceAll("[\\[\\]{}]", "").split("=")[2]);
					}
				}

			} else {

				String field = filterOn;
				final String finalField = field;

				SearchResponse<Map> searchResponse = client
						.getClient().search(
								s -> s.index(index).size(15).source(src -> src.filter(f -> f.includes(finalField)))
										.query(q -> finalField.contentEquals("user.mobileNumber")
												? q.matchPhrase(m -> m.field(finalField).query(text))
												: q.matchPhrasePrefix(m -> m.field(finalField).query(text))),
								Map.class);

				String[] resRegex = field.split("\\.");

				for (Hit<Map> hit : searchResponse.hits().hits()) {
					if (hit.source() != null) {
						Collection<Object> values = hit.source().values();
						results.add(cleanAutoCompleteResponse(resRegex, values.toString()));
					}
				}
			}

		} catch (Exception e) {
			logger.error("Error in getListPageFilterValue: {}", e.getMessage(), e);
		}

		return results.stream().distinct().sorted().collect(Collectors.toList());
	}

	@Override
	public GeoHashAggregationData getNewGeoAggregation(String index, String type, MapSearchQuery searchQuery,
			String geoAggregationField, Integer geoAggegationPrecision) {

		GeoHashAggregationData geoHashAggData = null;

		try {
			MapSearchParams searchParams = searchQuery.getSearchParams();

			BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();
			Query searchQueryDsl = getBoolQuery(searchQuery);
			if (searchQueryDsl != null) {
				boolQueryBuilder.must(searchQueryDsl);
			}

			applyMapBounds(searchParams, boolQueryBuilder, geoAggregationField);

			Query finalQuery = boolQueryBuilder.build()._toQuery();

			String aggName = geoAggregationField + "-" + geoAggegationPrecision;

			SearchResponse<Void> searchResponse = client.getClient().search(s -> {
				var search = s.index(index).trackTotalHits(t -> t.enabled(true)).size(0);

				if (finalQuery != null) {
					search = search.query(finalQuery);
				}

				return search.aggregations(aggName, a -> a.geohashGrid(g -> g.field(geoAggregationField)
						.precision(GeoHashPrecision.of(p -> p.geohashLength(geoAggegationPrecision)))));
			}, Void.class);

			Long totalCount = searchResponse.hits().total() != null ? searchResponse.hits().total().value() : 0L;

			Map<String, Long> geoHashData = new HashMap<>();

			Aggregate aggregate = searchResponse.aggregations().get(aggName);
			if (aggregate != null && aggregate.geohashGrid() != null) {
				for (GeoHashGridBucket bucket : aggregate.geohashGrid().buckets().array()) {
					geoHashData.put(bucket.key(), bucket.docCount());
				}
			}

			geoHashAggData = new GeoHashAggregationData(geoHashData, totalCount);

		} catch (Exception e) {
			logger.error("Error in getNewGeoAggregation: {}", e.getMessage(), e);
		}

		return geoHashAggData;
	}

	@Override
	public FilterPanelData getListPanel(String index, String type) {
		try {
			SearchResponse<Void> response = client.getClient().search(s -> s.index(index).size(0)
					.aggregations("speciesGroup",
							buildTermsAggregationWithOrder("sgroup_filter.keyword", 100, true, false))
					.aggregations(USERGROUP,
							buildTermsAggregationWithOrder("user_group_observations.ug_filter.keyword", 100, false,
									true))
					.aggregations("trait",
							buildTermsAggregationWithOrder("facts.trait_value.trait_filter.keyword", 1000, true, false))
					.aggregations("state",
							buildTermsAggregationWithOrder("location_information.state.keyword", 100, true, false))
					.aggregations("customField",
							buildTermsAggregationWithOrder(
									"custom_fields.custom_field.custom_field_values.custom_field_filter.keyword", 1000,
									true, false)),
					Void.class);

			FilterPanelData filterPanel = new FilterPanelData();

			filterPanel.setSpeciesGroup(getAggregationSpeciesGroup(response.aggregations().get("speciesGroup")));
			filterPanel.setStates(getAggregationList(response.aggregations().get("state")));
			filterPanel.setUserGroup(getAggregationUserGroup(response.aggregations().get(USERGROUP)));
			filterPanel.setTraits(getTraits(response.aggregations().get("trait")));
			filterPanel.setCustomFields(getCustomFields(response.aggregations().get("customField")));

			return filterPanel;

		} catch (Exception e) {
			logger.error("Error in getListPanel: {}", e.getMessage(), e);
		}

		return null;
	}

	private Aggregation buildTermsAggregationWithOrder(String field, int size, boolean orderByKeyAsc,
			boolean orderByCountDesc) {

		List<NamedValue<co.elastic.clients.elasticsearch._types.SortOrder>> orderList = new ArrayList<>();

		if (orderByKeyAsc) {
			orderList.add(NamedValue.of("_key", co.elastic.clients.elasticsearch._types.SortOrder.Asc));
		}

		if (orderByCountDesc) {
			orderList.add(NamedValue.of("_count", co.elastic.clients.elasticsearch._types.SortOrder.Desc));
		}

		return new Aggregation.Builder().terms(t -> {
			t.field(field).size(size);
			if (!orderList.isEmpty()) {
				t.order(orderList);
			}
			return t;
		}).build();
	}

	private List<String> getAggregationList(Aggregate aggregate) {
		List<String> resultList = new ArrayList<>();

		if (aggregate == null || aggregate.sterms() == null) {
			return resultList;
		}

		for (StringTermsBucket b : aggregate.sterms().buckets().array()) {
			resultList.add(b.key().stringValue());
		}

		return resultList;
	}

	private List<SpeciesGroup> getAggregationSpeciesGroup(Aggregate aggregate) {
		List<SpeciesGroup> sGroup = new ArrayList<>();

		if (aggregate == null || aggregate.sterms() == null) {
			return sGroup;
		}

		for (StringTermsBucket b : aggregate.sterms().buckets().array()) {
			// pattern = sgroupId | sgroupName | sGroupOrder
			String[] sGroupArray = b.key().stringValue().split("\\|");

			if (sGroupArray.length >= 3) {
				sGroup.add(new SpeciesGroup(Long.parseLong(sGroupArray[0]), sGroupArray[1],
						Integer.parseInt(sGroupArray[2])));
			}
		}

		return sGroup;
	}

	private List<UserGroup> getAggregationUserGroup(Aggregate aggregate) {
		List<UserGroup> userGroup = new ArrayList<>();

		if (aggregate == null || aggregate.sterms() == null) {
			return userGroup;
		}

		for (StringTermsBucket b : aggregate.sterms().buckets().array()) {
			// pattern = usergroupId | userGroupName | domain name | webaddress
			String[] ugArray = b.key().stringValue().split("\\|");

			if (ugArray.length >= 4) {
				String webAddress;
				if (ugArray[2] != null && ugArray[2].length() != 0) {
					webAddress = ugArray[2];
				} else {
					webAddress = "/group/" + ugArray[3];
				}

				userGroup.add(new UserGroup(Long.parseLong(ugArray[0]), ugArray[1], webAddress));
			}
		}

		return userGroup;
	}

	private List<Traits> getTraits(Aggregate aggregate) {
		Map<Long, Traits> traitMap = new TreeMap<>();
		List<Traits> traits = new ArrayList<>();

		if (aggregate == null || aggregate.sterms() == null) {
			return traits;
		}

		for (StringTermsBucket b : aggregate.sterms().buckets().array()) {
			String[] traitArray = b.key().stringValue().split("\\|");
			// pattern = traitID | traitName | traitType | traitValue | TraitValueIconURL

			if (traitArray.length >= 5) {
				Long traitId = Long.parseLong(traitArray[0]);

				if (traitMap.containsKey(traitId)) {
					Traits traitMapped = traitMap.get(traitId);
					List<TraitValue> valueList = traitMapped.getTraitValues();
					valueList.add(new TraitValue(traitArray[3], traitArray[4]));
					traitMapped.setTraitValues(valueList);
					traitMap.put(traitId, traitMapped);
				} else {
					List<TraitValue> valueList = new ArrayList<>();
					valueList.add(new TraitValue(traitArray[3], traitArray[4]));

					Traits traitsMapped = new Traits(traitId, traitArray[1], traitArray[2], valueList);

					traitMap.put(traitId, traitsMapped);
				}
			}
		}

		for (Map.Entry<Long, Traits> entry : traitMap.entrySet()) {
			traits.add(entry.getValue());
		}

		return traits;
	}

	private List<CustomFields> getCustomFields(Aggregate aggregate) {
		Map<Long, CustomFields> customFieldMap = new TreeMap<>();
		List<CustomFields> customFieldList = new ArrayList<>();

		if (aggregate == null || aggregate.sterms() == null) {
			return customFieldList;
		}

		for (StringTermsBucket b : aggregate.sterms().buckets().array()) {
			String[] customFieldArray = b.key().stringValue().split("\\|");
			// pattern = cfId | cfName | cfFieldType | cfDataType | cfValueIcon | cfValue

			if (customFieldArray.length >= 6) {
				Long customFieldId = Long.parseLong(customFieldArray[0]);

				if (customFieldMap.containsKey(customFieldId)) {
					CustomFields customFieldMapped = customFieldMap.get(customFieldId);
					List<CustomFieldValues> valueList = customFieldMapped.getValues();

					if (!customFieldArray[2].equalsIgnoreCase("FIELD TEXT")) {
						if (valueList == null) {
							valueList = new ArrayList<>();
						}
						valueList.add(new CustomFieldValues(customFieldArray[5], customFieldArray[4]));
					}

					customFieldMapped.setValues(valueList);
					customFieldMap.put(customFieldId, customFieldMapped);

				} else {
					List<CustomFieldValues> values = null;

					if (!customFieldArray[2].equalsIgnoreCase("FIELD TEXT")) {
						values = new ArrayList<>();
						values.add(new CustomFieldValues(customFieldArray[5], customFieldArray[4]));
					}

					CustomFields customFieldMapped = new CustomFields(customFieldId, customFieldArray[1],
							customFieldArray[2], customFieldArray[3], values);

					customFieldMap.put(customFieldId, customFieldMapped);
				}
			}
		}

		for (Map.Entry<Long, CustomFields> entry : customFieldMap.entrySet()) {
			customFieldList.add(entry.getValue());
		}

		return customFieldList;
	}

	private String toTitleCase(String input) {
		StringBuilder titleCase = new StringBuilder(input.length());
		boolean nextTitleCase = true;

		for (char c : input.toCharArray()) {
			if (Character.isSpaceChar(c)) {
				nextTitleCase = true;
			} else if (nextTitleCase) {
				c = Character.toTitleCase(c);
				nextTitleCase = false;
			}

			titleCase.append(c);
		}

		return titleCase.toString();
	}

	@Override
	public List<ObservationLatLon> getSpeciesCoordinates(String index, String type, String speciesId) {

		try {
			SearchResponse<Map> response = client.getClient()
					.search(s -> s.index(index).size(10000)
							.source(src -> src.filter(f -> f.includes(Constants.OBSERVATION_ID, Constants.LOCATION)))
							.query(q -> q.term(t -> t.field("max_voted_reco.species_id").value(speciesId))), Map.class);

			List<ObservationLatLon> obvList = new ArrayList<>();

			for (Hit<Map> hit : response.hits().hits()) {
				if (hit.source() != null) {
					Location loc = objectMapper.readValue(
							objectMapper.writeValueAsString(hit.source().get(Constants.LOCATION)), Location.class);

					obvList.add(
							new ObservationLatLon(Long.parseLong(hit.source().get(Constants.OBSERVATION_ID).toString()),
									loc.getLat(), loc.getLon()));
				}
			}

			return obvList;

		} catch (Exception e) {
			logger.error("Error in getSpeciesCoordinates: {}", e.getMessage(), e);
		}

		return new ArrayList<>();
	}

	@SuppressWarnings("unchecked")
	@Override
	public String fetchIndex() {
		Map<String, Set<String>> indexOuterLevelProperties = new HashMap<>();

		try {
			GetMappingResponse getMappingResponse = client.getClient().indices().getMapping(g -> g.index("*")
					.allowNoIndices(true).expandWildcards(ExpandWildcard.Open).ignoreUnavailable(true));

			Map<String, IndexMappingRecord> allMappings = getMappingResponse.mappings();

			for (Map.Entry<String, IndexMappingRecord> indexEntry : allMappings.entrySet()) {
				String indexName = indexEntry.getKey();

				if (!indexName.startsWith(".")) {
					TypeMapping mappings = indexEntry.getValue().mappings();

					if (mappings != null && mappings.properties() != null) {
						indexOuterLevelProperties.put(indexName, mappings.properties().keySet());
					}
				}
			}

			return objectMapper.writeValueAsString(indexOuterLevelProperties);

		} catch (IOException e) {
			logger.error("Error in fetchIndex: {}", e.getMessage(), e);
		}

		return null;
	}

	@Override
	public AuthorUploadedObservationInfo getUserData(String index, String type, Long userId, Integer size, Long sGroup,
			Boolean hasMedia) {

		try {
			List<MaxVotedRecoFreq> maxVotedRecoFreqs = new ArrayList<>();

			SearchResponse<Void> response = client.getClient().search(s -> {
				var search = s.index(index).size(0);

				search = search.query(q -> q.bool(b -> {
					b.must(m -> m.term(t -> t.field("author_id").value(userId)));

					if (sGroup != null) {
						b.must(m -> m.term(t -> t.field("group_id").value(sGroup)));
					}

					if (Boolean.TRUE.equals(hasMedia)) {
						b.must(m -> m.term(t -> t.field("no_media").value(0)));
					}

					return b;
				}));

				return search.aggregations("uploadUniqueSpecies",
						new Aggregation.Builder().terms(t -> t.field("max_voted_reco.id").size(50000)
								.order(List.of(NamedValue.of("_count", SortOrder.Desc)))).build());
			}, Void.class);

			Aggregate aggregate = response.aggregations().get("uploadUniqueSpecies");

			if (aggregate == null || aggregate.lterms() == null) {
				return new AuthorUploadedObservationInfo(0L, maxVotedRecoFreqs);
			}

			int count = 1;

			for (LongTermsBucket b : aggregate.lterms().buckets().array()) {
				if (count <= (size - 10)) {
					count++;
				} else {
					if (count > size) {
						break;
					}

					maxVotedRecoFreqs.add(new MaxVotedRecoFreq(b.key(), b.docCount()));
					count++;
				}
			}

			Long total = (long) aggregate.lterms().buckets().array().size();

			return new AuthorUploadedObservationInfo(total, maxVotedRecoFreqs);

		} catch (Exception e) {
			logger.error("Error in getUserData: {}", e.getMessage(), e);
		}

		return null;
	}

	@Override
	public MapResponse autocompleteUserIBP(String index, String type, String userGroupId, String name)
			throws IOException {

		logger.info("Autocomplete user search for index: {}, type: {}, userGroupId: {}, name: {}", sanitize(index),
				sanitize(type), sanitize(userGroupId), sanitize(name));

		// Build bool query
		BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();

		// Add must_not nested query if userGroupId is provided
		if (userGroupId != null && !userGroupId.isEmpty()) {
			boolQueryBuilder.mustNot(q -> q.nested(n -> n.path(USERGROUP)
					.query(nq -> nq.term(t -> t.field("userGroup.usergroupids").value(userGroupId)))
					.scoreMode(ChildScoreMode.None)));
		}

		// Add prefix match on user.name
		if (name != null && !name.isEmpty()) {
			boolQueryBuilder.must(q -> q.matchPhrasePrefix(m -> m.field("user.name").query(name)));
		}

		Query finalQuery = boolQueryBuilder.build()._toQuery();

		SearchResponse<Map> searchResponse = client.getClient().search(s -> s.index(index).size(100).query(finalQuery),
				Map.class);

		List<MapDocument> result = new ArrayList<>();

		for (Hit<Map> hit : searchResponse.hits().hits()) {
			if (hit.source() != null) {
				result.add(new MapDocument(objectMapper.writeValueAsString(hit.source())));
			}
		}

		long totalHits = 0L;
		if (searchResponse.hits().total() != null) {
			totalHits = searchResponse.hits().total().value();
		}

		logger.info("Autocomplete user search completed. Total hits: {}", totalHits);

		return new MapResponse(result, totalHits, null);
	}

	public void asyncUpdateByTaxonId(TaxonomyUpdateData taxonomyData, Query filterQuery, Query speciesQuery)
			throws IOException {

		Map<String, JsonData> params = new HashMap<>();
		params.put("targetId", JsonData.of(taxonomyData.getTargetId()));
		params.put("name", JsonData.of(taxonomyData.getName()));
		params.put("normalized_name", JsonData.of(taxonomyData.getNormalizedName()));
		params.put("old_name", JsonData.of(taxonomyData.getOldName()));
		params.put("italicised_form", JsonData.of(taxonomyData.getItalicisedForm()));
		params.put("canonical_form", JsonData.of(taxonomyData.getCanonicalForm()));
		params.put("position", JsonData.of(taxonomyData.getPosition()));
		params.put("timestamp", JsonData.of(taxonomyData.getTimestamp()));
		ObjectMapper mapper = new ObjectMapper();
		String breadCrumbsJson = mapper.writeValueAsString(taxonomyData.getBreadCrumbs());
		params.put("breadCrumbs", JsonData.fromJson(breadCrumbsJson));
		params.put("rank", JsonData.of(taxonomyData.getRank()));
		params.put("status", JsonData.of(taxonomyData.getStatus()));

		String painlessScript = ESmoduleConfig.fetchFileAsString("scripts/updateObservationTaxonomy.painless");

		Script script = Script.of(
				s -> s.source(src -> src.scriptString(painlessScript)).lang(ScriptLanguage.Painless).params(params));

		UpdateByQueryRequest updateByQueryRequest = UpdateByQueryRequest.of(u -> u.index("extended_observation")
				.conflicts(Conflicts.Proceed).waitForCompletion(false).script(script).query(filterQuery));

		logger.info("UpdateByQueryRequest: {}", updateByQueryRequest.toString());

		UpdateByQueryResponse response = client.getClient().updateByQuery(updateByQueryRequest);
		logger.info("UpdateByQuery Observation task ID: {}", response.task());

		String painlessSpeciesScript = ESmoduleConfig.fetchFileAsString("scripts/updateSpeciesTaxonomy.painless");

		Script speciesScript = Script.of(s -> s.source(src -> src.scriptString(painlessSpeciesScript))
				.lang(ScriptLanguage.Painless).params(params));

		updateByQueryRequest = UpdateByQueryRequest.of(u -> u.index("extended_species").conflicts(Conflicts.Proceed)
				.waitForCompletion(false).script(speciesScript).query(speciesQuery));

		logger.info("UpdateByQueryRequest: {}", updateByQueryRequest.toString());

		response = client.getClient().updateByQuery(updateByQueryRequest);
		logger.info("UpdateByQuery Species task ID: {}", response.task());
	}

	private String sanitize(String value) {
		return value == null ? null : value.replaceAll("[\n\r\t]", "_");
	}

}
