package com.strandls.esmodule.controllers;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.strandls.esmodule.ApiConstants;
import com.strandls.esmodule.ErrorConstants;
import com.strandls.esmodule.indexes.pojo.ElasticIndexes;
import com.strandls.esmodule.indexes.pojo.ExtendedTaxonDefinition;
import com.strandls.esmodule.indexes.pojo.UserScore;
import com.strandls.esmodule.models.AggregationResponse;
import com.strandls.esmodule.models.AuthorUploadedObservationInfo;
import com.strandls.esmodule.models.DayAggregation;
import com.strandls.esmodule.models.FilterPanelData;
import com.strandls.esmodule.models.GeoHashAggregationData;
import com.strandls.esmodule.models.IdentifiersInfo;
import com.strandls.esmodule.models.MapBoundParams;
import com.strandls.esmodule.models.MapBounds;
import com.strandls.esmodule.models.MapDocument;
import com.strandls.esmodule.models.MapQueryResponse;
import com.strandls.esmodule.models.MapResponse;
import com.strandls.esmodule.models.MapSearchParams;
import com.strandls.esmodule.models.MapSortType;
import com.strandls.esmodule.models.MonthAggregation;
import com.strandls.esmodule.models.ObservationInfo;
import com.strandls.esmodule.models.ObservationLatLon;
import com.strandls.esmodule.models.ObservationNearBy;
import com.strandls.esmodule.models.UploadersInfo;
import com.strandls.esmodule.models.query.MapBoolQuery;
import com.strandls.esmodule.models.query.MapRangeQuery;
import com.strandls.esmodule.models.query.MapSearchQuery;
import com.strandls.esmodule.services.ElasticAdminSearchService;
import com.strandls.esmodule.services.ElasticSearchService;
import com.strandls.esmodule.utils.UtilityMethods;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

@Tag(name = "ES services")
@Path(ApiConstants.V1 + ApiConstants.SERVICES)
public class ESController {

	private final Logger logger = LoggerFactory.getLogger(ESController.class);

	@Inject
	public ElasticSearchService elasticSearchService;

	@Inject
	public ElasticAdminSearchService elasticAdminSearchService;

	@Inject
	public UtilityMethods utilityMethods;

	@GET
	@Path(ApiConstants.PING)
	@Produces(MediaType.TEXT_PLAIN)
	@Operation(summary = "Ping", description = "Ping endpoint to test server health")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "pong", content = @Content(mediaType = "text/plain", schema = @Schema(type = "string"))) })
	public String ping() {
		return "PONG";
	}

	@GET
	@Path(ApiConstants.IDENTIFIERSINFO + "/{index}/{userIds}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@Operation(summary = "Fetch details of identifiers", description = "Returns a list of objects containing name, profile pic and author id of identifiers")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(array = @ArraySchema(schema = @Schema(implementation = IdentifiersInfo.class)))),
			@ApiResponse(responseCode = "400", description = "Exception"),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public Response getIdentifierInfo(@PathParam("index") String index, @PathParam("userIds") String userIds) {
		try {
			List<IdentifiersInfo> result = elasticSearchService.identifierInfo(index, userIds);
			return Response.status(Status.OK).entity(result).build();
		} catch (Exception e) {
			return Response.status(Status.BAD_REQUEST).build();
		}
	}

	@GET
	@Path(ApiConstants.UPLOADERSINFO + "/{index}/{userIds}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	@Operation(summary = "Fetch details of uploaders", description = "Returns a list of objects containing name, profile pic and author id of uploaders")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(array = @ArraySchema(schema = @Schema(implementation = UploadersInfo.class)))),
			@ApiResponse(responseCode = "400", description = "Exception"),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public Response getUploaderInfo(@PathParam("index") String index, @PathParam("userIds") String userIds) {
		try {
			List<UploadersInfo> result = elasticSearchService.uploaderInfo(index, userIds);
			return Response.status(Status.OK).entity(result).build();
		} catch (Exception e) {
			return Response.status(Status.BAD_REQUEST).build();
		}
	}

	@POST
	@Path(ApiConstants.DATA + "/{index}/{type}/{documentId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Create Document", description = "Returns succuess failure")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = MapQueryResponse.class))),
			@ApiResponse(responseCode = "400", description = "Exception"),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public MapQueryResponse create(@PathParam("index") String index, @PathParam("type") String type,
			@PathParam("documentId") String documentId, @Parameter(name = "document") MapDocument document) {
		String docString = String.valueOf(document.getDocument());
		try {
			new ObjectMapper().readValue(docString, Map.class);
		} catch (IOException e) {
			throw new WebApplicationException(Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build());
		}
		try {
			return elasticSearchService.create(index, type, documentId, docString);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@GET
	@Path(ApiConstants.DATA + "/{index}/{type}/{documentId}")
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Fetch Document", description = "Returns Document")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = MapDocument.class))),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public MapDocument fetch(@PathParam("index") String index, @PathParam("type") String type,
			@PathParam("documentId") String documentId) {
		try {
			return elasticSearchService.fetch(index, type, documentId);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@PUT
	@Path(ApiConstants.DATA + "/{index}/{type}/{documentId}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Update Document", description = "Returns Success Failure")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = MapQueryResponse.class))),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public MapQueryResponse update(@PathParam("index") String index, @PathParam("type") String type,
			@PathParam("documentId") String documentId, @Parameter(name = "document") Map<String, Object> document) {
		try {
			return elasticSearchService.update(index, type, documentId, document);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@DELETE
	@Path(ApiConstants.DATA + "/{index}/{type}/{documentId}")
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Delete Document", description = "Returns Success Failure")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = MapQueryResponse.class))),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public MapQueryResponse delete(@PathParam("index") String index, @PathParam("type") String type,
			@PathParam("documentId") String documentId) {
		try {
			return elasticSearchService.delete(index, type, documentId);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@POST
	@Path(ApiConstants.BULK_UPLOAD + "/{index}/{type}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Bulk Upload Create Document", description = "Returns Success Failure")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(array = @ArraySchema(schema = @Schema(implementation = MapQueryResponse.class)))),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public List<MapQueryResponse> bulkUpload(@PathParam("index") String index, @PathParam("type") String type,
			@Parameter(name = "jsonArray") String jsonArray) {
		try {
			return elasticSearchService.bulkUpload(index, type, jsonArray);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@PUT
	@Path(ApiConstants.BULK_UPDATE + "/{index}/{type}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Bulk Upload Update Document", description = "Returns Success Failure")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(array = @ArraySchema(schema = @Schema(implementation = MapQueryResponse.class)))),
			@ApiResponse(responseCode = "400", description = "No Documents to update"),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	@SuppressWarnings("resource")
	public List<MapQueryResponse> bulkUpdate(@PathParam("index") String index, @PathParam("type") String type,
			@Parameter(name = "updateDocs") List<Map<String, Object>> updateDocs) {
		if (updateDocs == null)
			throw new WebApplicationException(
					Response.status(Status.BAD_REQUEST).entity("No documents to update").build());
		for (Map<String, Object> doc : updateDocs) {
			if (!doc.containsKey("id"))
				throw new WebApplicationException(Response.status(Status.BAD_REQUEST)
						.entity("Id not present of the document to be updated").build());
		}
		try {
			return elasticSearchService.bulkUpdate(index, type, updateDocs);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@POST
	@Path(ApiConstants.TERM_SEARCH + "/{index}/{type}")
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(operationId = "term_search", summary = "Search a Document", description = "Returns Document")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = MapResponse.class))),
			@ApiResponse(responseCode = "400", description = "key or value not specified"),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public MapResponse search(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("key") String key, @QueryParam("value") String value,
			@QueryParam("geoAggregationField") String geoAggregationField,
			@QueryParam("geoAggegationPrecision") Integer geoAggegationPrecision,
			@Parameter(name = "searchParam") MapSearchParams searchParams) {
		if (key == null || value == null)
			throw new WebApplicationException(
					Response.status(Status.BAD_REQUEST).entity("key or value not specified").build());
		try {
			return elasticSearchService.termSearch(index, type, key, value, searchParams, geoAggregationField,
					geoAggegationPrecision);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@POST
	@Path(ApiConstants.TERM_SEARCH + "/{index}/{type}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Bool Search a Document", description = "Returns Document")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = MapResponse.class))),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public MapResponse boolSearch(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("from") Integer from, @QueryParam("limit") Integer limit, @QueryParam("sortOn") String sortOn,
			@QueryParam("sortType") MapSortType sortType, @QueryParam("geoAggregationField") String geoAggregationField,
			@QueryParam("geoAggegationPrecision") Integer geoAggegationPrecision,
			@Parameter(name = "query") List<MapBoolQuery> query) {
		try {
			MapSearchParams searchParams = new MapSearchParams(from, limit, sortOn, sortType);
			return elasticSearchService.boolSearch(index, type, query, searchParams, geoAggregationField,
					geoAggegationPrecision);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@POST
	@Path(ApiConstants.RANGE_SEARCH + "/{index}/{type}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Range Search a Document", description = "Returns Document")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = MapResponse.class))),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public MapResponse rangeSearch(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("from") Integer from, @QueryParam("limit") Integer limit, @QueryParam("sortOn") String sortOn,
			@QueryParam("sortType") MapSortType sortType, @QueryParam("geoAggregationField") String geoAggregationField,
			@QueryParam("geoAggegationPrecision") Integer geoAggegationPrecision,
			@Parameter(name = "query") List<MapRangeQuery> query) {
		try {
			MapSearchParams searchParams = new MapSearchParams(from, limit, sortOn, sortType);
			return elasticSearchService.rangeSearch(index, type, query, searchParams, geoAggregationField,
					geoAggegationPrecision);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@GET
	@Path(ApiConstants.GEOHASH_AGGREGATION + "/{index}/{type}")
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Geo Hash Aggregation", description = "Returns Document")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = MapDocument.class))),
			@ApiResponse(responseCode = "400", description = "ERROR") })
	public MapDocument geohashAggregation(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("geoAggregationField") String field, @QueryParam("geoAggegationPrecision") Integer precision) {
		if (field == null || precision == null)
			throw new WebApplicationException(
					Response.status(Status.BAD_REQUEST).entity("Field or precision not specified").build());
		if (precision < 1 || precision > 12)
			throw new WebApplicationException(
					Response.status(Status.BAD_REQUEST).entity("Precision value must be between 1 and 12").build());
		try {
			return elasticSearchService.geohashAggregation(index, type, field, precision);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@POST
	@Path(ApiConstants.TERMS_AGGREGATION + "/{index}/{type}")
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Terms Aggregation", description = "Returns Document")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = MapDocument.class))),
			@ApiResponse(responseCode = "400", description = "Incomplete map bounds request"),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public MapDocument termsAggregation(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("field") String field, @QueryParam("subField") String subField,
			@QueryParam("size") Integer size, @QueryParam("locationField") String locationField,
			@Parameter(name = "query") MapSearchQuery query) {
		if (field == null)
			throw new WebApplicationException(
					Response.status(Status.BAD_REQUEST).entity("Aggregation field cannot be empty").build());
		MapSearchParams searchParams = query.getSearchParams();
		MapBoundParams boundParams = searchParams.getMapBoundParams();
		MapBounds mapBounds = null;
		if (boundParams != null)
			mapBounds = boundParams.getBounds();
		if ((locationField != null && mapBounds == null) || (locationField == null && mapBounds != null))
			throw new WebApplicationException(
					Response.status(Status.BAD_REQUEST).entity("Incomplete map bounds request").build());
		try {
			return elasticSearchService.termsAggregation(index, type, field, subField, size, locationField, query);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@GET
	@Path(ApiConstants.TEMPORAL_AGGREGATION + "/{index}/{user}")
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Aggregation for temporal distribution-date created in user page", description = "Return observations created on data group by year, filtered by userId")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(type = "object"))),
			@ApiResponse(responseCode = "400", description = "Exception"),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public Response getAggregationPerDay(@PathParam("index") String index, @PathParam("user") String user) {
		try {
			Map<String, List<DayAggregation>> response = elasticSearchService.aggregationByDay(index, user);
			return Response.status(Status.OK).entity(response).build();
		} catch (Exception e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@GET
	@Path(ApiConstants.MONTH_AGGREGATION + "/{index}/{user}")
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Aggregation for temporal distribution-month observed in user page", description = "Return observed on data grouped by month into intervals of 50 years, filtered by userId")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(type = "object"))),
			@ApiResponse(responseCode = "400", description = "Exception"),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public Response getAggregationPerMonth(@PathParam("index") String index, @PathParam("user") String user) {
		try {
			Map<String, List<MonthAggregation>> response = elasticSearchService.aggregationByMonth(index, user);
			return Response.status(Status.OK).entity(response).build();
		} catch (Exception e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@POST
	@Path(ApiConstants.AGGREGATION + "/{index}/{type}/{filter}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Aggregation for List Page", description = "Returns Aggregated values")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = AggregationResponse.class))),
			@ApiResponse(responseCode = "400", description = "Location field not specified for bounds"),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public AggregationResponse getAggregation(@PathParam("index") String index, @PathParam("type") String type,
			@PathParam("filter") String filter, @QueryParam("geoAggregationField") String geoAggregationField,
			@QueryParam("geoFilterField") String geoShapeFilterField, @Parameter(name = "query") MapSearchQuery query)
			throws IOException {
		MapSearchParams searchParams = query.getSearchParams();
		MapBoundParams boundParams = searchParams.getMapBoundParams();
		MapBounds bounds = null;
		if (boundParams != null)
			bounds = boundParams.getBounds();
		if (bounds != null && geoAggregationField == null)
			throw new WebApplicationException(
					Response.status(Status.BAD_REQUEST).entity(ErrorConstants.LOCATION_FIELD_NOT_SPECIFIED).build());
		try {
			return elasticSearchService.aggregation(index, type, query, geoAggregationField, filter,
					geoShapeFilterField);
		} catch (Exception e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@GET
	@Path(ApiConstants.RIGHTPAN + "/{index}/{type}")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Aggregation for List Page", description = "Returns Aggregated values")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = ObservationInfo.class))),
			@ApiResponse(responseCode = "400", description = "Inappropriate Data"),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public Response getObservationInfo(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("id") String id, @DefaultValue("true") @QueryParam("isMaxVotedRecoId") Boolean isMaxVotedRecoId)
			throws IOException {
		try {
			ObservationInfo info = elasticSearchService.getObservationRightPan(index, type, id, isMaxVotedRecoId);
			return Response.status(Status.OK).entity(info).build();
		} catch (Exception e) {
			return Response.status(Status.BAD_REQUEST).build();
		}
	}

	@GET
	@Path(ApiConstants.NEARBY + "/{index}/{type}")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "NearBy Observation", description = "Returns all the nearby Observation")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(array = @ArraySchema(schema = @Schema(implementation = ObservationNearBy.class)))),
			@ApiResponse(responseCode = "400", description = "Inappropriate Data"),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public Response getNearByObservation(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("lat") String lat, @QueryParam("lon") String lon) {
		try {
			Double latitude = Double.parseDouble(lat);
			Double longitude = Double.parseDouble(lon);
			List<ObservationNearBy> result = elasticSearchService.observationNearBy(index, type, latitude, longitude);
			return Response.status(Status.OK).entity(result).build();
		} catch (Exception e) {
			return Response.status(Status.BAD_REQUEST).build();
		}
	}

	@POST
	@Path(ApiConstants.SEARCH + ApiConstants.GEOHASH_AGGREGATION + "/{index}/{type}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Calculate the geohash Aggregation based on the filter conditions", description = "Returns the GeoHashAggregation in Key value pair")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = GeoHashAggregationData.class))),
			@ApiResponse(responseCode = "400", description = "Unable to retrieve the data"),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public GeoHashAggregationData getGeoHashAggregation(@PathParam("index") String index,
			@PathParam("type") String type, @QueryParam("geoAggregationField") String geoAggregationField,
			@QueryParam("geoAggegationPrecision") Integer geoAggegationPrecision,
			@QueryParam("onlyFilteredAggregation") Boolean onlyFilteredAggregation,
			@QueryParam("termsAggregationField") String termsAggregationField,
			@Parameter(name = "query") MapSearchQuery query) {
		MapSearchParams searchParams = query.getSearchParams();
		MapBoundParams boundParams = searchParams.getMapBoundParams();
		MapBounds bounds = null;
		if (boundParams != null)
			bounds = boundParams.getBounds();

		if (onlyFilteredAggregation != null && onlyFilteredAggregation && bounds == null)
			throw new WebApplicationException(
					Response.status(Status.BAD_REQUEST).entity("Bounds not specified for filtering").build());

		if (bounds != null && geoAggregationField == null)
			throw new WebApplicationException(
					Response.status(Status.BAD_REQUEST).entity(ErrorConstants.LOCATION_FIELD_NOT_SPECIFIED).build());
		try {
			return elasticSearchService.getNewGeoAggregation(index, type, query, geoAggregationField,
					geoAggegationPrecision);
		} catch (Exception e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@POST
	@Path(ApiConstants.SEARCH + "/{index}/{type}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(operationId = "search", summary = "Search for List Page", description = "Returns List of Document")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = MapResponse.class))),
			@ApiResponse(responseCode = "400", description = "Inappropriate Bounds Data"),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public MapResponse search(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("geoAggregationField") String geoAggregationField,
			@QueryParam("geoAggegationPrecision") Integer geoAggegationPrecision,
			@QueryParam("onlyFilteredAggregation") Boolean onlyFilteredAggregation,
			@QueryParam("termsAggregationField") String termsAggregationField,
			@QueryParam("geoFilterField") String geoShapeFilterField, @Parameter(name = "query") MapSearchQuery query) {
		MapSearchParams searchParams = query.getSearchParams();
		MapBoundParams boundParams = searchParams.getMapBoundParams();
		MapBounds bounds = null;
		if (boundParams != null)
			bounds = boundParams.getBounds();

		if (onlyFilteredAggregation != null && onlyFilteredAggregation && bounds == null)
			throw new WebApplicationException(
					Response.status(Status.BAD_REQUEST).entity("Bounds not specified for filtering").build());

		if (bounds != null && geoAggregationField == null)
			throw new WebApplicationException(
					Response.status(Status.BAD_REQUEST).entity(ErrorConstants.LOCATION_FIELD_NOT_SPECIFIED).build());

		try {
			return elasticSearchService.search(index, type, query, geoAggregationField, geoAggegationPrecision,
					onlyFilteredAggregation, termsAggregationField, geoShapeFilterField);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@GET
	@Path(ApiConstants.MAPPING + "/{index}")
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Mapping of Document", description = "Returns Document")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = MapDocument.class))),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public MapDocument getMapping(@PathParam("index") String index) {
		try {
			return elasticAdminSearchService.getMapping(index);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@POST
	@Path(ApiConstants.MAPPING + "/{index}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Post Mapping of Document", description = "Returns Success Failure")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = MapQueryResponse.class))),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public MapQueryResponse postMapping(@PathParam("index") String index,
			@Parameter(name = "mapping") MapDocument mapping) {
		String docString = String.valueOf(mapping.getDocument());
		try {
			return elasticAdminSearchService.postMapping(index, docString);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@POST
	@Path(ApiConstants.INDEX_ADMIN + "/{index}/{type}")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Create Index", description = "Returns Success Failure")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = MapQueryResponse.class))),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public MapQueryResponse createIndex(@PathParam("index") String index, @PathParam("type") String type) {
		try {
			return elasticAdminSearchService.createIndex(index, type);
		} catch (Exception e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@POST
	@Path(ApiConstants.ESMAPPING + "/{index}")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Post Mapping of Document to ES", description = "Returns Success Failure")
	@ApiResponses({ @ApiResponse(responseCode = "200", description = "Success"),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public Response esPostMapping(@PathParam("index") String index) {
		try {
			List<String> indexNameAndMapping = utilityMethods.getEsindexWithMapping(index);
			elasticAdminSearchService.esPostMapping(indexNameAndMapping.get(0), indexNameAndMapping.get(1));
			return Response.status(Status.OK).build();
		} catch (Exception e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@SuppressWarnings("unchecked")
	@GET
	@Path(ApiConstants.AUTOCOMPLETE + "/{index}/{type}")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "AutoCompletion", description = "Returns Success Failure")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(array = @ArraySchema(schema = @Schema(implementation = ElasticIndexes.class)))),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public Response autoCompletion(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("field") String field, @QueryParam("text") String fieldText,
			@QueryParam("groupId") String filterField, @QueryParam("group") Integer filter) {
		String elasticIndex = utilityMethods.getEsIndexConstants(index);
		String elasticType = utilityMethods.getEsIndexTypeConstant(type);
		try {
			List<? extends ElasticIndexes> records = null;
			if (filter == null) {
				records = elasticSearchService.autoCompletion(elasticIndex, elasticType, field, fieldText,
						utilityMethods.getClass(index));
			} else {
				records = elasticSearchService.autoCompletion(elasticIndex, elasticType, field, fieldText, filterField,
						filter, utilityMethods.getClass(index));
			}
			if (index.equals("etd")) {
				if (field.equals("name")) {
					records = utilityMethods.rankDocument((List<ExtendedTaxonDefinition>) records, field, fieldText);
				} else if (field.equals("common_name")) {
					records = utilityMethods.rankDocumentBasedOnCommonName((List<ExtendedTaxonDefinition>) records,
							fieldText);
				}
			}
			return Response.status(Status.OK).entity(records).build();
		} catch (Exception e) {
			throw new WebApplicationException(Response.status(Status.NO_CONTENT).entity(e.getMessage()).build());
		}
	}

	@GET
	@Path(ApiConstants.MATCHPHRASE + "/{index}/{type}")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Match Phrase In Elastic", description = "Returns Success Failure")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = ExtendedTaxonDefinition.class))),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public Response matchPhrase(@DefaultValue("etd") @PathParam("index") String index,
			@DefaultValue("er") @PathParam("type") String type,
			@DefaultValue("name") @QueryParam("scientificField") String scientificField,
			@QueryParam("scientificText") String scientificText,
			@DefaultValue("canonical_form") @QueryParam("canonicalField") String canonicalField,
			@QueryParam("canonicalText") String canonicalText) {
		index = utilityMethods.getEsIndexConstants(index);
		type = utilityMethods.getEsIndexTypeConstant(type);
		Boolean checkOnAllParam = false;
		if (scientificText != null && !scientificText.isEmpty()) {
			checkOnAllParam = true;
		}
		try {
			List<ExtendedTaxonDefinition> records = elasticSearchService.matchPhrase(index, type, scientificField,
					scientificText, canonicalField, canonicalText, checkOnAllParam);
			if (records.size() > 1) {
				records = utilityMethods.rankDocument(records, canonicalField, scientificText);
				return Response.status(Status.OK).entity(records.get(0)).build();
			}
			return Response.status(Status.OK).entity(records.get(0)).build();
		} catch (Exception e) {
			throw new WebApplicationException(Response.status(Status.NO_CONTENT).entity(e.getMessage()).build());
		}
	}

	@GET
	@Path(ApiConstants.MATCH + "/{index}/{type}")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Match In Elastic", description = "Returns Success Failure")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(array = @ArraySchema(schema = @Schema(implementation = Object.class)))),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public Response match(@DefaultValue("etd") @PathParam("index") String index,
			@DefaultValue("er") @PathParam("type") String type,
			@DefaultValue("name") @QueryParam("scientificField") String scientificField,
			@QueryParam("scientificText") String scientificText,
			@DefaultValue("canonical_form") @QueryParam("canonicalField") String canonicalField,
			@QueryParam("canonicalText") String canonicalText) {
		index = utilityMethods.getEsIndexConstants(index);
		type = utilityMethods.getEsIndexTypeConstant(type);
		Boolean checkOnAllParam = false;
		if (scientificText != null && !scientificText.isEmpty()) {
			checkOnAllParam = true;
		}
		try {
			List<ExtendedTaxonDefinition> records = elasticSearchService.matchPhrase(index, type, scientificField,
					scientificText, canonicalField, canonicalText, checkOnAllParam);
			if (records.size() > 1) {
				records = utilityMethods.rankDocument(records, canonicalField, scientificText);
				return Response.status(Response.Status.OK).entity(records).build();
			}
			return Response.status(Response.Status.OK).entity(records).build();
		} catch (Exception e) {
			throw new WebApplicationException(
					Response.status(Response.Status.NO_CONTENT).entity(e.getMessage()).build());
		}
	}

	@GET
	@Path(ApiConstants.GETTOPUSERS + "/{index}/{type}")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Getting top users based on score", description = "Returns Success Failure")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(array = @ArraySchema(schema = @Schema(type = "object")))),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public Response topUsers(@DefaultValue("eaf") @PathParam("index") String index,
			@DefaultValue("er") @PathParam("type") String type,
			@DefaultValue("") @QueryParam("value") String sortingValue,
			@DefaultValue("20") @QueryParam("how_many") String topUser, @QueryParam("time") String timePeriod) {
		if (sortingValue.isEmpty())
			sortingValue = null;
		String timeFilter = utilityMethods.getTimeWindow(timePeriod);
		index = utilityMethods.getEsIndexConstants(index);
		type = utilityMethods.getEsIndexTypeConstant(type);
		List<LinkedHashMap<String, LinkedHashMap<String, String>>> records = elasticSearchService.getTopUsers(index,
				type, sortingValue, Integer.parseInt(topUser), timeFilter);
		return Response.status(Response.Status.OK).entity(records).build();
	}

	@GET
	@Path(ApiConstants.FILTERAUTOCOMPLETE + "/{index}/{type}")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Getting Filter Suggestion For List Page", description = "Return Success Failure")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(array = @ArraySchema(schema = @Schema(type = "string")))),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public Response getListPageFilterValue(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("field") String filterOn, @QueryParam("text") String text) {
		index = utilityMethods.getEsIndexConstants(index);
		type = utilityMethods.getEsIndexTypeConstant(type);
		List<String> results = elasticSearchService.getListPageFilterValue(index, type, filterOn, text);
		return Response.status(Response.Status.OK).entity(results).build();
	}

	@GET
	@Path(ApiConstants.USERIBP + ApiConstants.AUTOCOMPLETE + "/{index}/{type}")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Auto complete username", description = "Returns List of userIbp")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = MapResponse.class))),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public Response autocompleteUserIBP(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("userGroupId") String userGroupId, @QueryParam("name") String name) throws IOException {
		MapResponse results = elasticSearchService.autocompleteUserIBP(index, type, userGroupId, name);
		return Response.status(Response.Status.OK).entity(results).build();
	}

	@GET
	@Path(ApiConstants.SPECIESCONTRIBUTOR + ApiConstants.AUTOCOMPLETE + "/{index}/{type}")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Auto complete username", description = "Returns List of userIbp")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = MapResponse.class))),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public Response autocompleteUserSpeciesContributor(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("name") String name) throws IOException {
		MapResponse results = elasticSearchService.autocompleteSpeciesContributors(index, type, name);
		return Response.status(Status.OK).entity(results).build();
	}

	@GET
	@Path(ApiConstants.GETUSERSCORE)
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Getting User Activity Score", description = "Returns Success Failure")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = UserScore.class))),
			@ApiResponse(responseCode = "500", description = "ERROR") })
	public Response getUserScore(@DefaultValue("eaf") @QueryParam("index") String index,
			@DefaultValue("er") @QueryParam("type") String type, @QueryParam("authorId") String authorId,
			@DefaultValue("") @QueryParam("time") String timePeriod) {
		String timeFilter = null;
		if (!timePeriod.isEmpty()) {
			timeFilter = utilityMethods.getTimeWindow(timePeriod);
		}
		index = utilityMethods.getEsIndexConstants(index);
		type = utilityMethods.getEsIndexTypeConstant(type);
		List<LinkedHashMap<String, LinkedHashMap<String, String>>> records = elasticSearchService.getUserScore(index,
				type, Integer.parseInt(authorId), timeFilter);
		UserScore userScoreRecord = new UserScore();
		userScoreRecord.setRecord(records);
		return Response.status(Status.OK).entity(userScoreRecord).build();
	}

	@GET
	@Path(ApiConstants.FILTERS + ApiConstants.LIST + "/{index}/{type}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Get all the dynamic filters", description = "Return all the filter")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = FilterPanelData.class))),
			@ApiResponse(responseCode = "400", description = "unable to get the data") })
	public Response getFilterLists(@PathParam("index") String index, @PathParam("type") String type) {
		try {
			FilterPanelData result = elasticSearchService.getListPanel(index, type);
			return Response.status(Status.OK).entity(result).build();
		} catch (Exception e) {
			return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
		}
	}

	@GET
	@Path(ApiConstants.SPECIES + "/{index}/{type}/{speciesId}")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "search for the observation with the given speciesId", description = "Returns a list of observation for the given speciesId")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(array = @ArraySchema(schema = @Schema(implementation = ObservationLatLon.class)))),
			@ApiResponse(responseCode = "400", description = "Unable to retrieve the data") })
	public Response getSpeciesCoords(@PathParam("index") String index, @PathParam("type") String type,
			@PathParam("speciesId") String speciesId) {
		try {
			List<ObservationLatLon> result = elasticSearchService.getSpeciesCoordinates(index, type, speciesId);
			return Response.status(Status.OK).entity(result).build();
		} catch (Exception e) {
			return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
		}
	}

	@GET
	@Path(ApiConstants.FETCHINDEX)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "fetch index information from elastic", description = "return successful response")
	@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(type = "string")))
	public Response fetchIndex() {
		String response = elasticSearchService.fetchIndex();
		if (response != null)
			return Response.status(Status.OK).entity(response).build();
		else
			return Response.status(Status.BAD_REQUEST).build();
	}

	@GET
	@Path(ApiConstants.USERINFO + "/{index}/{type}/{authorId}")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "fetch the observation uploaded freq by user", description = "Returns the maxvotedId freq")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = AuthorUploadedObservationInfo.class))),
			@ApiResponse(responseCode = "400", description = "unable to get the result") })
	public Response getUploadUserInfo(@PathParam("index") String index, @PathParam("type") String type,
			@PathParam("authorId") String authorId, @QueryParam("size") String size,
			@QueryParam("sGroup") String sGroup, @QueryParam("hasMedia") Boolean hasMedia) {
		try {
			Long aId = Long.parseLong(authorId);
			Integer sizeInteger = Integer.parseInt(size);
			Long speciesGroup = null;
			if (sGroup != null)
				speciesGroup = Long.parseLong(sGroup);
			AuthorUploadedObservationInfo result = elasticSearchService.getUserData(index, type, aId, sizeInteger,
					speciesGroup, hasMedia);
			return Response.status(Status.OK).entity(result).build();
		} catch (Exception e) {
			return Response.status(Status.BAD_REQUEST).entity(e.getMessage()).build();
		}
	}
}