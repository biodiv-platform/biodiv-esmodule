package com.strandls.esmodule.controllers;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

/**
 * @author Abhishek Rudra
 *
 */
@Api("ES services")
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
	public String ping() {
		return "PONG";
	}

	@GET
	@Path(ApiConstants.IDENTIFIERSINFO + "/{index}/{userIds}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)

	@ApiOperation(value = "Fetch details of identifiers", notes = "Returns a list of objects containing name,profile pic and author id of identifiers", response = IdentifiersInfo.class, responseContainer = "List")
	@ApiResponses(value = { @ApiResponse(code = 400, message = "Exception", response = String.class),
			@ApiResponse(code = 500, message = "ERROR", response = String.class) })

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

	@ApiOperation(value = "Fetch details of uploaders", notes = "Returns a list of objects containing name,profile pic and author id of uploaders", response = UploadersInfo.class, responseContainer = "List")
	@ApiResponses(value = { @ApiResponse(code = 400, message = "Exception", response = String.class),
			@ApiResponse(code = 500, message = "ERROR", response = String.class) })
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

	@ApiOperation(value = "Create Document", notes = "Returns succuess failure", response = MapQueryResponse.class)
	@ApiResponses(value = { @ApiResponse(code = 400, message = "Exception", response = String.class),
			@ApiResponse(code = 500, message = "ERROR", response = String.class) })

	public MapQueryResponse create(@PathParam("index") String index, @PathParam("type") String type,
			@PathParam("documentId") String documentId, @ApiParam(name = "document") MapDocument document) {

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

	@ApiOperation(value = "Fetch Document", notes = "Returns Document", response = MapDocument.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })

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

	@ApiOperation(value = "Update Document", notes = "Returns Success Failur", response = MapQueryResponse.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })

	public MapQueryResponse update(@PathParam("index") String index, @PathParam("type") String type,
			@PathParam("documentId") String documentId, @ApiParam(name = "document") Map<String, Object> document) {

		try {
			return elasticSearchService.update(index, type, documentId, document);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@PUT
	@Path(ApiConstants.UPDATE + "/{type}/{id}")
	@Produces(MediaType.APPLICATION_JSON)

	@ApiOperation(value = "Aggregation for temporal distribution-month observed in user page", notes = "Return observed on data grouped by month into intervals of 50 years, filtered by userId", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 400, message = "Exception", response = String.class),
			@ApiResponse(code = 500, message = "ERROR", response = String.class) })

	public String updateEsField(@PathParam("type") String type, @PathParam("id") String id,
			@ApiParam(name = "content") String content) {

		String response = null;

		try {
			response = elasticSearchService.esUpdate(type, id, content);
			return response;
		} catch (Exception e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@DELETE
	@Path(ApiConstants.DATA + "/{index}/{type}/{documentId}")
	@Produces(MediaType.APPLICATION_JSON)

	@ApiOperation(value = "Delete Document", notes = "Returns Success Failure", response = MapQueryResponse.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })

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

	@ApiOperation(value = "Bulk Upload Create Document", notes = "Returns Success Failure", response = MapQueryResponse.class, responseContainer = "List")
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })

	public List<MapQueryResponse> bulkUpload(@PathParam("index") String index, @PathParam("type") String type,
			@ApiParam(name = "jsonArray") String jsonArray) {

		try {
			return elasticSearchService.bulkUpload(index, type, jsonArray);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@SuppressWarnings("resource")
	@PUT
	@Path(ApiConstants.BULK_UPDATE + "/{index}/{type}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)

	@ApiOperation(value = "Bulk Upload Update Document", notes = "Returns Success Failure", response = MapQueryResponse.class, responseContainer = "List")
	@ApiResponses(value = { @ApiResponse(code = 400, message = "No Documents to update", response = String.class),
			@ApiResponse(code = 500, message = "ERROR", response = String.class) })

	public List<MapQueryResponse> bulkUpdate(@PathParam("index") String index, @PathParam("type") String type,
			@ApiParam(name = "updateDocs") List<Map<String, Object>> updateDocs) {

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

	@ApiOperation(value = "Search a Document", notes = "Returns Document", response = MapResponse.class)
	@ApiResponses(value = { @ApiResponse(code = 400, message = "key or value not specified", response = String.class),
			@ApiResponse(code = 500, message = "ERROR", response = String.class) })

	public MapResponse search(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("key") String key, @QueryParam("value") String value,
			@QueryParam("geoAggregationField") String geoAggregationField,
			@QueryParam("geoAggegationPrecision") Integer geoAggegationPrecision,
			@ApiParam(name = "searchParam") MapSearchParams searchParams) {

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

	@ApiOperation(value = "Bool Search a Document", notes = "Returns Document", response = MapResponse.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })

	public MapResponse boolSearch(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("from") Integer from, @QueryParam("limit") Integer limit, @QueryParam("sortOn") String sortOn,
			@QueryParam("sortType") MapSortType sortType, @QueryParam("geoAggregationField") String geoAggregationField,
			@QueryParam("geoAggegationPrecision") Integer geoAggegationPrecision,
			@ApiParam(name = "query") List<MapBoolQuery> query) {

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

	@ApiOperation(value = "Range Search a Document", notes = "Returns Document", response = MapResponse.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })

	public MapResponse rangeSearch(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("from") Integer from, @QueryParam("limit") Integer limit, @QueryParam("sortOn") String sortOn,
			@QueryParam("sortType") MapSortType sortType, @QueryParam("geoAggregationField") String geoAggregationField,
			@QueryParam("geoAggegationPrecision") Integer geoAggegationPrecision,
			@ApiParam(name = "query") List<MapRangeQuery> query) {

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

	@ApiOperation(value = "Geo Hash Aggregation", notes = "Returns Document", response = MapDocument.class)
	@ApiResponses(value = { @ApiResponse(code = 400, message = "ERROR", response = String.class) })

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

	@ApiOperation(value = "Terms Aggregation", notes = "Returns Document", response = MapDocument.class)
	@ApiResponses(value = {
			@ApiResponse(code = 400, message = "Incomplete map bounds request", response = String.class),
			@ApiResponse(code = 500, message = "ERROR", response = String.class) })

	public MapDocument termsAggregation(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("field") String field, @QueryParam("subField") String subField,
			@QueryParam("size") Integer size, @QueryParam("locationField") String locationField,
			@ApiParam(name = "query") MapSearchQuery query) {

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

	@ApiOperation(value = "Aggregation for temporal distribution-date created in user page", notes = "Return observations created on data group by year, filtered by userId", response = Map.class)
	@ApiResponses(value = { @ApiResponse(code = 400, message = "Exception", response = String.class),
			@ApiResponse(code = 500, message = "ERROR", response = String.class) })

	public Response getAggregationPerDay(@PathParam("index") String index, @PathParam("user") String user) {

		Map<String, List<DayAggregation>> response = null;

		try {
			response = elasticSearchService.aggregationByDay(index, user);
			return Response.status(Status.OK).entity(response).build();
		} catch (Exception e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@GET
	@Path(ApiConstants.MONTH_AGGREGATION + "/{index}/{user}")
	@Produces(MediaType.APPLICATION_JSON)

	@ApiOperation(value = "Aggregation for temporal distribution-month observed in user page", notes = "Return observed on data grouped by month into intervals of 50 years, filtered by userId", response = Map.class)
	@ApiResponses(value = { @ApiResponse(code = 400, message = "Exception", response = String.class),
			@ApiResponse(code = 500, message = "ERROR", response = String.class) })

	public Response getAggregationPerMonth(@PathParam("index") String index, @PathParam("user") String user) {

		Map<String, List<MonthAggregation>> response = null;

		try {
			response = elasticSearchService.aggregationByMonth(index, user);
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

	@ApiOperation(value = "Aggregation for List Page", notes = "Returns Aggregated values", response = AggregationResponse.class)
	@ApiResponses(value = {
			@ApiResponse(code = 400, message = "Location field not specified for bounds", response = String.class),
			@ApiResponse(code = 500, message = "ERROR", response = String.class) })

	public AggregationResponse getAggregation(@PathParam("index") String index, @PathParam("type") String type,
			@PathParam("filter") String filter, @QueryParam("geoAggregationField") String geoAggregationField,
			@QueryParam("geoFilterField") String geoShapeFilterField, @ApiParam(name = "query") MapSearchQuery query)
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

	@ApiOperation(value = "Aggregation for List Page", notes = "Returns Aggregated values", response = ObservationInfo.class)
	@ApiResponses(value = { @ApiResponse(code = 400, message = "Inappropriate Data", response = String.class) })

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

	@ApiOperation(value = "NearBy Observation", notes = "Returns all the nearby Observation", response = ObservationNearBy.class, responseContainer = "List")
	@ApiResponses(value = { @ApiResponse(code = 400, message = "Inappropriate Data", response = String.class) })

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

	@ApiOperation(value = "Calculate the geohash Aggregation based on the filter conditions", notes = "Returns the GeoHashAggregation in Key value pair", response = GeoHashAggregationData.class)
	@ApiResponses(value = {
			@ApiResponse(code = 400, message = "Unable to retireve the data", response = String.class) })
	public GeoHashAggregationData getGeoHashAggregation(@PathParam("index") String index,
			@PathParam("type") String type, @QueryParam("geoAggregationField") String geoAggregationField,
			@QueryParam("geoAggegationPrecision") Integer geoAggegationPrecision,
			@QueryParam("onlyFilteredAggregation") Boolean onlyFilteredAggregation,
			@QueryParam("termsAggregationField") String termsAggregationField,
			@ApiParam(name = "query") MapSearchQuery query) {

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

	@ApiOperation(value = "Search for List Page", notes = "Returns List of Document", response = MapResponse.class)
	@ApiResponses(value = { @ApiResponse(code = 400, message = "Inappropriate Bounds Data", response = String.class),
			@ApiResponse(code = 500, message = "ERROR", response = String.class) })

	public MapResponse search(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("geoAggregationField") String geoAggregationField,
			@QueryParam("geoAggegationPrecision") Integer geoAggegationPrecision,
			@QueryParam("onlyFilteredAggregation") Boolean onlyFilteredAggregation,
			@QueryParam("termsAggregationField") String termsAggregationField,
			@QueryParam("geoFilterField") String geoShapeFilterField, @ApiParam(name = "query") MapSearchQuery query) {

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

	// ---------- Admin Services -------------

	@GET
	@Path(ApiConstants.MAPPING + "/{index}")
	@Produces(MediaType.APPLICATION_JSON)

	@ApiOperation(value = "Mapping of Document", notes = "Returns Document", response = MapDocument.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })

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

	@ApiOperation(value = "Post Mapping of Document", notes = "Returns Success Failure", response = MapQueryResponse.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })

	public MapQueryResponse postMapping(@PathParam("index") String index,
			@ApiParam(name = "mapping") MapDocument mapping) {

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

	@ApiOperation(value = "Create Index", notes = "Returns Success Failure", response = MapQueryResponse.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })

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

	@ApiOperation(value = "Post Mapping of Document to ES", notes = "Returns Success Failure", response = String.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })

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

	@ApiOperation(value = "AutoCompletion", notes = "Returns Success Failure", response = ElasticIndexes.class, responseContainer = "List")
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })
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

	@ApiOperation(value = "Match Phrase In Elastic", notes = "Returns Success Failure", response = ExtendedTaxonDefinition.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })
	public Response matchPhrase(@DefaultValue("etd") @PathParam("index") String index,
			@DefaultValue("er") @PathParam("type") String type,
			@DefaultValue("name") @QueryParam("scientificField") String scientificField,
			@QueryParam("scientificText") String scientificText,
			@DefaultValue("canonical_form") @QueryParam("canonicalField") String canonicalField,
			@QueryParam("canonicalText") String canonicalText) {

		index = utilityMethods.getEsIndexConstants(index);
		type = utilityMethods.getEsIndexTypeConstant(type);
		Boolean checkOnAllParam = false;
		if (!scientificText.isEmpty() || scientificText != null) {
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

	@ApiOperation(value = "Match In Elastic", notes = "Returns Success Failure", response = List.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })
	public Response match(@DefaultValue("etd") @PathParam("index") String index,
			@DefaultValue("er") @PathParam("type") String type,
			@DefaultValue("name") @QueryParam("scientificField") String scientificField,
			@QueryParam("scientificText") String scientificText,
			@DefaultValue("canonical_form") @QueryParam("canonicalField") String canonicalField,
			@QueryParam("canonicalText") String canonicalText) {

		index = utilityMethods.getEsIndexConstants(index);
		type = utilityMethods.getEsIndexTypeConstant(type);
		Boolean checkOnAllParam = false;
		if (!scientificText.isEmpty() || scientificText != null) {
			checkOnAllParam = true;
		}

		try {
			List<ExtendedTaxonDefinition> records = elasticSearchService.matchPhrase(index, type, scientificField,
					scientificText, canonicalField, canonicalText, checkOnAllParam);
			if (records.size() > 1) {
				records = utilityMethods.rankDocument(records, canonicalField, scientificText);
				return Response.status(Status.OK).entity(records).build();
			}
			return Response.status(Status.OK).entity(records).build();
		} catch (Exception e) {
			throw new WebApplicationException(Response.status(Status.NO_CONTENT).entity(e.getMessage()).build());
		}
	}

	@GET
	@Path(ApiConstants.GETTOPUSERS + "/{index}/{type}")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)

	@ApiOperation(value = "Getting top users based on score", notes = "Returns Success Failure", response = LinkedHashMap.class, responseContainer = "List")
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })
	public Response topUsers(@DefaultValue("eaf") @PathParam("index") String index,
			@DefaultValue("er") @PathParam("type") String type,
			@DefaultValue("") @QueryParam("value") String sortingValue,
			@DefaultValue("20") @QueryParam("how_many") String topUser, @QueryParam("time") String timePeriod) {

		String timeFilter = null;
		if (sortingValue.isEmpty())
			sortingValue = null;

		timeFilter = utilityMethods.getTimeWindow(timePeriod);

		index = utilityMethods.getEsIndexConstants(index);
		type = utilityMethods.getEsIndexTypeConstant(type);

		List<LinkedHashMap<String, LinkedHashMap<String, String>>> records = elasticSearchService.getTopUsers(index,
				type, sortingValue, Integer.parseInt(topUser), timeFilter);
		return Response.status(Status.OK).entity(records).build();
	}

	@GET
	@Path(ApiConstants.FILTERAUTOCOMPLETE + "/{index}/{type}")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Getting Filter Suggestion For List Page", notes = "Return Success Failure", response = String.class, responseContainer = "List")
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })
	public Response getListPageFilterValue(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("field") String filterOn, @QueryParam("text") String text) {
		index = utilityMethods.getEsIndexConstants(index);
		type = utilityMethods.getEsIndexTypeConstant(type);
		List<String> results = elasticSearchService.getListPageFilterValue(index, type, filterOn, text);
		return Response.status(Status.OK).entity(results).build();
	}

	@GET
	@Path(ApiConstants.USERIBP + ApiConstants.AUTOCOMPLETE + "/{index}/{type}")
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Auto complete username", notes = "Returns List of userIbp", response = MapResponse.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })
	public Response autocompleteUserIBP(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("userGroupId") String userGroupId, @QueryParam("name") String name) throws IOException {
		MapResponse results = elasticSearchService.autocompleteUserIBP(index, type, userGroupId, name);
		return Response.status(Status.OK).entity(results).build();
	}

	@GET
	@Path(ApiConstants.GETUSERSCORE)
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	@ApiOperation(value = "Getting User Activity Score", notes = "Returns Success Failure", response = UserScore.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })
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

	@ApiOperation(value = "Get all the dynamic filters", notes = "Return all the filter", response = FilterPanelData.class)
	@ApiResponses(value = { @ApiResponse(code = 400, message = "unable to get the data", response = String.class) })

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

	@ApiOperation(value = "search for the observation with the given speciesId", notes = "Returns a list of observation for the given speciesId", response = ObservationLatLon.class, responseContainer = "List")
	@ApiResponses(value = { @ApiResponse(code = 400, message = "Unable to retrive the data", response = String.class) })

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
	@ApiOperation(value = "fetch index information from elastic", notes = "return succesful response", response = String.class)
	@ApiResponses(value = {
			@ApiResponse(code = 400, message = "Unable to fetch index information", response = String.class) })
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

	@ApiOperation(value = "fetch the observation uploaded freq by user", notes = "Returns the maxvotedId freq", response = AuthorUploadedObservationInfo.class)
	@ApiResponses(value = { @ApiResponse(code = 400, message = "unable to get the result", response = String.class) })

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
