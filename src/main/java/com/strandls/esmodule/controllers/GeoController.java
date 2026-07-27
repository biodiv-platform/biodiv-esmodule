package com.strandls.esmodule.controllers;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.strandls.esmodule.ApiConstants;
import com.strandls.esmodule.models.GeoAggregationData;
import com.strandls.esmodule.models.MapResponse;
import com.strandls.esmodule.services.ElasticSearchGeoService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

/**
 * Controller for geo related query services
 *
 * @author mukund
 */
@Tag(name = "Geo service", description = "Geo spatial query APIs")
@Path(ApiConstants.V1 + ApiConstants.GEO)
public class GeoController {

	@Inject
	ElasticSearchGeoService service;

	@GET
	@Path(ApiConstants.WITHIN + "/{index}/{type}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Within", description = "Returns data within bounding box")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = MapResponse.class))),
			@ApiResponse(responseCode = "500", description = "Internal server error", content = @Content(schema = @Schema(type = "string"))) })
	public MapResponse within(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("geoField") String geoField, @QueryParam("top") double top, @QueryParam("left") double left,
			@QueryParam("bottom") double bottom, @QueryParam("right") double right) {

		try {
			return service.getGeoWithinDocuments(index, type, geoField, top, left, bottom, right);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@POST
	@Path(ApiConstants.AGGREGATION)
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Aggregation (POST)", description = "Returns geo aggregation result from request body")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = GeoAggregationData.class))),
			@ApiResponse(responseCode = "500", description = "Internal server error", content = @Content(schema = @Schema(type = "string"))) })
	public Response getGeoAggregation(String jsonString) {
		try {
			Map<String, Long> hashToDocCount = service.getGeoAggregation(jsonString);
			return Response.ok().entity(new GeoAggregationData(hashToDocCount)).build();
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@POST
	@Path(ApiConstants.BOUNDS)
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Bounds", description = "Returns bounds from the data")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(array = @ArraySchema(schema = @Schema(type = "array", implementation = Double.class)))),
			@ApiResponse(responseCode = "500", description = "Internal server error", content = @Content(schema = @Schema(type = "string"))) })
	public Response getGeoBounds(String jsonString) {
		try {
			List<List<Double>> boundPoints = service.getGeoBounds(jsonString);
			return Response.ok().entity(boundPoints).build();
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}

	@GET
	@Path(ApiConstants.AGGREGATION + "/{index}/{type}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Aggregation (GET)", description = "Returns geo aggregation result with bounding box and precision", operationId = "getGeoAggregationFromIndexType")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(schema = @Schema(implementation = GeoAggregationData.class))),
			@ApiResponse(responseCode = "500", description = "Internal server error", content = @Content(schema = @Schema(type = "string"))) })
	public Response getGeoAggregation(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("geoField") String geoField, @QueryParam("precision") Integer precision,
			@QueryParam("top") Double top, @QueryParam("left") Double left, @QueryParam("bottom") Double bottom,
			@QueryParam("right") Double right, @QueryParam("speciesId") Long speciesId) {

		try {
			Map<String, Long> hashToDocCount = service.getGeoAggregation(index, type, geoField, precision, top, left,
					bottom, right, speciesId);
			return Response.ok().entity(new GeoAggregationData(hashToDocCount)).build();
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}
}
