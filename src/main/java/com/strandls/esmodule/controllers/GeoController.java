package com.strandls.esmodule.controllers;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.strandls.esmodule.ApiConstants;
import com.strandls.esmodule.models.MapResponse;
import com.strandls.esmodule.services.ElasticSearchGeoService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

/**
 * Controller for geo related query services
 * 
 * @author mukund
 *
 */

@Api("Geo service")
@Path(ApiConstants.V1 + ApiConstants.GEO)
public class GeoController {

	@Inject
	ElasticSearchGeoService service;

	@GET
	@Path(ApiConstants.WITHIN + "/{index}/{type}")
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)

	@ApiOperation(value = "Witin", notes = "Returns Data", response = MapResponse.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })

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

	@ApiOperation(value = "Aggregation", notes = "Returns Data", response = Map.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })
	public Response getGeoAggregation(String jsonString) {
		try {
			Map<String, Long> hashToDocCount = service.getGeoAggregation(jsonString); 
			return Response.ok().entity(hashToDocCount).build();
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}
	
	@POST
	@Path(ApiConstants.BOUNDS)
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)

	@ApiOperation(value = "Bounds", notes = "Returns bounds from the data", response = Map.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })
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

	@ApiOperation(value = "Aggregation", notes = "Returns Data", response = Map.class)
	@ApiResponses(value = { @ApiResponse(code = 500, message = "ERROR", response = String.class) })
	public Response getGeoAggregation(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("geoField") String geoField, @QueryParam("precision") Integer precision, 
			@QueryParam("top") Double top, @QueryParam("left") Double left,
			@QueryParam("bottom") Double bottom, @QueryParam("right") Double right, @QueryParam("speciesId") Long speciesId) {
		try {
			Map<String, Long> hashToDocCount = service.getGeoAggregation(index, type, geoField, precision, top, left, bottom, right, speciesId); 
			return Response.ok().entity(hashToDocCount).build();
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}
}