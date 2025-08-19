package com.strandls.esmodule.controllers;

import java.io.IOException;

import com.strandls.esmodule.ApiConstants;
import com.strandls.esmodule.binning.models.GeojsonData;
import com.strandls.esmodule.binning.servicesImpl.BinningServiceImpl;
import com.strandls.esmodule.models.MapBounds;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.inject.Inject;
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
 * Controller for binning related services
 */
@Tag(name = "Binning Service", description = "Spatial binning API")
@Path(ApiConstants.V1 + ApiConstants.BINNING)
public class BinningController {

	@Inject
	BinningServiceImpl service;

	@POST
	@Path(ApiConstants.SQUARE + "/{index}/{type}")
	@Produces(MediaType.APPLICATION_JSON)
	@Operation(summary = "Binning", description = "Returns square-binned spatial data as GeoJSON")
	@ApiResponses({
			@ApiResponse(responseCode = "200", description = "Success", content = @Content(mediaType = "application/json", schema = @Schema(implementation = GeojsonData.class))),
			@ApiResponse(responseCode = "500", description = "Internal Server Error", content = @Content(mediaType = "application/json", schema = @Schema(type = "string"))) })
	public GeojsonData bin(@PathParam("index") String index, @PathParam("type") String type,
			@QueryParam("geoField") String geoField, @QueryParam("cellSideKm") Double cellSideKm, MapBounds bounds) {

		try {
			return service.squareBin(index, type, geoField, bounds, cellSideKm);
		} catch (IOException e) {
			throw new WebApplicationException(
					Response.status(Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build());
		}
	}
}
