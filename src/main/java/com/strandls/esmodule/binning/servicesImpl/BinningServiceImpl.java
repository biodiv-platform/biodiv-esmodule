package com.strandls.esmodule.binning.servicesImpl;

import java.io.IOException;

import com.strandls.esmodule.binning.models.GeojsonData;
import com.strandls.esmodule.binning.services.BinningService;
import com.strandls.esmodule.models.MapBounds;
import com.strandls.esmodule.utils.GeoGridService;

import jakarta.inject.Inject;

/**
 * Bin the geographical data in different shapes and size.
 *
 * @author mukund
 *
 */
public class BinningServiceImpl implements BinningService {

	@Inject
	private GeojsonServiceImpl geojsonService;

	public GeojsonData squareBin(String index, String type, String geoField, MapBounds bounds, Double cellSideKm)
			throws IOException {

		double[][][] coordinatesList = GeoGridService.squareGrid(bounds.getRight(), bounds.getLeft(), bounds.getTop(),
				bounds.getBottom(), cellSideKm);

		return geojsonService.getGeojsonData(index, type, geoField, coordinatesList);
	}

}
