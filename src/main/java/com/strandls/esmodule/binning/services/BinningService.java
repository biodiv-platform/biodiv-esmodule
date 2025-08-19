/**
 *
 */
package com.strandls.esmodule.binning.services;

import java.io.IOException;

import com.strandls.esmodule.binning.models.GeojsonData;
import com.strandls.esmodule.models.MapBounds;

/**
 * @author Abhishek Rudra
 *
 */
public interface BinningService {

	/*
	 * Bin the geographical data in different shapes and size.
	 */
	public GeojsonData squareBin(String index, String type, String geoField, MapBounds bounds, Double cellSideKm)
			throws IOException;

}
