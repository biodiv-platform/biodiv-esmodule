/**
 *
 */
package com.strandls.esmodule.binning.services;

import java.io.IOException;

import com.strandls.esmodule.binning.models.GeojsonData;

/**
 * @author Abhishek Rudra
 *
 */
public interface GeojsonService {

	/*
	 *
	 */
	public GeojsonData getGeojsonData(String index, String type, String geoField, double[][][] coordinatesList)
			throws IOException;
}
