/**
 *
 */
package com.strandls.esmodule.models;

import java.util.Map;

/**
 * @author Abhishek Rudra
 *
 */
public class GeoHashAggregationData {

	private Map<String, Long> geoHashData;
	private Long totalCount;

	/**
	 *
	 */
	public GeoHashAggregationData() {
		super();
	}

	/**
	 * @param geoHashData
	 * @param totalCount
	 */
	public GeoHashAggregationData(Map<String, Long> geoHashData, Long totalCount) {
		super();
		this.geoHashData = geoHashData;
		this.totalCount = totalCount;
	}

	public Map<String, Long> getGeoHashData() {
		return geoHashData;
	}

	public void setGeoHashData(Map<String, Long> geoHashData) {
		this.geoHashData = geoHashData;
	}

	public Long getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(Long totalCount) {
		this.totalCount = totalCount;
	}

}
