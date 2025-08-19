package com.strandls.esmodule.models;

import java.util.Map;

public class GeoAggregationData {
	Map<String, Long> data = null;

	public GeoAggregationData() {
	}

	public GeoAggregationData(Map<String, Long> geoAggregationData) {
		this.data = geoAggregationData;
	}

	public Map<String, Long> getData() {
		return data;
	}

	public void setGeoAggregationData(Map<String, Long> geoAggregationData) {
		this.data = geoAggregationData;
	}
}
