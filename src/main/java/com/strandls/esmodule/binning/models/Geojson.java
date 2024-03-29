package com.strandls.esmodule.binning.models;

import java.util.Collection;

public class Geojson {

	private static final String type = "FeatureCollection";

	private Collection<Feature> features;

	public Geojson(Collection<Feature> features) {
		super();
		this.features = features;
	}

	public Collection<Feature> getFeatures() {
		return features;
	}

	public void setFeatures(Collection<Feature> features) {
		this.features = features;
	}

	public String getType() {
		return type;
	}
}
