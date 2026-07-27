package com.strandls.esmodule.models;

import java.util.List;

public class MapBoundParams {

	private MapBounds bounds;

	private List<MapGeoPoint> polygon;

	private List<List<MapGeoPoint>> multipolygon;

	public MapBoundParams() {
		super();
	}

	public MapBoundParams(MapBounds bounds, List<MapGeoPoint> polygon, List<List<MapGeoPoint>> multipolygon) {
		super();
		this.bounds = bounds;
		this.polygon = polygon;
		this.multipolygon = multipolygon;
	}

	public MapBounds getBounds() {
		return bounds;
	}

	public void setBounds(MapBounds bounds) {
		this.bounds = bounds;
	}

	public List<MapGeoPoint> getPolygon() {
		return polygon;
	}

	public void setPolygon(List<MapGeoPoint> polygon) {
		this.polygon = polygon;
	}

	@Override
	public String toString() {
		return "MapBoundParams [bounds=" + bounds + ", polygon=" + polygon + "]";
	}

	public List<List<MapGeoPoint>> getMultipolygon() {
		return multipolygon;
	}

	public void setMultipolygon(List<List<MapGeoPoint>> multipolygon) {
		this.multipolygon = multipolygon;
	}

}
