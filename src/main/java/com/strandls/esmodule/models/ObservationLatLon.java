package com.strandls.esmodule.models;

public class ObservationLatLon {

	private Long observationId;
	private Double latitude;
	private Double longitude;

	/**
	 *
	 */
	public ObservationLatLon() {
		super();
	}

	/**
	 * @param observationId
	 * @param latitude
	 * @param longitude
	 */
	public ObservationLatLon(Long observationId, Double latitude, Double longitude) {
		super();
		this.observationId = observationId;
		this.latitude = latitude;
		this.longitude = longitude;
	}

	public Long getObservationId() {
		return observationId;
	}

	public void setObservationId(Long observationId) {
		this.observationId = observationId;
	}

	public Double getLatitude() {
		return latitude;
	}

	public void setLatitude(Double latitude) {
		this.latitude = latitude;
	}

	public Double getLongitude() {
		return longitude;
	}

	public void setLongitude(Double longitude) {
		this.longitude = longitude;
	}

}
