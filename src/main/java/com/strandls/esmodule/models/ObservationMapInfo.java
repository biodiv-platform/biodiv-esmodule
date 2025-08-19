/**
 *
 */
package com.strandls.esmodule.models;

/**
 * @author Abhishek Rudra
 *
 */
public class ObservationMapInfo {

	private Long id;
	private String name;
	private Double latitude;
	private Double longitude;

	/**
	 *
	 */
	public ObservationMapInfo() {
		super();
	}

	/**
	 * @param id
	 * @param name
	 * @param latitude
	 * @param longitude
	 */
	public ObservationMapInfo(Long id, String name, Double latitude, Double longitude) {
		super();
		this.id = id;
		this.name = name;
		this.latitude = latitude;
		this.longitude = longitude;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
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
