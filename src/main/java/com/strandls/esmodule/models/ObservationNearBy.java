/**
 *
 */
package com.strandls.esmodule.models;

/**
 * @author Abhishek Rudra
 *
 */
public class ObservationNearBy {

	private Long observationId;
	private String name;
	private String thumbnail;
	private Double distance;
	private String speciesGroupName;

	/**
	 *
	 */
	public ObservationNearBy() {
		super();
	}

	/**
	 * @param observationId
	 * @param name
	 * @param thumbnail
	 * @param distance
	 * @param speciesGroupName
	 */
	public ObservationNearBy(Long observationId, String name, String thumbnail, Double distance,
			String speciesGroupName) {
		super();
		this.observationId = observationId;
		this.name = name;
		this.thumbnail = thumbnail;
		this.distance = distance;
		this.speciesGroupName = speciesGroupName;
	}

	public Long getObservationId() {
		return observationId;
	}

	public void setObservationId(Long observationId) {
		this.observationId = observationId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getThumbnail() {
		return thumbnail;
	}

	public void setThumbnail(String thumbnail) {
		this.thumbnail = thumbnail;
	}

	public Double getDistance() {
		return distance;
	}

	public void setDistance(Double distance) {
		this.distance = distance;
	}

	public String getSpeciesGroupName() {
		return speciesGroupName;
	}

	public void setSpeciesGroupName(String speciesGroupName) {
		this.speciesGroupName = speciesGroupName;
	}

}
