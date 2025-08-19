/**
 *
 */
package com.strandls.esmodule.models;

/**
 * @author Abhishek Rudra
 *
 */
public class SimilarObservation {
	private Long observationId;
	private String name;
	private String reprImage;

	/**
	 *
	 */
	public SimilarObservation() {
		super();
	}

	/**
	 * @param observationId
	 * @param name
	 * @param reprImage
	 */
	public SimilarObservation(Long observationId, String name, String reprImage) {
		super();
		this.observationId = observationId;
		this.name = name;
		this.reprImage = reprImage;
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

	public String getReprImage() {
		return reprImage;
	}

	public void setReprImage(String reprImage) {
		this.reprImage = reprImage;
	}

}
