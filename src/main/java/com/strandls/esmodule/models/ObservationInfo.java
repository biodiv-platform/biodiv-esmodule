/**
 * 
 */
package com.strandls.esmodule.models;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Abhishek Rudra
 *
 */
public class ObservationInfo {

	private Map<Object, Long> monthAggregation;
	private List<SimilarObservation> similarObservation;
	private List<ObservationMapInfo> latlon;

	/**
	 * 
	 */
	public ObservationInfo() {
		super();
	}

	/**
	 * @param monthAggregation
	 * @param similarObservation
	 * @param latlon
	 */
	public ObservationInfo(Map<Object, Long> monthAggregation, List<SimilarObservation> similarObservation,
			List<ObservationMapInfo> latlon) {
		super();
		this.monthAggregation = monthAggregation;
		this.similarObservation = similarObservation;
		this.latlon = latlon;
	}

	public Map<Object, Long> getMonthAggregation() {
		return monthAggregation;
	}

	public void setMonthAggregation(HashMap<Object, Long> monthAggregation) {
		this.monthAggregation = monthAggregation;
	}

	public List<SimilarObservation> getSimilarObservation() {
		return similarObservation;
	}

	public void setSimilarObservation(List<SimilarObservation> similarObservation) {
		this.similarObservation = similarObservation;
	}

	public List<ObservationMapInfo> getLatlon() {
		return latlon;
	}

	public void setLatlon(List<ObservationMapInfo> latlon) {
		this.latlon = latlon;
	}

}
