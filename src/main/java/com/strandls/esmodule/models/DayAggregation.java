/**
 *
 */
package com.strandls.esmodule.models;

/**
 * @author Rishitha Ravi
 *
 *
 */
public class DayAggregation {

	private String date;
	private Long value;

	public DayAggregation() {
		super();
	}

	public DayAggregation(String date, Long value) {
		super();
		this.date = date;
		this.value = value;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public Long getValue() {
		return value;
	}

	public void setValue(Long value) {
		this.value = value;
	}

}