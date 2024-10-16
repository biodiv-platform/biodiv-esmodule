/**
 * 
 */
package com.strandls.esmodule.models;

/**
 * @author Rishitha Ravi
 *
 * 
 */
public class MonthAggregation {

	private String month;
	private String year;
	private Long value;

	public MonthAggregation() {
		super();
	}

	public MonthAggregation(String month, String year, Long value) {
		super();
		this.month = month;
		this.year = year;
		this.value = value;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public Long getValue() {
		return value;
	}

	public void setValue(Long value) {
		this.value = value;
	}

}