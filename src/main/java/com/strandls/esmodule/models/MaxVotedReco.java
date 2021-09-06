/**
 * 
 */
package com.strandls.esmodule.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author Abhishek Rudra
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MaxVotedReco {

	private Long id;
	private String scientific_name;
	private String rank;
	private String taxonstatus;

	/**
	 * 
	 */
	public MaxVotedReco() {
		super();
	}

	/**
	 * @param id
	 * @param scientific_name
	 * @param rank
	 * @param taxonstatus
	 */
	public MaxVotedReco(Long id, String scientific_name, String rank, String taxonstatus) {
		super();
		this.id = id;
		this.scientific_name = scientific_name;
		this.rank = rank;
		this.taxonstatus = taxonstatus;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getScientific_name() {
		return scientific_name;
	}

	public void setScientific_name(String scientific_name) {
		this.scientific_name = scientific_name;
	}

	public String getRank() {
		return rank;
	}

	public void setRank(String rank) {
		this.rank = rank;
	}

	public String getTaxonstatus() {
		return taxonstatus;
	}

	public void setTaxonstatus(String taxonstatus) {
		this.taxonstatus = taxonstatus;
	}

}
