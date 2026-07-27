package com.strandls.esmodule.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Breadcrumb {

	@JsonProperty("taxon_name")
	private String taxonName;

	@JsonProperty("taxon_rank")
	private String taxonRank;

	@JsonProperty("taxon_id")
	private Long taxonId;

	public Breadcrumb() {
	}

	public Breadcrumb(String taxonName, String taxonRank, Long taxonId) {
		this.taxonName = taxonName;
		this.taxonRank = taxonRank;
		this.taxonId = taxonId;
	}

	public String getTaxonName() {
		return taxonName;
	}

	public void setTaxonName(String taxonName) {
		this.taxonName = taxonName;
	}

	public String getTaxonRank() {
		return taxonRank;
	}

	public void setTaxonRank(String taxonRank) {
		this.taxonRank = taxonRank;
	}

	public Long getTaxonId() {
		return taxonId;
	}

	public void setTaxonId(Long taxonId) {
		this.taxonId = taxonId;
	}
}