package com.strandls.esmodule.models;

public class TaxonMatchQuery {
	 
	private String scientificText;
	private String canonicalText;
 
	public TaxonMatchQuery() {
	}
 
	public TaxonMatchQuery(String scientificText, String canonicalText) {
		this.scientificText = scientificText;
		this.canonicalText = canonicalText;
	}
 
	public String getScientificText() {
		return scientificText;
	}
 
	public void setScientificText(String scientificText) {
		this.scientificText = scientificText;
	}
 
	public String getCanonicalText() {
		return canonicalText;
	}
 
	public void setCanonicalText(String canonicalText) {
		this.canonicalText = canonicalText;
	}
}