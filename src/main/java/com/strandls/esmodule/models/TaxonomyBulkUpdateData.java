package com.strandls.esmodule.models;

import java.util.List;

public class TaxonomyBulkUpdateData {

	private Long targetId;
	private Long recoId;
	private Long speciesId;
	private String position;
	private String timestamp;
	private String name;
	private String normalizedName;
	private String italicisedForm;
	private String canonicalForm;
	private String binomialForm;
	private String scientificName;
	private String title;
	private String status;
	private List<Long> transferSynonymIds;
	private Long newId;
	private List<Breadcrumb> acceptedBreadCrumbs;

	public TaxonomyBulkUpdateData() {
	}

	public Long getTargetId() {
		return targetId;
	}

	public void setTargetId(Long targetId) {
		this.targetId = targetId;
	}

	public Long getRecoId() {
		return recoId;
	}

	public void setRecoId(Long recoId) {
		this.recoId = recoId;
	}

	public Long getSpeciesId() {
		return speciesId;
	}

	public void setSpeciesId(Long speciesId) {
		this.speciesId = speciesId;
	}

	public String getPosition() {
		return position;
	}

	public void setPosition(String position) {
		this.position = position;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getNormalizedName() {
		return normalizedName;
	}

	public void setNormalizedName(String normalizedName) {
		this.normalizedName = normalizedName;
	}

	public String getItalicisedForm() {
		return italicisedForm;
	}

	public void setItalicisedForm(String italicisedForm) {
		this.italicisedForm = italicisedForm;
	}

	public String getCanonicalForm() {
		return canonicalForm;
	}

	public void setCanonicalForm(String canonicalForm) {
		this.canonicalForm = canonicalForm;
	}

	public String getBinomialForm() {
		return binomialForm;
	}

	public void setBinomialForm(String binomialForm) {
		this.binomialForm = binomialForm;
	}

	public String getScientificName() {
		return scientificName;
	}

	public void setScientificName(String scientificName) {
		this.scientificName = scientificName;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
	
	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public List<Long> getTransferSynonymIds() {
		return transferSynonymIds;
	}

	public void setTransferSynonymIds(List<Long> transferSynonymIds) {
		this.transferSynonymIds = transferSynonymIds;
	}

	public Long getNewId() {
		return newId;
	}

	public void setNewId(Long newId) {
		this.newId = newId;
	}
	
	public List<Breadcrumb> getAcceptedBreadCrumbs() {
		return acceptedBreadCrumbs;
	}

	public void setAcceptedBreadCrumbs(List<Breadcrumb> acceptedBreadCrumbs) {
		this.acceptedBreadCrumbs = acceptedBreadCrumbs;
	}
}