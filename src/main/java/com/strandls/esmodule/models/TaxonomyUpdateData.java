package com.strandls.esmodule.models;

import java.util.List;

public class TaxonomyUpdateData {

	private Long targetId;
	private String name;
	private String normalizedName;
	private String oldName;
	private String italicisedForm;
	private String canonicalForm;
	private String position;
	private String timestamp;
	private List<Breadcrumb> breadCrumbs;
	private String rank;
	private String status;

	public TaxonomyUpdateData() {
	}

	public TaxonomyUpdateData(Long targetId, String name, String normalizedName, String oldName, String italicisedForm,
			String canonicalForm, String position, String timestamp, List<Breadcrumb> breadCrumbs, String rank,
			String status) {
		this.targetId = targetId;
		this.name = name;
		this.normalizedName = normalizedName;
		this.oldName = oldName;
		this.italicisedForm = italicisedForm;
		this.canonicalForm = canonicalForm;
		this.position = position;
		this.timestamp = timestamp;
		this.breadCrumbs = breadCrumbs;
		this.rank = rank;
		this.status = status;
	}

	public Long getTargetId() {
		return targetId;
	}

	public void setTargetId(Long targetId) {
		this.targetId = targetId;
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

	public String getOldName() {
		return oldName;
	}

	public void setOldName(String oldName) {
		this.oldName = oldName;
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

	public List<Breadcrumb> getBreadCrumbs() {
		return breadCrumbs;
	}

	public void setBreadCrumbs(List<Breadcrumb> breadCrumbs) {
		this.breadCrumbs = breadCrumbs;
	}

	public String getRank() {
		return rank;
	}

	public void setRank(String rank) {
		this.rank = rank;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
}