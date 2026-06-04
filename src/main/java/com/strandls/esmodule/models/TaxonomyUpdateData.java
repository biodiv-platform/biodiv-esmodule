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
	private List<Long> transferSynonymIds;
	private List<Object> commonNames;
	private Long newId;
	private List<Long> bulkIds;

	public TaxonomyUpdateData() {
	}

	public TaxonomyUpdateData(Long targetId, String name, String normalizedName, String oldName, String italicisedForm,
			String canonicalForm, String position, String timestamp, List<Breadcrumb> breadCrumbs, String rank,
			String status, List<Long> transferSynonymIds, Long newId, List<Object> commonNames, List<Long> bulkIds) {
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
		this.transferSynonymIds = transferSynonymIds;
		this.newId = newId;
		this.commonNames = commonNames;
		this.bulkIds = bulkIds;
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
	
	public List<Long> getTransferSynonymIds(){
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
	
	public List<Object> getCommonNames(){
		return commonNames;
	}
	
	public void setCommonNames(List<Object> commonNames) {
		this.commonNames = commonNames;
	}
	
	public List<Long> getBulkIds(){
		return bulkIds;
	}
	
	public void setBulkIds(List<Long> bulkIds) {
		this.bulkIds = bulkIds;
	}
}