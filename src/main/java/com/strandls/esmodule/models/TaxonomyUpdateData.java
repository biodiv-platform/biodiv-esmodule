package com.strandls.esmodule.models;

import java.util.List;

public class TaxonomyUpdateData {

	private Long targetId;
	private Long recoId;
	private Long speciesId;
	private String name;
	private String normalizedName;
	private String oldName;
	private String italicisedForm;
	private String canonicalForm;
	private String binomialForm;
	private String position;
	private String timestamp;
	private String scientificName;
	private String title;
	private List<Breadcrumb> breadCrumbs;
	private String rank;
	private String status;
	private List<Long> transferSynonymIds;
	private List<Object> commonNames;
	private Long newId;
	private List<Long> bulkIds;
	private List<Long> deleteRecoIds;
	private List<Long> transferRecoIds;
	private List<Long> deleteSpeciesIds;
	private List<Breadcrumb> acceptedBreadCrumbs;
	private List<Object> synonyms;

	public TaxonomyUpdateData() {
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

	public String getBinomialForm() {
		return binomialForm;
	}

	public void setBinomialForm(String binomialForm) {
		this.binomialForm = binomialForm;
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

	public List<Breadcrumb> getBreadCrumbs() {
		return breadCrumbs;
	}

	public void setBreadCrumbs(List<Breadcrumb> breadCrumbs) {
		this.breadCrumbs = breadCrumbs;
	}

	public List<Breadcrumb> getAcceptedBreadCrumbs() {
		return acceptedBreadCrumbs;
	}

	public void setAcceptedBreadCrumbs(List<Breadcrumb> acceptedBreadCrumbs) {
		this.acceptedBreadCrumbs = acceptedBreadCrumbs;
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

	public List<Object> getCommonNames() {
		return commonNames;
	}

	public void setCommonNames(List<Object> commonNames) {
		this.commonNames = commonNames;
	}

	public List<Object> getSynonyms() {
		return synonyms;
	}

	public void setSynonyms(List<Object> synonyms) {
		this.synonyms = synonyms;
	}

	public List<Long> getBulkIds() {
		return bulkIds;
	}

	public void setBulkIds(List<Long> bulkIds) {
		this.bulkIds = bulkIds;
	}

	public List<Long> getDeleteRecoIds() {
		return deleteRecoIds;
	}

	public void setDeleteRecoIds(List<Long> deleteRecoIds) {
		this.deleteRecoIds = deleteRecoIds;
	}

	public List<Long> getTransferRecoIds() {
		return transferRecoIds;
	}

	public void setTransferRecoIds(List<Long> transferRecoIds) {
		this.transferRecoIds = transferRecoIds;
	}

	public List<Long> getDeleteSpeciesIds() {
		return deleteSpeciesIds;
	}

	public void setDeleteSpeciesIds(List<Long> deleteSpeciesIds) {
		this.deleteSpeciesIds = deleteSpeciesIds;
	}
}