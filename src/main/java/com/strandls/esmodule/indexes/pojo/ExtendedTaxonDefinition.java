package com.strandls.esmodule.indexes.pojo;

import java.util.List;
import java.util.Map;

public class ExtendedTaxonDefinition implements ElasticIndexes {

	private Integer parent_taxon_definition_id;
	private String group_name;
	private List<Integer> accepted_ids;
	private List<Map<String, String>> hierarchy;
	private String italicised_form;
	private Integer species_id;
	private String species_title;
	private String path;
	private String repr_image_id;
	private String repr_image_url;
	private Float group_id;
	private String name;
	private List<CommonName> common_names;
	private String rank;
	private Integer id;
	private String position;
	private String lowercase_match_name;
	private String canonical_form;
	private String status;
	private List<String> accepted_names;

	public ExtendedTaxonDefinition() {
		super();
	}

	/**
	 * @param parent_taxon_definition_id
	 * @param group_name
	 * @param accepted_ids
	 * @param hierarchy
	 * @param italicised_form
	 * @param species_id
	 * @param species_title
	 * @param path
	 * @param repr_image_id
	 * @param repr_image_url
	 * @param group_id
	 * @param name
	 * @param common_names
	 * @param rank
	 * @param id
	 * @param position
	 * @param lowercase_match_name
	 * @param canonical_form
	 * @param status
	 * @param accepted_names
	 */
	public ExtendedTaxonDefinition(Integer parent_taxon_definition_id, String group_name, List<Integer> accepted_ids,
			List<Map<String, String>> hierarchy, String italicised_form, Integer species_id, String species_title,
			String path, String repr_image_id, String repr_image_url, Float group_id, String name,
			List<CommonName> common_names, String rank, Integer id, String position, String lowercase_match_name,
			String canonical_form, String status, List<String> accepted_names) {
		super();
		this.parent_taxon_definition_id = parent_taxon_definition_id;
		this.group_name = group_name;
		this.accepted_ids = accepted_ids;
		this.hierarchy = hierarchy;
		this.italicised_form = italicised_form;
		this.species_id = species_id;
		this.species_title = species_title;
		this.path = path;
		this.repr_image_id = repr_image_id;
		this.repr_image_url = repr_image_url;
		this.group_id = group_id;
		this.name = name;
		this.common_names = common_names;
		this.rank = rank;
		this.id = id;
		this.position = position;
		this.lowercase_match_name = lowercase_match_name;
		this.canonical_form = canonical_form;
		this.status = status;
		this.accepted_names = accepted_names;
	}

	public Integer getParent_taxon_definition_id() {
		return parent_taxon_definition_id;
	}

	public void setParent_taxon_definition_id(Integer parent_taxon_definition_id) {
		this.parent_taxon_definition_id = parent_taxon_definition_id;
	}

	public String getGroup_name() {
		return group_name;
	}

	public void setGroup_name(String group_name) {
		this.group_name = group_name;
	}

	public List<Integer> getAccepted_ids() {
		return accepted_ids;
	}

	public void setAccepted_ids(List<Integer> accepted_ids) {
		this.accepted_ids = accepted_ids;
	}

	public List<Map<String, String>> getHierarchy() {
		return hierarchy;
	}

	public void setHierarchy(List<Map<String, String>> hierarchy) {
		this.hierarchy = hierarchy;
	}

	public String getItalicised_form() {
		return italicised_form;
	}

	public void setItalicised_form(String italicised_form) {
		this.italicised_form = italicised_form;
	}

	public Integer getSpecies_id() {
		return species_id;
	}

	public void setSpecies_id(Integer species_id) {
		this.species_id = species_id;
	}

	public String getSpecies_title() {
		return species_title;
	}

	public void setSpecies_title(String species_title) {
		this.species_title = species_title;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getRepr_image_id() {
		return repr_image_id;
	}

	public void setRepr_image_id(String repr_image_id) {
		this.repr_image_id = repr_image_id;
	}

	public String getRepr_image_url() {
		return repr_image_url;
	}

	public void setRepr_image_url(String repr_image_url) {
		this.repr_image_url = repr_image_url;
	}

	public Float getGroup_id() {
		return group_id;
	}

	public void setGroup_id(Float group_id) {
		this.group_id = group_id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<CommonName> getCommon_names() {
		return common_names;
	}

	public void setCommon_names(List<CommonName> common_names) {
		this.common_names = common_names;
	}

	public String getRank() {
		return rank;
	}

	public void setRank(String rank) {
		this.rank = rank;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getPosition() {
		return position;
	}

	public void setPosition(String position) {
		this.position = position;
	}

	public String getLowercase_match_name() {
		return lowercase_match_name;
	}

	public void setLowercase_match_name(String lowercase_match_name) {
		this.lowercase_match_name = lowercase_match_name;
	}

	public String getCanonical_form() {
		return canonical_form;
	}

	public void setCanonical_form(String canonical_form) {
		this.canonical_form = canonical_form;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public List<String> getAccepted_names() {
		return accepted_names;
	}

	public void setAccepted_names(List<String> accepted_names) {
		this.accepted_names = accepted_names;
	}

}
