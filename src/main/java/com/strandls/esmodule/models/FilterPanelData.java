/**
 *
 */
package com.strandls.esmodule.models;

import java.util.List;

/**
 * @author Abhishek Rudra
 *
 */
public class FilterPanelData {

	private List<SpeciesGroup> speciesGroup;
	private List<UserGroup> userGroup;
	private List<String> states;
	private List<Traits> traits;
	private List<CustomFields> customFields;

	/**
	 *
	 */
	public FilterPanelData() {
		super();
	}

	/**
	 * @param speciesGroup
	 * @param userGroup
	 * @param states
	 * @param traits
	 * @param customFields
	 */
	public FilterPanelData(List<SpeciesGroup> speciesGroup, List<UserGroup> userGroup, List<String> states,
			List<Traits> traits, List<CustomFields> customFields) {
		super();
		this.speciesGroup = speciesGroup;
		this.userGroup = userGroup;
		this.states = states;
		this.traits = traits;
		this.customFields = customFields;
	}

	public List<SpeciesGroup> getSpeciesGroup() {
		return speciesGroup;
	}

	public void setSpeciesGroup(List<SpeciesGroup> speciesGroup) {
		this.speciesGroup = speciesGroup;
	}

	public List<UserGroup> getUserGroup() {
		return userGroup;
	}

	public void setUserGroup(List<UserGroup> userGroup) {
		this.userGroup = userGroup;
	}

	public List<String> getStates() {
		return states;
	}

	public void setStates(List<String> states) {
		this.states = states;
	}

	public List<Traits> getTraits() {
		return traits;
	}

	public void setTraits(List<Traits> traits) {
		this.traits = traits;
	}

	public List<CustomFields> getCustomFields() {
		return customFields;
	}

	public void setCustomFields(List<CustomFields> customFields) {
		this.customFields = customFields;
	}

}
