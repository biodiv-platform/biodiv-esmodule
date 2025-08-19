/**
 *
 */
package com.strandls.esmodule.models;

import java.util.List;

/**
 * @author Abhishek Rudra
 *
 */
public class Traits {
	private Long id;
	private String name;
	private String type;
	private List<TraitValue> traitValues;

	/**
	 *
	 */
	public Traits() {
		super();
	}

	/**
	 * @param id
	 * @param name
	 * @param type
	 * @param traitValues
	 */
	public Traits(Long id, String name, String type, List<TraitValue> traitValues) {
		super();
		this.id = id;
		this.name = name;
		this.type = type;
		this.traitValues = traitValues;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public List<TraitValue> getTraitValues() {
		return traitValues;
	}

	public void setTraitValues(List<TraitValue> traitValues) {
		this.traitValues = traitValues;
	}

}
