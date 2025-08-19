/**
 *
 */
package com.strandls.esmodule.models;

/**
 * @author Abhishek Rudra
 *
 */
public class SpeciesGroup {
	private Long id;
	private String name;
	private Integer order;

	/**
	 *
	 */
	public SpeciesGroup() {
		super();
	}

	/**
	 * @param id
	 * @param name
	 * @param order
	 */
	public SpeciesGroup(Long id, String name, Integer order) {
		super();
		this.id = id;
		this.name = name;
		this.order = order;
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

	public Integer getOrder() {
		return order;
	}

	public void setOrder(Integer order) {
		this.order = order;
	}

}
