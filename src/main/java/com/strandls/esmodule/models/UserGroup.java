/**
 *
 */
package com.strandls.esmodule.models;

/**
 * @author Abhishek Rudra
 *
 */
public class UserGroup {

	private Long id;
	private String name;
	private String webAddress;

	/**
	 *
	 */
	public UserGroup() {
		super();
	}

	/**
	 * @param id
	 * @param name
	 * @param webAddress
	 */
	public UserGroup(Long id, String name, String webAddress) {
		super();
		this.id = id;
		this.name = name;
		this.webAddress = webAddress;
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

	public String getWebAddress() {
		return webAddress;
	}

	public void setWebAddress(String webAddress) {
		this.webAddress = webAddress;
	}

}
