/**
 *
 */
package com.strandls.esmodule.models;

import java.util.List;

/**
 * @author Abhishek Rudra
 *
 */
public class CustomFields {
	private Long id;
	private String name;
	private String fieldtype;
	private String dataType;
	private List<CustomFieldValues> values;

	/**
	 *
	 */
	public CustomFields() {
		super();
	}

	/**
	 * @param id
	 * @param name
	 * @param fieldtype
	 * @param dataType
	 * @param values
	 */
	public CustomFields(Long id, String name, String fieldtype, String dataType, List<CustomFieldValues> values) {
		super();
		this.id = id;
		this.name = name;
		this.fieldtype = fieldtype;
		this.dataType = dataType;
		this.values = values;
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

	public String getFieldtype() {
		return fieldtype;
	}

	public void setFieldtype(String fieldtype) {
		this.fieldtype = fieldtype;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public List<CustomFieldValues> getValues() {
		return values;
	}

	public void setValues(List<CustomFieldValues> values) {
		this.values = values;
	}

}
