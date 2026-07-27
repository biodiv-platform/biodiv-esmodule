/**
 *
 */
package com.strandls.esmodule.models;

/**
 * @author Abhishek Rudra
 *
 */
public class CustomFieldValues {

	private String value;
	private String valueIcon;

	/**
	 *
	 */
	public CustomFieldValues() {
		super();
	}

	/**
	 * @param value
	 * @param valueIcon
	 */
	public CustomFieldValues(String value, String valueIcon) {
		super();
		this.value = value;
		this.valueIcon = valueIcon;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getValueIcon() {
		return valueIcon;
	}

	public void setValueIcon(String valueIcon) {
		this.valueIcon = valueIcon;
	}

}
