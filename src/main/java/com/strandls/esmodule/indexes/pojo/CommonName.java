package com.strandls.esmodule.indexes.pojo;

public class CommonName {
	private String three_letter_code;
	private String name;
	private String language_name;
	private Integer id;
	private Integer language_id;

	public CommonName() {
		super();
	}

	public CommonName(String three_letter_code, String name, String language_name, Integer id, Integer language_id) {
		super();
		this.three_letter_code = three_letter_code;
		this.name = name;
		this.language_name = language_name;
		this.id = id;
		this.language_id = language_id;
	}

	public String getThree_letter_code() {
		return three_letter_code;
	}

	public void setThree_letter_code(String three_letter_code) {
		this.three_letter_code = three_letter_code;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getLanguage_name() {
		return language_name;
	}

	public void setLanguage_name(String language_name) {
		this.language_name = language_name;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Integer getLanguage_id() {
		return language_id;
	}

	public void setLanguage_id(Integer language_id) {
		this.language_id = language_id;
	}
}
