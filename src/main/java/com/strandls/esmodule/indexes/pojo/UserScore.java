/**
 *
 */
package com.strandls.esmodule.indexes.pojo;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author Abhishek Rudra
 *
 */
public class UserScore {
	List<LinkedHashMap<String, LinkedHashMap<String, String>>> record;

	public List<LinkedHashMap<String, LinkedHashMap<String, String>>> getRecord() {
		return record;
	}

	public void setRecord(List<LinkedHashMap<String, LinkedHashMap<String, String>>> record) {
		this.record = record;
	}

}
