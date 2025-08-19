package com.strandls.esmodule.models;

import java.io.Serializable;

public class ForceUpdateResponse implements Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = -933711373941353953L;

	private Long documentSent;
	private Long documentFound;
	private Long documentUpdated;

	public ForceUpdateResponse(Long documentSent, Long documentFound, Long documentUpdated) {
		super();
		this.documentSent = documentSent;
		this.documentFound = documentFound;
		this.documentUpdated = documentUpdated;
	}

	public Long getDocumentSent() {
		return documentSent;
	}

	public void setDocumentSent(Long documentSent) {
		this.documentSent = documentSent;
	}

	public Long getDocumentFound() {
		return documentFound;
	}

	public void setDocumentFound(Long documentFound) {
		this.documentFound = documentFound;
	}

	public Long getDocumentUpdated() {
		return documentUpdated;
	}

	public void setDocumentUpdated(Long documentUpdated) {
		this.documentUpdated = documentUpdated;
	}

}
