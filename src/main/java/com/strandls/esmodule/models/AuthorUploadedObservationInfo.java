/**
 * 
 */
package com.strandls.esmodule.models;

import java.util.List;

/**
 * @author Abhishek Rudra
 *
 * 
 */
public class AuthorUploadedObservationInfo {

	private Long totalCount;
	private List<MaxVotedRecoFreq> maxVotedRecoFreqs;

	public AuthorUploadedObservationInfo() {
		super();
	}

	public AuthorUploadedObservationInfo(Long totalCount, List<MaxVotedRecoFreq> maxVotedRecoFreqs) {
		super();
		this.totalCount = totalCount;
		this.maxVotedRecoFreqs = maxVotedRecoFreqs;
	}

	public Long getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(Long totalCount) {
		this.totalCount = totalCount;
	}

	public List<MaxVotedRecoFreq> getMaxVotedRecoFreqs() {
		return maxVotedRecoFreqs;
	}

	public void setMaxVotedRecoFreqs(List<MaxVotedRecoFreq> maxVotedRecoFreqs) {
		this.maxVotedRecoFreqs = maxVotedRecoFreqs;
	}

}
