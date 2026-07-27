
/**
 *
 */
package com.strandls.esmodule.models;

/**
 * @author Abhishek Rudra
 *
 *
 */
public class MaxVotedRecoFreq {

	private Long maxVotedRecoId;
	private Long freq;

	public MaxVotedRecoFreq() {
		super();
	}

	public MaxVotedRecoFreq(Long maxVotedRecoId, Long freq) {
		super();
		this.maxVotedRecoId = maxVotedRecoId;
		this.freq = freq;
	}

	public Long getMaxVotedRecoId() {
		return maxVotedRecoId;
	}

	public void setMaxVotedRecoId(Long maxVotedRecoId) {
		this.maxVotedRecoId = maxVotedRecoId;
	}

	public Long getFreq() {
		return freq;
	}

	public void setFreq(Long freq) {
		this.freq = freq;
	}

}
