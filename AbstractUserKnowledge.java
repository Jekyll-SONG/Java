package hibernate;

import java.sql.Timestamp;

/**
 * AbstractUserKnowledge entity provides the base persistence definition of the
 * UserKnowledge entity. @author MyEclipse Persistence Tools
 */

public abstract class AbstractUserKnowledge implements java.io.Serializable {

	// Fields

	private Integer recordId;
	private Integer userId;
	private Integer knowledgeId;
	private Timestamp date;
	private Integer isWrong;

	// Constructors

	/** default constructor */
	public AbstractUserKnowledge() {
	}

	/** minimal constructor */
	public AbstractUserKnowledge(Timestamp date) {
		this.date = date;
	}

	/** full constructor */
	public AbstractUserKnowledge(Integer userId, Integer knowledgeId,
			Timestamp date, Integer isWrong) {
		this.userId = userId;
		this.knowledgeId = knowledgeId;
		this.date = date;
		this.isWrong = isWrong;
	}

	// Property accessors

	public Integer getRecordId() {
		return this.recordId;
	}

	public void setRecordId(Integer recordId) {
		this.recordId = recordId;
	}

	public Integer getUserId() {
		return this.userId;
	}

	public void setUserId(Integer userId) {
		this.userId = userId;
	}

	public Integer getKnowledgeId() {
		return this.knowledgeId;
	}

	public void setKnowledgeId(Integer knowledgeId) {
		this.knowledgeId = knowledgeId;
	}

	public Timestamp getDate() {
		return this.date;
	}

	public void setDate(Timestamp date) {
		this.date = date;
	}

	public Integer getIsWrong() {
		return this.isWrong;
	}

	public void setIsWrong(Integer isWrong) {
		this.isWrong = isWrong;
	}

}