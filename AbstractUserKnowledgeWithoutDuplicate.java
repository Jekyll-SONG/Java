package hibernate;

/**
 * AbstractUserKnowledgeWithoutDuplicate entity provides the base persistence
 * definition of the UserKnowledgeWithoutDuplicate entity. @author MyEclipse
 * Persistence Tools
 */

public abstract class AbstractUserKnowledgeWithoutDuplicate implements
		java.io.Serializable {

	// Fields

	private Integer recordId;
	private Integer userId;
	private Integer knowledgeId;
	private Double isWrong;

	// Constructors

	/** default constructor */
	public AbstractUserKnowledgeWithoutDuplicate() {
	}

	/** full constructor */
	public AbstractUserKnowledgeWithoutDuplicate(Integer userId,
			Integer knowledgeId, Double isWrong) {
		this.userId = userId;
		this.knowledgeId = knowledgeId;
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

	public Double getIsWrong() {
		return this.isWrong;
	}

	public void setIsWrong(Double isWrong) {
		this.isWrong = isWrong;
	}

}