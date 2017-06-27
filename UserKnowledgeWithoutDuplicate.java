package hibernate;

/**
 * UserKnowledgeWithoutDuplicate entity. @author MyEclipse Persistence Tools
 */
public class UserKnowledgeWithoutDuplicate extends
		AbstractUserKnowledgeWithoutDuplicate implements java.io.Serializable {

	// Constructors

	/** default constructor */
	public UserKnowledgeWithoutDuplicate() {
	}

	/** full constructor */
	public UserKnowledgeWithoutDuplicate(Integer userId, Integer knowledgeId,
			Double isWrong) {
		super(userId, knowledgeId, isWrong);
	}

}
