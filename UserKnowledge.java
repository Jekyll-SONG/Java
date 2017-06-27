package hibernate;

import java.sql.Timestamp;

/**
 * UserKnowledge entity. @author MyEclipse Persistence Tools
 */
public class UserKnowledge extends AbstractUserKnowledge implements
		java.io.Serializable {

	// Constructors

	/** default constructor */
	public UserKnowledge() {
	}

	/** minimal constructor */
	public UserKnowledge(Timestamp date) {
		super(date);
	}

	/** full constructor */
	public UserKnowledge(Integer userId, Integer knowledgeId, Timestamp date,
			Integer isWrong) {
		super(userId, knowledgeId, date, isWrong);
	}

}
