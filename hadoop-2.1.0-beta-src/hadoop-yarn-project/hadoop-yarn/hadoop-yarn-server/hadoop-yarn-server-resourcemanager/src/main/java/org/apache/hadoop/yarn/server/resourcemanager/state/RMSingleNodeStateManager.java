package org.apache.hadoop.yarn.server.resourcemanager.state;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;

/**
 * A StateManager implementation for single node RM.
 * 
 */
public class RMSingleNodeStateManager implements RMStateManager {
	public RMSingleNodeStateManager() {
	}

	public void registerApplicationId(ApplicationId appId)
			throws RMStateException {
	}

	public void registerApplicationSubmission(
			ApplicationSubmissionContext submissionContext)
			throws RMStateException {
	}
}
