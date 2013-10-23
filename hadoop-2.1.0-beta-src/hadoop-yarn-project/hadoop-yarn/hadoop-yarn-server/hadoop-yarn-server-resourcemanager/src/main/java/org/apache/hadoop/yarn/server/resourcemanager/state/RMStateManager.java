package org.apache.hadoop.yarn.server.resourcemanager.state;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;

public interface RMStateManager {
	void registerApplicationId(ApplicationId appId) throws RMStateException;
	void registerApplicationSubmission(ApplicationSubmissionContext submissionContext) throws RMStateException;
}
