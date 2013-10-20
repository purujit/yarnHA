package org.apache.hadoop.yarn.server.resourcemanager.state;

import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * A StateManager implementation for single node RM.
 * 
 */
public class SingleNodeStateManager implements RMStateManager {
	public SingleNodeStateManager() {
	}

	public void registerApplicationId(ApplicationId appId)
			throws StateException {
	}
}
