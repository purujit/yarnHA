package org.apache.hadoop.yarn.server.resourcemanager.state;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public interface RMStateManager {
	void registerApplicationId(ApplicationId appId) throws RMStateException;
}
