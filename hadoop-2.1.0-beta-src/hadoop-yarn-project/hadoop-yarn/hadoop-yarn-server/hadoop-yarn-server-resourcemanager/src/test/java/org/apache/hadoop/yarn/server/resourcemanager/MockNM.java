/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

public class MockNM {

  private int responseId;
  private NodeId nodeId;
  private final int memory;
  private final int vCores = 1;
  private ResourceTracker resourceTracker;
  private final int httpPort = 2;
  private MasterKey currentContainerTokenMasterKey;
  private MasterKey currentNMTokenMasterKey;

  /* nodeIdStr - in host:port form */
  public MockNM(String nodeIdStr, int memory, ResourceTracker resourceTracker) {
    this.memory = memory;
    this.resourceTracker = resourceTracker;
    String[] splits = nodeIdStr.split(":");
    nodeId = BuilderUtils.newNodeId(splits[0], Integer.parseInt(splits[1]));
  }

  public NodeId getNodeId() {
    return nodeId;
  }

  public int getHttpPort() {
    return httpPort;
  }
  
  void setResourceTrackerService(ResourceTracker resourceTracker) {
    this.resourceTracker = resourceTracker;
  }

  public void containerStatus(ContainerStatus containerStatus) throws Exception {
    Map<ApplicationId, List<ContainerStatus>> conts = 
        new HashMap<ApplicationId, List<ContainerStatus>>();
    conts.put(containerStatus.getContainerId().getApplicationAttemptId().getApplicationId(),
        Arrays.asList(new ContainerStatus[] { containerStatus }));
    nodeHeartbeat(conts, true);
  }

  public RegisterNodeManagerResponse registerNode() throws Exception {
    RegisterNodeManagerRequest req = Records.newRecord(
        RegisterNodeManagerRequest.class);
    req.setNodeId(nodeId);
    req.setHttpPort(httpPort);
    Resource resource = BuilderUtils.newResource(memory, vCores);
    req.setResource(resource);
    RegisterNodeManagerResponse registrationResponse =
        resourceTracker.registerNodeManager(req);
    this.currentContainerTokenMasterKey =
        registrationResponse.getContainerTokenMasterKey();
    this.currentNMTokenMasterKey = registrationResponse.getNMTokenMasterKey();
    return registrationResponse;
  }

  public NodeHeartbeatResponse nodeHeartbeat(boolean isHealthy) throws Exception {
    return nodeHeartbeat(new HashMap<ApplicationId, List<ContainerStatus>>(),
        isHealthy, ++responseId);
  }

  // Used when completing an application.
  public NodeHeartbeatResponse nodeHeartbeat(ApplicationAttemptId attemptId,
      int containerId, ContainerState containerState) throws Exception {
    HashMap<ApplicationId, List<ContainerStatus>> nodeUpdate =
        new HashMap<ApplicationId, List<ContainerStatus>>(1);
    ContainerStatus amContainerStatus = BuilderUtils.newContainerStatus(
        BuilderUtils.newContainerId(attemptId, 1),
        ContainerState.COMPLETE, "Success", 0);
    ArrayList<ContainerStatus> containerStatusList =
        new ArrayList<ContainerStatus>(1);
    containerStatusList.add(amContainerStatus);
    nodeUpdate.put(attemptId.getApplicationId(), containerStatusList);
    return nodeHeartbeat(nodeUpdate, true);
  }

  public NodeHeartbeatResponse nodeHeartbeat(Map<ApplicationId,
      List<ContainerStatus>> conts, boolean isHealthy) throws Exception {
    return nodeHeartbeat(conts, isHealthy, ++responseId);
  }

  public NodeHeartbeatResponse nodeHeartbeat(Map<ApplicationId,
      List<ContainerStatus>> conts, boolean isHealthy, int resId) throws Exception {
    NodeHeartbeatRequest req = Records.newRecord(NodeHeartbeatRequest.class);
    NodeStatus status = Records.newRecord(NodeStatus.class);
    status.setResponseId(resId);
    status.setNodeId(nodeId);
    for (Map.Entry<ApplicationId, List<ContainerStatus>> entry : conts.entrySet()) {
      status.setContainersStatuses(entry.getValue());
    }
    NodeHealthStatus healthStatus = Records.newRecord(NodeHealthStatus.class);
    healthStatus.setHealthReport("");
    healthStatus.setIsNodeHealthy(isHealthy);
    healthStatus.setLastHealthReportTime(Time.now());
    status.setNodeHealthStatus(healthStatus);
    req.setNodeStatus(status);
    req.setLastKnownContainerTokenMasterKey(this.currentContainerTokenMasterKey);
    req.setLastKnownNMTokenMasterKey(this.currentNMTokenMasterKey);
    NodeHeartbeatResponse heartbeatResponse =
        resourceTracker.nodeHeartbeat(req);
    
    MasterKey masterKeyFromRM = heartbeatResponse.getContainerTokenMasterKey();
    if (masterKeyFromRM != null
        && masterKeyFromRM.getKeyId() != this.currentContainerTokenMasterKey
            .getKeyId()) {
      this.currentContainerTokenMasterKey = masterKeyFromRM;
    }

    masterKeyFromRM = heartbeatResponse.getNMTokenMasterKey();
    if (masterKeyFromRM != null
        && masterKeyFromRM.getKeyId() != this.currentNMTokenMasterKey
            .getKeyId()) {
      this.currentNMTokenMasterKey = masterKeyFromRM;
    }
    
    return heartbeatResponse;
  }

}
