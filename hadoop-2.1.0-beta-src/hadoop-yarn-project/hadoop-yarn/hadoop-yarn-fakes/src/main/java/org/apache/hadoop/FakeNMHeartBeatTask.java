package org.apache.hadoop;

import java.util.List;
import java.util.Map;
import java.util.TimerTask;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;

class FakeNMHeartBeatTask extends TimerTask {
    private final MockNM nm;
    private Map<ApplicationId, List<ContainerStatus>> containers;
    public FakeNMHeartBeatTask(MockNM nm,
            Map<ApplicationId, List<ContainerStatus>> containers) {
        this.nm = nm;
        this.containers = containers;
    }

    public void run() {
        try {
            NodeHeartbeatResponse resp = nm.nodeHeartbeat(containers, true);
            if (resp.getNodeAction() != NodeAction.NORMAL) {
                // TODO(purujit): For now, dump out the responses. But
                // eventually, parse and interpret them.
                System.out.println(resp.toString());
                throw new Exception(resp.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}