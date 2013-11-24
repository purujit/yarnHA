package org.apache.hadoop;

import java.util.TimerTask;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;

class FakeNMHeartBeatTask extends TimerTask {
    private final MockNM nm;

    public FakeNMHeartBeatTask(MockNM nm) {
        this.nm = nm;
    }

    public void run() {
        try {
            NodeHeartbeatResponse resp = nm.nodeHeartbeat(true);
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