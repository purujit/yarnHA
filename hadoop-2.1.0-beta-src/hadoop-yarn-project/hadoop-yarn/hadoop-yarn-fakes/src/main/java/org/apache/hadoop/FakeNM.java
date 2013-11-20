package org.apache.hadoop;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Timer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.PeriodicStatsAccumulator;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;

/**
 * A NM implementation that does no real work but fakes working.
 * 
 */
public class FakeNM {
    public static void main(String[] args) {
        try {
            Configuration conf = new YarnConfiguration();
            System.out
                    .println(conf
                            .getInt(YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
                                    YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB));
            Path yarnSitePath = new Path(
                    "/usr/local/hadoop/etc/hadoop/yarn-site.xml");

            conf.addResource(yarnSitePath);
            ResourceTracker resourceTracker;

            resourceTracker = ServerRMProxy.createRMProxy(conf,
                    ResourceTracker.class);

            MockNM nm = new MockNM("localhost:12000", 2 << 10, resourceTracker);
            // Register with RM.
            System.out.println(nm.registerNode().toString());
            // Send periodic heartbeats.
            Timer timer = new Timer();
            timer.schedule(new FakeNMHeartBeatTask(nm), 0, conf.getLong(
                    YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 60000));
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        // Also, with a specified random distribution fail.
        // We also have to bring up a fake container manager.
    }
}
