package org.apache.hadoop;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.PeriodicStatsAccumulator;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;

/**
 * A YARN implementation that does no real work but fakes working against a real
 * RM.
 * 
 */
public class FakeYarn {
    public static void main(String[] args) {
        try {
            final Configuration conf = new YarnConfiguration();
            Path yarnSitePath = new Path(
                    "/usr/local/hadoop/etc/hadoop/yarn-site.xml");
            conf.addResource(yarnSitePath);
            final Random rnd = new Random();
            final ResourceTracker resourceTracker;
            resourceTracker = ServerRMProxy.createRMProxy(conf,
                    ResourceTracker.class);

            // This vector will keep the NMs and the cms alive.
            final Vector<FakeNMContainerManager> cms = new Vector<FakeNMContainerManager>();
            // Start a bunch of NMs along with corresponding ContainerManagers.
            for (int i = 0; i < ParametersForFakeYarn.NUMBER_OF_NODES; ++i) {
                MockNM nm = new MockNM("localhost:"
                        + (ParametersForFakeYarn.NM_PORT_BASE + i),
                        ParametersForFakeYarn.NODE_MEMORY_GB << 10,
                        resourceTracker);
                // Create and start the container manager.
                cms.add(new FakeNMContainerManager(new InetSocketAddress(
                        "localhost", ParametersForFakeYarn.NM_PORT_BASE + i),
                        nm));
                cms.lastElement().init(conf);
                cms.lastElement().start();
            }

            // Set up failures.
            Runnable nmFailureTask = new Runnable() {
                int nextNodeId = cms.size();

                @Override
                public void run() {
                    int nodeToKill = rnd.nextInt(cms.size());
                    cms.get(nodeToKill).Kill();
                    MockNM nm = new MockNM(
                            "localhost:"
                                    + (ParametersForFakeYarn.NM_PORT_BASE + ++nextNodeId),
                            ParametersForFakeYarn.NODE_MEMORY_GB << 10,
                            resourceTracker);
                    // Add a node to replace it.
                    cms.set(nodeToKill, new FakeNMContainerManager(
                            new InetSocketAddress("localhost",
                                    ParametersForFakeYarn.NM_PORT_BASE
                                            + nextNodeId), nm));
                    cms.get(nodeToKill).init(conf);
                    cms.get(nodeToKill).start();
                }
            };

            RandomIntervalTimerTask failureTask = new RandomIntervalTimerTask(
                    nmFailureTask, rnd,
                    ParametersForFakeYarn.AVERAGE_NM_FAILURE_PERIOD_SECONDS);
            failureTask.run();
            
            // Submit a bunch of applications to hit average utilization.
            final int numConcurrentApps = (int) Math
                    .floor(ParametersForFakeYarn.TARGET_UTILIZATION
                            * ParametersForFakeYarn.NUMBER_OF_NODES);
            int avgSubmissionPeriodSec = (ParametersForFakeYarn.AM_HEARTBEAT_INTERVAL_SECONDS
                    / 2
                    + ParametersForFakeYarn.APPLICATION_START_DELAY_SECONDS
                    + ParametersForFakeYarn.AVERAGE_APPLICATION_DURATION_SECONDS
                    + ParametersForFakeYarn.SCHEDULING_DELAY_SECONDS + ParametersForFakeYarn.NM_HEARTBEAT_INTERVAL_SECONDS);

            for (int i = 0; i < numConcurrentApps; ++i) {
                final FakeClient client = new FakeClient(conf);
                Runnable submissionTask = new Runnable() {
                    int appId = 0;

                    @Override
                    public void run() {
                        try {
                            client.submitApplication("Application:"
                                    + client.hashCode() + ":" + ++appId);
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (YarnException e) {
                            e.printStackTrace();
                        }
                    }
                };
                RandomIntervalTimerTask rTask = new RandomIntervalTimerTask(
                        submissionTask, rnd, avgSubmissionPeriodSec);
                rTask.run();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }
}
