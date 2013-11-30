package org.apache.hadoop;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

public class FakeNMContainerManager extends AbstractService implements
        ContainerManagementProtocol {
    private final InetSocketAddress serviceAddress;
    private final MockNM nm;
    private final Timer timer;
    private Server server;
    private Map<ApplicationId, List<ContainerStatus>> containers = new ConcurrentHashMap<ApplicationId, List<ContainerStatus>>();
    private static final Log LOG = LogFactory.getLog(FakeClient.class);
    private Map<ApplicationId, FakeAM> runningAMs = new HashMap<ApplicationId, FakeAM>();

    public FakeNMContainerManager(InetSocketAddress serviceAddress, MockNM nm) {
        super("ContainerManger:" + serviceAddress.toString());
        this.serviceAddress = serviceAddress;
        this.nm = nm;
        this.timer = new Timer();
    }

    @Override
    protected void serviceStart() throws Exception {
        Configuration conf = getConfig();
        Configuration serverConf = new Configuration(conf);
        YarnRPC rpc = YarnRPC.create(conf);

        server = rpc.getServer(ContainerManagementProtocol.class, this,
                serviceAddress, serverConf, null, 1);
        server.start();
        super.serviceStart();
        this.nm.registerNode();
        // Send periodic heart beats.
        this.timer.schedule(new FakeNMHeartBeatTask(nm, containers), 0, conf
                .getLong(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 60000));
    }

    private ContainerStatus newContainerStatus(ContainerId containerId,
            ContainerState containerState, String diagnostics) {
        ContainerStatus containerStatus = Records
                .newRecord(ContainerStatus.class);
        containerStatus.setState(containerState);
        containerStatus.setContainerId(containerId);
        containerStatus.setDiagnostics(diagnostics);
        containerStatus.setExitStatus(ContainerExitStatus.INVALID);
        return containerStatus;
    }

    @Override
    public StartContainersResponse startContainers(
            StartContainersRequest requests) throws YarnException, IOException {
        List<ContainerId> succeededContainers = new ArrayList<ContainerId>();
        Map<ContainerId, SerializedException> failedContainers = new HashMap<ContainerId, SerializedException>();
        for (StartContainerRequest request : requests
                .getStartContainerRequests()) {
            final ContainerTokenIdentifier containerTokenIdentifier = BuilderUtils
                    .newContainerTokenIdentifier(request.getContainerToken());
            final ContainerId containerId = containerTokenIdentifier
                    .getContainerID();
            final ApplicationId applicationId = containerId
                    .getApplicationAttemptId().getApplicationId();
            succeededContainers.add(containerId);
            ContainerStatus status = this.newContainerStatus(containerId,
                    ContainerState.RUNNING, "");
            ArrayList<ContainerStatus> appContainers = new ArrayList<ContainerStatus>();
            appContainers.add(status);
            containers.put(applicationId, appContainers);

            Credentials creds = new Credentials();
            ByteBuffer tokens = request.getContainerLaunchContext().getTokens();
            if (tokens != null) {
                DataInputByteBuffer buf = new DataInputByteBuffer();
                tokens.rewind();
                buf.reset(tokens);
                try {
                    creds.readTokenStorageStream(buf);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            final FakeAM am = new FakeAM(this, containerId,
                    containerId.getApplicationAttemptId(), creds,
                    serviceAddress.getHostName(), getConfig());
            synchronized (runningAMs) {
                runningAMs.put(containerId.getApplicationAttemptId().getApplicationId(), am);
            }
            this.timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    LOG.info("Starting: "
                            + containerId.getApplicationAttemptId());
                    try {
                        am.runApplication();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }, ParametersForFakeYarn.APPLICATION_START_DELAY_SECONDS * 1000);
        }
        Map<String, ByteBuffer> servicesMetaData = new HashMap<String, ByteBuffer>();
        return StartContainersResponse.newInstance(servicesMetaData,
                succeededContainers, failedContainers);
    }

    @Override
    public StopContainersResponse stopContainers(StopContainersRequest request)
            throws YarnException, IOException {
        LOG.info("Handling stop container request: " + request.toString());
        
        List<ContainerId> succeededRequests = new ArrayList<ContainerId>();
        Map<ContainerId, SerializedException> failedRequests = new HashMap<ContainerId, SerializedException>();
        for (ContainerId containerId : request.getContainerIds()) {
            if (!containers.containsKey(containerId.getApplicationAttemptId().getApplicationId())) {
                LOG.error("Unknown container.");
                failedRequests.put(containerId, SerializedException
                        .newInstance(new Exception("Container not found")));
            } else {
                List<ContainerStatus> statuses = containers.get(containerId
                        .getApplicationAttemptId().getApplicationId());
                if (statuses.size() != 1
                        || statuses.get(0).getContainerId().compareTo(containerId) != 0
                        || statuses.get(0).getState() != ContainerState.COMPLETE) {
                    LOG.error("Weird container. Num containers: " + statuses.size() + " ContainerId:" + containerId);
                    if (statuses.size() >= 1) {
                        LOG.error("ContainerId:" + statuses.get(0).getContainerId() + " state:" + statuses.get(0).getState());
                    }
                    failedRequests.put(containerId, SerializedException
                            .newInstance(new Exception("Container not found")));

                } else {
                    succeededRequests.add(containerId);
                    containers.remove(containerId.getApplicationAttemptId()
                            .getApplicationId());
                }
            }
        }
        return StopContainersResponse.newInstance(succeededRequests,
                failedRequests);
    }

    @Override
    public GetContainerStatusesResponse getContainerStatuses(
            GetContainerStatusesRequest request) throws YarnException,
            IOException {
        LOG.info("Handling getContainerStatus");
        List<ContainerStatus> statuses = new ArrayList<ContainerStatus>();
        for (ApplicationId appId : containers.keySet()) {
            statuses.addAll(containers.get(appId));
        }
        Map<ContainerId, SerializedException> failedRequests = new HashMap<ContainerId, SerializedException>();
        return GetContainerStatusesResponse.newInstance(statuses,
                failedRequests);
    }

    public void MarkAMFinished(ApplicationId appId, ContainerId containerId) {
        synchronized (runningAMs) {
         runningAMs.remove(appId);   
        }
        ContainerStatus status = this.newContainerStatus(containerId,
                ContainerState.COMPLETE, "");
        ArrayList<ContainerStatus> appContainers = new ArrayList<ContainerStatus>();
        appContainers.add(status);
        containers.put(appId, appContainers);
    }

    public void Kill() {
        // stop listenting for CMP.
        this.stop();
        // Cancel heartbeats.
        this.timer.cancel();
        // stop all the AMs.
        synchronized (runningAMs) {
         for (FakeAM am : runningAMs.values())
             am.Kill();
        }
    }
}
