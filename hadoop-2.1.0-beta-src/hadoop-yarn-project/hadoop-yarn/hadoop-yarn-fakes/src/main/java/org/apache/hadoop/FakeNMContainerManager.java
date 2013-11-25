package org.apache.hadoop;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
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
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.security.authorize.NMPolicyProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

import com.sun.research.ws.wadl.Application;

public class FakeNMContainerManager extends AbstractService implements
        ContainerManagementProtocol {
    private final InetSocketAddress serviceAddress;
    private final MockNM nm;
    private final Timer timer;
    private Server server;
    private Map<ApplicationId, List<ContainerStatus>> containers = new ConcurrentHashMap<ApplicationId, List<ContainerStatus>>();
    private static final Log LOG = LogFactory.getLog(FakeClient.class);

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
            LOG.info("Starting container for app id" + applicationId);
            ContainerStatus status = this.newContainerStatus(containerId,
                    ContainerState.RUNNING, "");
            ArrayList<ContainerStatus> appContainers = new ArrayList<ContainerStatus>();
            appContainers.add(status);
            containers.put(applicationId, appContainers);
            // In about a minute mark this container as running.
            this.timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    try {
                        AMRMTokenIdentifier tokenId = new AMRMTokenIdentifier(
                                containerTokenIdentifier.getContainerID()
                                        .getApplicationAttemptId());
                        ApplicationMasterProtocol rmAppMasterService = tokenId
                                .getUser()
                                .doAs(new PrivilegedExceptionAction<ApplicationMasterProtocol>() {
                                    @Override
                                    public ApplicationMasterProtocol run()
                                            throws Exception {
                                        return ClientRMProxy
                                                .createRMProxy(
                                                        new YarnConfiguration(
                                                                getConfig()),
                                                        ApplicationMasterProtocol.class);
                                    }
                                });
                        FakeAM am = new FakeAM(containerTokenIdentifier,
                                serviceAddress.getHostName(),
                                rmAppMasterService);
                        am.registerAppAttempt();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }, 10000);
        }
        Map<String, ByteBuffer> servicesMetaData = new HashMap<String, ByteBuffer>();
        return StartContainersResponse.newInstance(servicesMetaData,
                succeededContainers, failedContainers);
    }

    @Override
    public StopContainersResponse stopContainers(StopContainersRequest request)
            throws YarnException, IOException {
        System.out.println(request.toString());
        // TODO Auto-generated method stub
        return null;
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
}
