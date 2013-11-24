package org.apache.hadoop;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.security.authorize.NMPolicyProvider;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;

public class FakeNMContainerManager extends AbstractService implements
        ContainerManagementProtocol {
    private final InetSocketAddress serviceAddress;
    private final MockNM nm;
    private Server server;

    public FakeNMContainerManager(InetSocketAddress serviceAddress, MockNM nm) {
        super("ContainerManger:" + serviceAddress.toString());
        this.serviceAddress = serviceAddress;
        this.nm = nm;
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

    }

    @Override
    public StartContainersResponse startContainers(
            StartContainersRequest request) throws YarnException, IOException {
        System.out.println(request.toString());
        // TODO Auto-generated method stub
        return null;
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
        System.out.println(request.toString());
        // TODO Auto-generated method stub
        return null;
    }

}
