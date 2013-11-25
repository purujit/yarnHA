package org.apache.hadoop;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.Records;

public class FakeAM {
    private volatile int responseId = 0;
    private final ContainerTokenIdentifier containerTokenId;
    private ApplicationMasterProtocol amRMProtocol;
    private final String hostName;
    private Timer timer = new Timer();

    public FakeAM(ContainerTokenIdentifier containerTokenId, String hostName,
            ApplicationMasterProtocol amRMProtocol) {
        super();
        this.containerTokenId = containerTokenId;
        this.amRMProtocol = amRMProtocol;
        this.hostName = hostName;
    }

    public RegisterApplicationMasterResponse registerAppAttempt()
            throws Exception {
        responseId = 0;
        final RegisterApplicationMasterRequest req = Records
                .newRecord(RegisterApplicationMasterRequest.class);
        req.setHost(hostName);
        req.setRpcPort(1);
        req.setTrackingUrl("");
        RegisterApplicationMasterResponse response = amRMProtocol.registerApplicationMaster(req);
//        RegisterApplicationMasterResponse response = containerTokenId.getUser()
//                .doAs(new PrivilegedExceptionAction<RegisterApplicationMasterResponse>() {
//                    @Override
//                    public RegisterApplicationMasterResponse run() throws Exception {
//                      return amRMProtocol.registerApplicationMaster(req);
//                    }
//                  });
        // Set up heart beats.
        this.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                List<ResourceRequest> resourceRequest = new ArrayList<ResourceRequest>();
                List<ContainerId> releases = new ArrayList<ContainerId>();
                final AllocateRequest allocReq =
                        AllocateRequest.newInstance(++responseId, 0F, resourceRequest,
                          releases, null);
                try {
                    amRMProtocol.allocate(allocReq);
//                    containerTokenId.getUser()
//                    .doAs(new PrivilegedExceptionAction<Object>() {
//                        @Override
//                        public Object run() throws Exception {
//                          return amRMProtocol.allocate(allocReq);
//                        }
//                      });
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (YarnException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }, 10000, 60000);
        return response;
    }
}
