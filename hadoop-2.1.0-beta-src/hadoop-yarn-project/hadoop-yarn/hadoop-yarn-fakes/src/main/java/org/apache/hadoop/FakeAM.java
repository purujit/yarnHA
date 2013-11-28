package org.apache.hadoop;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.Records;
import org.mortbay.log.Log;

public class FakeAM {
    private volatile int responseId = 0;
    private final ApplicationAttemptId appAttemptId;
    private final Configuration conf;
    private final String hostName;
    private Timer timer = new Timer();
    private final Credentials creds;
    private final FakeNMContainerManager containerManager;
    private final ContainerId containerId;

    public FakeAM(FakeNMContainerManager containerManager,
            ContainerId containerId, ApplicationAttemptId appAttemptId,
            Credentials creds, String hostName, Configuration conf) {
        super();
        this.appAttemptId = appAttemptId;
        this.conf = conf;
        this.hostName = hostName;
        this.creds = creds;
        this.containerManager = containerManager;
        this.containerId = containerId;
    }

    public void runApplication() throws IOException, InterruptedException {
        final UserGroupInformation ugi = UserGroupInformation
                .createRemoteUser(appAttemptId.toString());
        for (Token<? extends TokenIdentifier> tk : creds.getAllTokens()) {
            ugi.addToken(tk);
        }
        // Create the channel to talk to the RM.
        final ApplicationMasterProtocol rmAppMasterService = ugi
                .doAs(new PrivilegedExceptionAction<ApplicationMasterProtocol>() {
                    @Override
                    public ApplicationMasterProtocol run() throws Exception {
                        return ClientRMProxy.createRMProxy(
                                new YarnConfiguration(conf),
                                ApplicationMasterProtocol.class);
                    }
                });
        // Register the AM.
        final RegisterApplicationMasterRequest req = Records
                .newRecord(RegisterApplicationMasterRequest.class);
        req.setHost(hostName);
        req.setRpcPort(1);
        req.setTrackingUrl("");
        ugi.doAs(new PrivilegedExceptionAction<RegisterApplicationMasterResponse>() {
            @Override
            public RegisterApplicationMasterResponse run() throws Exception {
                return rmAppMasterService.registerApplicationMaster(req);
            }
        });
        Random rnd = new Random();
        // Assume, app runs for 3 minutes.
        final long lifetimeAvg = ParametersForFakeYarn.AVERAGE_APPLICATION_DURATION_SECONDS * 1000;
        final long lifetime = (long) Math.max(30, Math.ceil(rnd.nextGaussian() * lifetimeAvg/5.0 + lifetimeAvg));
        final long startTime = Time.now();
        this.timer.schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    if (Time.now() - startTime < lifetime) {
                        // send a heartbeat.
                        List<ResourceRequest> resourceRequest = new ArrayList<ResourceRequest>();
                        List<ContainerId> releases = new ArrayList<ContainerId>();
                        float progress = (float) Math.min(1.0,
                                (Time.now() - startTime) / (float) lifetime);
                        final AllocateRequest allocReq = AllocateRequest
                                .newInstance(++responseId, progress,
                                        resourceRequest, releases, null);
                        ugi.doAs(new PrivilegedExceptionAction<AllocateResponse>() {
                            @Override
                            public AllocateResponse run() throws Exception {
                                return rmAppMasterService.allocate(allocReq);
                            }
                        });
                    } else {
                        this.cancel();
                        // send termination.
                        final FinishApplicationMasterRequest finishRequest = Records
                                .newRecord(FinishApplicationMasterRequest.class);
                        finishRequest
                                .setFinalApplicationStatus(FinalApplicationStatus.SUCCEEDED);
                        ugi.doAs(new PrivilegedExceptionAction<FinishApplicationMasterResponse>() {
                            @Override
                            public FinishApplicationMasterResponse run()
                                    throws Exception {
                                return rmAppMasterService
                                        .finishApplicationMaster(finishRequest);
                            }
                        });
                        containerManager.MarkAMFinished(
                                appAttemptId.getApplicationId(), containerId);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 10000, ParametersForFakeYarn.AM_HEARTBEAT_INTERVAL_SECONDS * 1000);
    }
}
