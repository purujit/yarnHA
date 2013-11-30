package org.apache.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.util.Records;

public class FakeClient {
    private final Configuration conf;
    private static final Log LOG = LogFactory.getLog(FakeClient.class);

    public FakeClient(Configuration conf) {
        this.conf = conf;
    }

    public void submitApplication(String appName) throws IOException,
            YarnException {
        ApplicationClientProtocol applicationsManager = ClientRMProxy
                .createRMProxy(conf, ApplicationClientProtocol.class);
        GetNewApplicationRequest request = Records
                .newRecord(GetNewApplicationRequest.class);
        GetNewApplicationResponse response = applicationsManager
                .getNewApplication(request);
        ApplicationSubmissionContext appContext = Records
                .newRecord(ApplicationSubmissionContext.class);
        appContext.setApplicationId(response.getApplicationId());
        appContext.setApplicationName(appName);
        appContext.setMaxAppAttempts(1);
        appContext.setApplicationType("fake");
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(ParametersForFakeYarn.NODE_MEMORY_GB << 10);
        appContext.setResource(capability);

        ContainerLaunchContext amContainer = Records
                .newRecord(ContainerLaunchContext.class);
        List<String> commands = new ArrayList<String>();
        commands.add("dummy AM command");
        amContainer.setCommands(commands);

        appContext.setAMContainerSpec(amContainer);

        // Create the request to send to the ApplicationsManager
        SubmitApplicationRequest appRequest = Records
                .newRecord(SubmitApplicationRequest.class);
        appRequest.setApplicationSubmissionContext(appContext);

        // Submit the application to the ApplicationsManager
        // Ignore the response as either a valid response object is returned on
        // success or an exception thrown to denote the failure.
        applicationsManager.submitApplication(appRequest);
    }
}
