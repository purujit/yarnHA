package org.apache.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.client.ClientRMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

public class FakeClient {
    private final Configuration conf;

    public FakeClient(Configuration conf) {
        this.conf = conf;
    }

    public void submitApplication() throws IOException, YarnException {
        ApplicationClientProtocol applicationsManager = ClientRMProxy
                .createRMProxy(conf, ApplicationClientProtocol.class);
        GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
        GetNewApplicationResponse response = applicationsManager.getNewApplication(request);
        System.out.println(response.getApplicationId());
        
        // TODO(purujit): Use this application id to submit an application with a fake AM.
    }
}
