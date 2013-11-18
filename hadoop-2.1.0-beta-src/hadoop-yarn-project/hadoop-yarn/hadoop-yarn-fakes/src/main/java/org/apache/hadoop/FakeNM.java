package org.apache.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.ServerRMProxy;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
// import org.apache.hadoop.yarn.server.resourcemanager.MockNM;

/**
 * A NM implementation that does no real work but fakes working.
 * 
 */
public class FakeNM {
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        Configuration conf = new YarnConfiguration();
        ResourceTracker resourceTracker;
        try {
            resourceTracker = ServerRMProxy.createRMProxy(conf, ResourceTracker.class);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(conf.toString());
            return;
        }
        MockNM nm = new MockNM("Node:1", 2 << 30, resourceTracker);
    }
}
