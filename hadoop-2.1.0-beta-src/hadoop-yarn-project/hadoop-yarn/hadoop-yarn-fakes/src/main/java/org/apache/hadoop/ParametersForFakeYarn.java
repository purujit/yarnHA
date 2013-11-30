package org.apache.hadoop;

public class ParametersForFakeYarn {
    public static final int NUMBER_OF_NODES = 100;
    public static final int NM_PORT_BASE = 12000;
    public static final int NODE_MEMORY_GB = 2;
    public static final int AVERAGE_APPLICATION_DURATION_SECONDS = 300;
    public static final int APPLICATION_START_DELAY_SECONDS = 10;
    public static final double TARGET_UTILIZATION = 0.80;
    
    public static final int NM_HEARTBEAT_INTERVAL_SECONDS = 60;
    public static final int SCHEDULING_DELAY_SECONDS = 60;
    public static final int AM_HEARTBEAT_INTERVAL_SECONDS = 30;
    public static final int AVERAGE_NM_FAILURE_PERIOD_SECONDS = 300;
}
