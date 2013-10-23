package org.apache.hadoop.yarn.server.resourcemanager.state;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.stat.descriptive.rank.Max;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;

import com.google.common.collect.ConcurrentHashMultiset;

public class RMMultiReplicaStateManager implements RMStateManager {

	private static final Log LOG = LogFactory.getLog(RMStateManager.class);
	// TODO(purujit) : It needs to be initialized from the configuration with
	// the initial set of replicas. Or alternatively, it should be hidden behind
	// a replica manager abstraction.
	private final ConcurrentLinkedDeque<InetAddress> liveReplicas = new ConcurrentLinkedDeque<InetAddress>();
	
	// EPaxos instance number.
	private final AtomicInteger instanceNumber = new AtomicInteger(0);
	
	private static class EPaxosCommand {
	    public InetAddress replica;
	    public int instanceNumber;
	    public int sequenceNumber;
	    public int paxosState;
	    public ApplicationId appId;
	    public int newState;
	    // public EPaxosCommand(InetAddress replica, int instanceNumber, int)
	}
	
	private final ConcurrentMap<ApplicationId, ConcurrentLinkedQueue<EPaxosCommand>> cmds = new ConcurrentHashMap<ApplicationId, ConcurrentLinkedQueue<EPaxosCommand>>();

	@Override
	public void registerApplicationId(ApplicationId appId)
			throws RMStateException {
		// NOTE: We don't really need to do anything here. If a replica
		// dies after registering an ID, the client will get an error when it
		// tries to submit the application. But no major harm will be done and
		// the client can retry.
	}

	@Override
	public void registerApplicationSubmission(
			ApplicationSubmissionContext submissionContext)
			throws RMStateException {
		/**
		 * 1. Establish ordering constraints -
		 *    a) increment instance number
		 *    b) compute the interfering commands set, I - Set of (leader replica, instance number, sequence number, command)
		 *    c) compute sequence number
		 *    d) add this command to the cmds log as 'pre-accepted'.
		 */
	    final int thisInstanceNumber = instanceNumber.getAndIncrement();
	    EPaxosCommand[] interferingCommands = 
	            (EPaxosCommand[])cmds.get(submissionContext.getApplicationId()).toArray();
	    int sequenceNumber = 0;
	    for (EPaxosCommand cmd : interferingCommands) {
	        sequenceNumber = Math.max(sequenceNumber, cmd.sequenceNumber);
	    }
	    
	    
		 /** 
		  * 2. Send Pre-Accept to a fast-quorum using RMReplicationService and wait for responses.
		 * 3. Upon receiving at least floor(N/2) responses (we need to wait for fast-quorum) -
		 *    a) if received Pre-Accept OK from all fast-quorum replicas with unchanged deps and sequence number then 
		 *       run Commit for this instance.
		 *    b) else update deps and sequence number and then run Paxos-Accept phase.
		 * 4. Commit -
		 *    a) Add this command to the cmds log as 'committed'.
		 *    b) Asynchronously send commit notification to all the replicas.
		 *    c) Return.
		 * 5. Paxos-Accept
		 *    a) Add this command to the cmds log as 'accepted'.
		 *    b) Send 'accept' to at least floor(N/2) other replicas.
		 *    c) After receiving at least, floor(N/2) responses run Commit.
		 */
	}
}
