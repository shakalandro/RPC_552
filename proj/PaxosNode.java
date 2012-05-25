import java.lang.reflect.Method;
import java.util.*;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

/**
 * Implements the login for paxos.
 * 
 * @author Roy McElmurry (roy.miv@gmail.com)
 */
public abstract class PaxosNode extends RPCNode {
	private static final int STARTING_BACKOFF = 30;
    private static final int RANDOM_BACKOFF_MAX = 20;
	private static final Random r = new Random();
	// State data shared by all roles.
	private Map<Integer, PaxosState> rounds;
	
	/**
	 * Clients wishing to replicate some command must call this function. In time either the
	 * handlePaxosCommand() or retryPaxosCommand() methods will be called. The former is in response
	 * to the command being learned by this node. The latter is in response to a failure in
	 * replicating the command, which can happen if a value other than the proposed one was chosen
	 * for this round.
	 * @param addrs Replicas to talk to.
	 * @param payload The command to replicate.
	 */
	public void replicateCommand(List<Integer> addrs, byte[] payload) {
		int instNum = getInstNum();
		rounds.put(instNum, new PaxosState(instNum, getNextPropNum(0), payload, addrs));
		proposeCommand(addrs, instNum, payload);
	}
	
    /******************************** Proposer Code ********************************/
    
	// Returns the next unused paxos instance number.
	private int getInstNum() {
		if (this.rounds.isEmpty()) {
			return 0;
		}
		Set<Integer> knownInstances = this.rounds.keySet();
    	return Collections.max(knownInstances) + 1;
    }
	
	// Returns a proposal number that is at least twice as large as the given proposal number
	// and is within a set of numbers unique to this node.
	private int getNextPropNum(int last) {
		return (2 * this.addr * ((last / this.addr) + 1));
	}
	
	private void proposeCommand(List<Integer> addrs, int instNum, byte[] payload) {
		proposeCommand(addrs, instNum, payload, STARTING_BACKOFF);
	}
	
	private void proposeCommand(List<Integer> addrs, int instNum, byte[] payload, int backoff) {
		int propNum = this.rounds.get(instNum).propNum;
		for (Integer nodeAddr : addrs) {
			PaxosPacket prepare = PaxosPacket.makePrepareMessage(instNum, propNum, payload);
			RIOSend(nodeAddr, Protocol.PAXOS_PKT, prepare.pack());
		}
		try {
			Method m = Callback.getMethod("proposeCommand", this, new String[] {List.class.getName(),
					Integer.class.getName(), byte[].class.getName(), Integer.class.getName()});
			Callback retry = new Callback(m, this,
					new Object[] {addrs, instNum, payload,
							backoff * 2 + r.nextInt() % RANDOM_BACKOFF_MAX});
			addTimeout(retry, backoff);
		} catch (Exception e) {
			e.printStackTrace();
			fail();
		}
	}
	
	private void handlePromiseResponse(int from, int instNum, int highestAccept, byte[] payload) {
		PaxosState state = this.rounds.get(instNum);
		state.promised.add(from);
		if (highestAccept > state.highestAcceptedNum) {
			state.setHighest(highestAccept, payload);
		}
		if (state.quorumPromised()) {
			for (Integer nodeAddr : state.promised) {
				PaxosPacket accept = PaxosPacket.makeAcceptMessage(instNum,
						state.propNum, state.highestAcceptedValue);
				RIOSend(nodeAddr, Protocol.PAXOS_PKT, accept.pack());
			}
		}
	}
	
	private void handleAcceptResponse(int from, int instNum, int n, byte[] payload) {
		PaxosState state = this.rounds.get(instNum);
		state.accepted.add(from);
		if (state.quorumAccepted()) {
			for (Integer nodeAddr : state.participants) {
				PaxosPacket decision = PaxosPacket.makeDecisionMessage(instNum, n, payload);
				RIOSend(nodeAddr, Protocol.PAXOS_PKT, decision.pack());
			}
		}
	}
    
    /******************************** Participant Code ********************************/
	
	// Returns true if the participant promises not to accept a higher numbered proposal
	// and false otherwise.
	private void handlePrepareRequest(int from, int instNum, int n, byte[] payload) {
		PaxosState state = this.rounds.get(instNum);
		if (state == null) {
			state = new PaxosState(instNum, n, payload);
			this.rounds.put(instNum, state);
		}
		if (n > state.promisedPropNum) {
			state.promisedPropNum = n;
			PaxosPacket promise = PaxosPacket.makePromiseMessage(
					state.instNum, state.acceptedPropNum, state.acceptedValue);
			RIOSend(from, Protocol.PAXOS_PKT, promise.pack());
		}
	}
	
	// Returns true if the participant accept the proposal and false otherwise.
	// 1) We cannot accept if we promised not to
	// 2) We cannot accept if we already accepted a higher one
	private void handleAcceptRequest(int from, int instNum, int n, byte[] payload) {
		PaxosState state = this.rounds.get(instNum);
		if (n >= state.promisedPropNum) {
			state.acceptedPropNum = n;
			state.acceptedValue = payload;
			PaxosPacket accepted = PaxosPacket.makeAcceptedMessage(instNum, n, payload);
			RIOSend(from, Protocol.PAXOS_PKT, accepted.pack());
		}
	}
	
	/******************************** Learner Code ********************************/
	
	/**
	 * Subclasses must implement this method.
	 * @param instNum The paxos instance number the payload was accepted as.
	 * @param payload Data that you must parse and handle.
	 */
	public abstract void handlePaxosCommand(int instNum, byte[] payload);
	
	// Handle decision message and call client handler if the value was not a NOOP.
	private void handleDecisionMessage(int instNum, byte[] payload) {
		PaxosState state = this.rounds.get(instNum);
		if (state == null) {
			state = new PaxosState(instNum, payload);
		} else {
			state.value = payload;
			state.decided = true;
		}
		if (state.value != null) {
			handlePaxosCommand(state.instNum, state.value);
		}
	}
	
	private void learnCommand(List<Integer> addrs, int instNum) {
		proposeCommand(addrs, instNum, null);
	}
	
    /******************************** Glue Code ********************************/
    
	/**
     * This node has a packet to process
     */
    @Override
    public void onRIOReceive(Integer from, int protocol, byte[] msg) {
        if (protocol == Protocol.PAXOS_PKT) {
            PaxosPacket pkt = PaxosPacket.unpack(msg);
            switch (pkt.msgType) {
            	case PREPARE:
            		handlePrepareRequest(from, pkt.instance, pkt.proposal, pkt.payload);
            		break;
            	case PROMISE:
            		handlePromiseResponse(from, pkt.instance, pkt.proposal, pkt.payload);
            		break;
            	case ACCEPT:
            		handleAcceptRequest(from, pkt.instance, pkt.proposal, pkt.payload);
            		break;
            	case ACCEPTED:
            		handleAcceptResponse(from, pkt.instance, pkt.proposal, pkt.payload);
            		break;
            	case DECISION:
            		handleDecisionMessage(pkt.instance, pkt.payload);
            	default:
            		break;
            }
        } else {
        	super.onRIOReceive(from, protocol, msg);
        }
    }
    
    public PaxosNode() {
		this.rounds = new HashMap<Integer, PaxosState>();
	}
	
	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}
}
