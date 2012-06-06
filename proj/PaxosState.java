import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.washington.cs.cse490h.lib.Utility;

public class PaxosState {

    public static final String LOG_SEPERATOR = ",";
    public static final String EXECUTED_TRUE_STRING = "EXEC";
    public static final String EXECUTED_FALSE_STRING = "NO_EXEC";
	
	//General State
	public int instNum;
	public int propNum;
	public boolean decided;
	public boolean executed;
	
	//Proposer State
	public byte[] value;
	public List<Integer> participants;
	public Set<Integer> promised;
	public Set<Integer> accepted;
	public int highestAcceptedNum;
	public byte[] highestAcceptedValue;
	public boolean decisionsSent;
	public boolean acceptRequestsSent;
	
	//Acceptor State
	public int promisedPropNum;
	public int acceptedPropNum;
	public int highestExecutedInstNum;
	public byte[] acceptedValue;
	
	//Learner State
	public byte[] decidedValue;
	
	public PaxosState(int instNum, byte[] value, boolean executed, List<Integer> participants) {
		this(instNum, -1, value, participants);
		this.decided = false;
		this.executed = executed;
	}
	
	public PaxosState(int instNum, int propNum, byte[] value, List<Integer> participants) {
		this.executed = false;
		this.promised = new HashSet<Integer>();
		this.accepted = new HashSet<Integer>();
		this.instNum = instNum;
		this.propNum = propNum;
		this.value = value;
		this.highestAcceptedNum = -1;
		this.highestAcceptedValue = value;
		this.promisedPropNum = -1;
		this.acceptedPropNum = -1;
		this.acceptedValue = null;
		this.participants = participants;
	}
	
	public void setHighest(int n, byte[] v) {
		if (n < this.highestAcceptedNum) {
			throw new IllegalArgumentException("Tried to set highest prop number to something lower.");
		}
		this.highestAcceptedNum = n;
		this.highestAcceptedValue = v;
	}
	
	public boolean quorumPromised() {
		return promised.size() > participants.size() / 2;
	}
	
	public int numPromised() {
		return promised.size();
	}
	
	public boolean quorumAccepted() {
		return accepted.size() > participants.size() / 2;
	}
	
	public int numAccepted() {
		return accepted.size();
	}
	
	public String toLogString() {
		return this.instNum + LOG_SEPERATOR + Utility.byteArrayToString(this.decidedValue) + LOG_SEPERATOR + (this.executed ? EXECUTED_TRUE_STRING : EXECUTED_FALSE_STRING);
	}
	
	public String toStateLogString() {
		return this.instNum + LOG_SEPERATOR + this.promisedPropNum + LOG_SEPERATOR + 
				this.acceptedPropNum + LOG_SEPERATOR + (this.acceptedValue == null ? "null" : Utility.byteArrayToString(this.acceptedValue));
	}
	
	public static int getInstNumFromStateLog(String s) {
		String[] parts = s.split(LOG_SEPERATOR, 4);
		return Integer.parseInt(parts[0]);
	}
	
	public void updateFromStateLogString(String s) {
		String[] parts = s.split(LOG_SEPERATOR, 4);
		this.promisedPropNum = Integer.parseInt(parts[1]);
		this.acceptedPropNum = Integer.parseInt(parts[2]);
	}
	
	public static PaxosState getNewPaxosStateFromStateLog(String s, List<Integer> participants) {
		// Read data from state log 
		String[] parts = s.split(LOG_SEPERATOR, 4);
		int theInstance = Integer.parseInt(parts[0]);
		int thePromisedPropNum = Integer.parseInt(parts[1]);
		int theAcceptedPropNum = Integer.parseInt(parts[2]);
		byte [] theAcceptedValue = parts[3].equals("null") ? PaxosNode.noopMarker : Utility.stringToByteArray(parts[3]);
		PaxosState state = new PaxosState(theInstance, thePromisedPropNum, theAcceptedValue, participants);
		
		// Update state
		state.promisedPropNum = thePromisedPropNum;
		state.acceptedPropNum = theAcceptedPropNum;
		state.acceptedValue = theAcceptedValue;
		
		return state;
	}
	
	// for debugging
	public String toString() {
		return "(" + this.instNum + ") prop:" + this.propNum + ", promise:" + this.promisedPropNum +
				", accepted:" + this.acceptedPropNum + ", payload:" + Utility.byteArrayToString(this.value); 
	}
	
	public static PaxosState fromLogString(String s, List<Integer> participants) {
		String[] parts = s.split(LOG_SEPERATOR, 3);
		boolean executed = parts[2].trim().equals(EXECUTED_TRUE_STRING);
		PaxosState state = new PaxosState(Integer.parseInt(parts[0]), Utility.stringToByteArray(parts[1]), executed, participants);
		state.decidedValue = state.value;
		state.decided = true;
		state.acceptedValue = state.value;
		
		return state;
	}
}
