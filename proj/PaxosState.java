import java.util.*;

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
	public List<Integer> promised;
	public List<Integer> accepted;
	public int highestAcceptedNum;
	public byte[] highestAcceptedValue;
	
	//Acceptor State
	public int promisedPropNum;
	public int acceptedPropNum;
	public int highestExecutedInstNum;
	public byte[] acceptedValue;
	
	public PaxosState(int instNum, byte[] value, boolean executed) {
		this(instNum, -1, value);
		this.decided = true;
		this.executed = executed;
	}
	
	public PaxosState(int instNum, int propNum, byte[] value) {
		this(instNum, propNum, value, null);
	}
	
	public PaxosState(int instNum, int propNum, byte[] value, List<Integer> participants) {
		this.executed = false;
		this.promised = new ArrayList<Integer>();
		this.accepted = new ArrayList<Integer>();
		this.instNum = instNum;
		this.propNum = propNum;
		this.value = value;
		this.highestAcceptedNum = propNum;
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
	
	public boolean quorumAccepted() {
		return accepted.size() > participants.size() / 2;
	}
	
	public String toLogString() {
		return this.instNum + LOG_SEPERATOR + Utility.byteArrayToString(this.value) + LOG_SEPERATOR + (this.decided ? EXECUTED_TRUE_STRING : EXECUTED_FALSE_STRING);
	}
	
	public static PaxosState fromLogString(String s) {
		String[] parts = s.split(LOG_SEPERATOR, 3);
		return new PaxosState(Integer.parseInt(parts[0]), Utility.stringToByteArray(parts[1]), (parts[2].equals(EXECUTED_TRUE_STRING)));
	}
}