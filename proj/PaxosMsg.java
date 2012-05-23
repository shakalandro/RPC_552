/**
 * Defines the possible message types exchanged during a round of Paxos.
 * 
 * @author jennyabrahamson
 */
public enum PaxosMsg {
	PREPARE, PROMISE, ACCEPT, ACCEPTED;
	
    public static PaxosMsg getMessage(int ordinal) {
        return PaxosMsg.values()[ordinal];
    }
}
