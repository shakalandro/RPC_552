import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import edu.washington.cs.cse490h.lib.Utility;

class TxnState {
		public static final String COMMITTED = "committed";
		public static final String ABORTED = "aborted";
		public static final String WAITING = "waiting";
		
		public UUID txnID;
		public String request;
		public String args;
		public Set<Integer> participants;
		public Map<Integer, Boolean> responses;
		public String status;
		
		public TxnState(UUID txnID, Set<Integer> participants, String request, String args) {
			this(txnID, participants, new HashMap<Integer, Boolean>(), request, args);
		}
		
		public TxnState(UUID txnID, Set<Integer> participants, Map<Integer, Boolean> responses,
				String request, String args) {
			this.txnID = txnID;
			this.participants = participants;
			this.responses = responses;
			this.request = request;
			this.args = args;
		}
		
		/*
		 * Returns those that have accepted so far.
		 */
		public Set<Integer> getAcceptors() {
			Set<Integer> result = new HashSet<Integer>();
			for (Integer addr : responses.keySet()) {
				if (responses.get(addr)) {
					result.add(addr);
				}
			}
			return result;
		}
		
		public boolean allAccepted() {
			if (!allVotesIn()) 
				return false;
			for (Boolean accept : responses.values()) {
				if (!accept) 
					return false;
			}
			return true;
		}
		
		public boolean allVotesIn() {
			return responses.size() == participants.size();
		}
		
		public void accept(int from) {
			responses.put(from, true);
		}
		
		public void reject(int from) {
			responses.put(from, false);
		}
		
		/*
		 * Returns a string of the form
		 * 
		 * "txnID addrList request args"
		 */
		public String toRecordString() {
			String addrList = TxnPacket.addrListStr(participants);
			return txnID + " " + addrList + " " + request + " " + args;
		}

		/*
		 * Expects a string of the form
		 * 
		 * "txnID addrList request args"
		 */
		public static TxnState fromRecordString(String data) {
			String[] parts = data.split(" ", 4);
			Set<Integer> addrs = new HashSet<Integer>();
			String[] addrStrs = parts[1].split(",");
			for (String addr : addrStrs) {
				addrs.add(Integer.parseInt(addr));
			}
			return new TxnState(UUID.fromString(parts[0]), addrs, parts[2], parts[3]);
		}
	}