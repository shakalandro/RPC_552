import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.*;
import java.util.*;

import org.apache.commons.lang.ArrayUtils;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

/*
 * This class provides distributed transactional operations. The operations are generic.
 * 
 */


public class TransactionNode extends RPCNode {
	
	public static final String PROPOSAL_PREFIX = "propose";
	public static final String COMMIT_PREFIX = "commit";
	public static final String ABORT_PREFIX = "abort";
	
	public static final int PROPOSAL_RESPONSE_TIMEOUT = 20;
	public static final int DECISION_TIMEOUT = 20;
	public static final int DECISION_RESEND_TIMEOUT = 20;
	public static final String LOG_FILE = ".txn_log";
	
	public Map<UUID, TxnState> openTxns;
	private String logFile;
	private TxnLog txnLogger;
	
	@Override
	public void start() {
		super.start();
		this.openTxns = new HashMap<UUID, TxnState>();
		this.logFile = LOG_FILE + addr;
		this.txnLogger = new TxnLog(this.logFile, this);
		try {
			if (Utility.fileExists(this, this.logFile)) {
				PersistentStorageReader reader =  getReader(this.logFile);
				while (reader.ready()) {
					String line = reader.readLine();
					TxnLog.Record r = TxnLog.parseRecordType(line);
					switch (r) {
					case START:
						TxnState txnState = TxnState.fromRecordString(line);
						openTxns.put(txnState.txnID, txnState);
						break;
					case ACCEPT:
						// TODO: finish logging
						break;
					default:
						break;
					}
				}
			}
		} catch (IOException e) {
			logError("Could not parse transaction log during recovery.");
			e.printStackTrace();
			fail();
		}
	}
	
	////////////////////////////////// Coordinator Code //////////////////////////////////////////
	
	/*
	 * Sends a transaction command that a subclass must know how to respond to. The subclass must
	 * implement the transaction handlers which will be called if the transaction commits or aborts.
	 * If the transaction request is Foo, then the client must implement the following methods:
	 * 
	 * boolean proposeFoo(byte[] args);
	 * void commitFoo(byte[] args);
	 * void abortFoo(byte[] args);
	 */
	public void proposeTransaction(Set<Integer> participant_addrs, String request, String args) {
		if (!participant_addrs.contains(this.addr)) {
			participant_addrs.add(this.addr);
		}
		UUID txnID = UUID.randomUUID();
		TxnState txnState = new TxnState(txnID, participant_addrs, request, args);
		openTxns.put(txnID, txnState);
		for (int addr : openTxns.get(txnID).participants) {
			TxnPacket txnPkt = TxnPacket.getPropositionPacket(this, txnID, participant_addrs, 
					request, args);
			// Respond to the participant's accept or reject response
			Callback success = createCallback("receiveProposalResponse",
					new String[] {"java.lang.Integer", "java.lang.byte[]"}, null);
			// Abort the whole transaction
			Callback failure = createCallback("sendTxnAbort", new String[] {"java.util.UUID"},
					new Object[] {txnID});
			this.makeRequest(Command.TXN, txnPkt.pack(), success, failure, addr, "");
		}
		txnLogger.logStart(txnState);
		// Abort the transaction if we do not hear from all participants in a timely manner
		Callback abortTimeout = createCallback("proposalTimeoutAbort",
				new String[] {"java.util.UUID"}, new Object[] {txnID});
		addTimeout(abortTimeout, PROPOSAL_RESPONSE_TIMEOUT);
	}
	
	/*
	 * Success callback for proposeTransaction. If all the participants have responded then a
	 * decision is made and multicast out to the participants.
	 */
	private void receiveProposalResponse(int from, byte[] response) {
		TxnPacket pkt = TxnPacket.unpack(response);
		TxnState txnState = openTxns.get(pkt.getID());
		if (pkt.getProtocol() == TxnProtocol.TXN_ACCEPT) {
			logOutput("Received proposal acceptance from node " + from);
			txnState.accept(from);
		} else {
			logOutput("Received proposal rejection from node " + from + ": " + pkt.getPayload());
			txnState.reject(from);
		}
		if (txnState.allVotesIn()) {
			if (txnState.allAccepted()) {
				txnLogger.logCommit(txnState.txnID);
				sendTxnCommit(txnState.txnID);
			} else {
				txnLogger.logAbort(txnState.txnID);
				sendTxnAbort(txnState.txnID);
			}
		}
	}
	
	/*
	 * Callback for proposal response timer. Aborts the transaction if we have not heard from all
	 * of the participants.
	 */
	public void proposalTimeoutAbort(UUID txnID) {
		TxnState txnState = openTxns.get(txnID);
		if (!txnState.allVotesIn()) {
			sendTxnAbort(txnID);
		}
	}
	
	/*
	 * Notifies all participants of a commit decision.
	 */
	private void sendTxnCommit(UUID txnID) {
		TxnState txnState = openTxns.get(txnID);
		for (Integer addr : txnState.participants) {
			TxnPacket txnPkt = TxnPacket.getCommitPacket(this, txnID, txnState.request,
					txnState.args);
			makeRequest(Command.TXN, txnPkt.pack(), null, null, addr, "");
		}
	}
	
	/*
	 * Notifies all participants that accepted the proposal of an abort decision.
	 */
	private void sendTxnAbort(UUID txnID) {
		for (Integer addr : openTxns.get(txnID).getAcceptors()) {
			TxnPacket txnPkt = TxnPacket.getAbortPacket(this, txnID);
			makeRequest(Command.TXN, txnPkt.pack(), null, null, addr, "");
		}
	}
	
	////////////////////////////////// Participant Code //////////////////////////////////////////
	
	/*
	 * Handles responding to a transaction request. Responds with either an ACK or NACK. If the
	 * transaction proposal request is Foo, the user must implement a method with the following signature
	 * 
	 * boolean proposeFoo(byte[]);
	 * 
	 * where the byte array are arguments to the request that the client should know how to parse.
	 * If the client returns true then the transaction proposal is accepted otherwise the proposal
	 * is rejected.
	 */
	private TxnPacket receiveTxnProposal(TxnPacket pkt) {		
		String request = pkt.getRequest();
		TxnState txnState = openTxns.get(pkt.getID());
		try {
			Class<? extends TransactionNode> me = this.getClass();
			Method handler = me.getDeclaredMethod(PROPOSAL_PREFIX + request, byte[].class);
			Boolean accept = (Boolean)handler.invoke(me, pkt.getPayload());
			if (accept) {
				txnLogger.logAccept(txnState.txnID, pkt.getParticipants());
				txnState.status = TxnState.WAITING;
				// Request decision status if we don't hear back soon
				Callback decisionTimeout = createCallback("sendDecisionRequest",
						new String[] {"java.util.UUID"}, new Object[] {pkt.getID()});
				addTimeout(decisionTimeout, DECISION_TIMEOUT);
				return TxnPacket.getAcceptPacket(this, pkt.getID(), txnState.participants);
			} else {
				txnLogger.logReject(txnState.txnID);
				txnState.status = TxnState.ABORTED;
				recieveTxnAbort(pkt);
				return TxnPacket.getRejectPacket(this, pkt.getID(), "rejected");
			}
		} catch (NoSuchMethodException e) {
			logError("There is no handler for transaction proposal: " + request);
			return TxnPacket.getRejectPacket(this, pkt.getID(), "No proposal handler exists.");
		} catch (IllegalArgumentException e) {
			logError("The proposal handler for \"" + request + "\" does not take correct parameters.");
			return TxnPacket.getRejectPacket(this, pkt.getID(), "Invalid proposal handler.");
		} catch (Exception e) {
			logError("There was an issue invoking the proposal handler for: " + request +
					"\n" + e.getMessage());
			this.fail();
			return null;
		}
	}
	
	/*
	 * Handles responding to a transaction commit. If the transaction request is Foo, the user must
	 * implement a method with the following signature
	 * 
	 * void commitFoo(byte[]);
	 * 
	 * where the byte array are arguments to the request that the client should know how to parse.
	 * 
	 * NOTE:If the commit handler fails or does not exist as expected then the state of the
	 * transaction is undefined.
	 */
	private void recieveTxnCommit(TxnPacket pkt) {
		txnLogger.logCommit(pkt.getID());
		openTxns.get(pkt.getID()).status = TxnState.COMMITTED;
		String request = pkt.getRequest();
		try {
			Class<? extends TransactionNode> me = this.getClass();
			Method handler = me.getDeclaredMethod(COMMIT_PREFIX + request, byte[].class);
			handler.invoke(me, pkt.getPayload());
		} catch (NoSuchMethodException e) {
			logError("There is no handler for transaction commit: " + request);
			fail();
		} catch (IllegalArgumentException e) {
			logError("The commit handler for \"" + request + "\" does not take correct parameters.");
			fail();
		} catch (Exception e) {
			logError("There was an issue invoking the commit handler for: " + request +
					"\n" + e.getMessage());
			this.fail();
		}
	}
	
	/*
	 * Handles responding to a transaction abort. If the transaction request is Foo, the user must
	 * implement a method with the following signature
	 * 
	 * void abortFoo(byte[]);
	 * 
	 * where the byte array are arguments to the request that the client should know how to parse.
	 * 
	 * NOTE:If the abort handler fails or does not exist as expected then the state of the
	 * transaction is undefined.
	 */
	private void recieveTxnAbort(TxnPacket pkt) {
		txnLogger.logAbort(pkt.getID());
		openTxns.get(pkt.getID()).status = TxnState.ABORTED;
		String request = pkt.getRequest();
		try {
			Class<? extends TransactionNode> me = this.getClass();
			Method handler = me.getDeclaredMethod(ABORT_PREFIX + request, byte[].class);
			handler.invoke(me, pkt.getPayload());
		} catch (NoSuchMethodException e) {
			logError("There is no handler for transaction abort: " + request);
			fail();
		} catch (IllegalArgumentException e) {
			logError("The abort handler for \"" + request + "\" does not take correct parameters.");
			fail();
		} catch (Exception e) {
			logError("There was an issue invoking the abort handler for: " + request +
					"\n" + e.getMessage());
			this.fail();
		}
	}
	
	////////////////////////////////// Initiator Termination Code ////////////////////////////////
	
	public void sendDecisionRequest(UUID txnID) {
		TxnState txnState = openTxns.get(txnID);
		for (Integer addr : txnState.participants) {
			TxnPacket txnPkt = TxnPacket.getDecisionRequestPacket(this, txnID);
			Callback success = createCallback("receiveDecisionResponse",
					new String[] {"java.lang.byte[]"}, null);
			makeRequest(Command.TXN, txnPkt.pack(), success, null, addr, "");
		}
		Callback decisionTimeout = createCallback("resendDecisionRequest",
				new String[] {"java.util.UUID"}, new Object[] {txnID});
		addTimeout(decisionTimeout, DECISION_RESEND_TIMEOUT);
	}
	
	public void resendDecisionRequest(UUID txnID) {
		TxnState txnState = openTxns.get(txnID);
		if (txnState.status == TxnState.WAITING) {
			sendDecisionRequest(txnID);
		}
	}
	
	public void receiveDecisionResponse(byte[] response) {
		if (response != null && response.length > 0) {
			TxnPacket pkt = TxnPacket.unpack(response);
			if (pkt.getProtocol() == TxnProtocol.TXN_COMMIT) {
				recieveTxnCommit(pkt);
			} else if (pkt.getProtocol() == TxnProtocol.TXN_ABORT) {
				recieveTxnAbort(pkt);
			}
		}
	}
	
	////////////////////////////////// Responder Termination Code ////////////////////////////////
	
	public TxnPacket receiveTxnDecisionRequest(TxnPacket pkt) {
		// respond with status if we know it
		TxnState txnState = openTxns.get(pkt.getID());
		if (txnState.status == TxnState.ABORTED) {
			return TxnPacket.getAbortPacket(this, txnState.txnID);
		} else if (txnState.status == TxnState.COMMITTED) {
			return TxnPacket.getCommitPacket(this, txnState.txnID, txnState.request, txnState.args);
		}
		return null;
	}
		
	////////////////////////////////// RPC - TXN GLUE ////////////////////////////////////////////

	/*
	 * Handles the logic for responding to transaction control messages.
	 */
	@Override
	protected RPCResultPacket handleRPCCommand(Command request, int senderAddr, RPCRequestPacket pkt) {
        if (request == Command.TXN) {
        	TxnPacket txnPkt = TxnPacket.unpack(pkt.getPayload());
        	byte[] result = Utility.stringToByteArray("");
        	TxnProtocol protocol = txnPkt.getProtocol();
        	
    		switch (protocol) {
    		case TXN_PROP:
    			result = receiveTxnProposal(txnPkt).pack();
    			break;
    		case TXN_COMMIT:
    			recieveTxnCommit(txnPkt);
    			break;
    		case TXN_ABORT:
    			recieveTxnAbort(txnPkt);
    			break;
    		case TXN_DECISION_REQ:
    			TxnPacket response = receiveTxnDecisionRequest(txnPkt);
    			if (response != null) {
    				result = response.pack();
    			}
    			break;
    		default:
    			logError("Unknown transaction control message: " + protocol);
    		}
    		
        	return RPCResultPacket.getPacket(this, this.addr, Status.SUCCESS, result);
        } else {
        	return super.handleRPCCommand(request, senderAddr, pkt);
        }
	}
	
	////////////////////////////////// Helper Code //////////////////////////////////////////////
	
	private Callback createCallback(String methodName, String[] parameterTypes, Object[] params) {
		Method m;
		try {
			m = Callback.getMethod(methodName, this, parameterTypes);
		} catch (Exception e) {
			logError("Could not instantiate callback");
			e.printStackTrace();
			fail();
			return null;
		}
		return new Callback(m, this, params);
	}
	
	/*
	 * Helper class that handles writing to the transaction log.
	 */
	static class TxnLog {
		
		public PersistentStorageWriter writer;
		public TransactionNode node;
		
		public enum Record {
			START("start"),
			ACCEPT("accept"),
			REJECT("reject"),
			COMMIT("commit"),
			ABORT("abort");
			
			private final String msg;
			
			private Record(String msg) {
				this.msg = msg;
			}
		}
		
		public TxnLog(String filename, TransactionNode node) {
			try {
				this.writer = node.getWriter(filename, true);
				this.node = node;
			} catch (IOException e) {
				node.logError("Could not open read or write to transaction log: " + filename);
				node.fail();
			}
		}
		
		private void logRecord(Record r, String data) {
			try {
				writer.append(r.msg + " " + data + "\n");
			} catch (IOException e) {
				node.logError("Could not write to log file.");
				node.fail();
			}
		}
		
		public void logStart(TxnState txnState) {
			logRecord(Record.START, txnState.toRecordString());
		}
		
		public void logAccept(UUID txnID, Set<Integer> addrs) {
			logRecord(Record.ACCEPT, txnID.toString() + " " + TxnPacket.addrListStr(addrs));
		}
		
		public void logReject(UUID txnID) {
			logRecord(Record.REJECT, txnID.toString());
		}
		
		public void logCommit(UUID txnID) {
			logRecord(Record.COMMIT, txnID.toString());
		}
		
		public void logAbort(UUID txnID) {
			logRecord(Record.ABORT, txnID.toString());
		}
		
		/*
		 * Given a string, returns the log record type that it is. Returns null if the line does not
		 * match an existing record type.
		 */
		public static Record parseRecordType(String s) {
			if (s.equals(Record.START.msg)) {
				return Record.START;
			} else if (s.equals(Record.ACCEPT.msg)) {
				return Record.ACCEPT;
			} else if (s.equals(Record.REJECT.msg)) {
				return Record.REJECT;
			} else if (s.equals(Record.COMMIT.msg)) {
				return Record.COMMIT;
			} else if (s.equals(Record.ABORT.msg)) {
				return Record.ABORT;
			} else {
				return null;
			}
		}
	}
}
