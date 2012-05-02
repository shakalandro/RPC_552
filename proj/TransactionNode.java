import java.io.*;
import java.lang.reflect.*;
import java.util.*;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

/*
 * Allows for distributed transactions if a subclass knows how to respond to a request. The subclass must
 * implement the transaction handlers which will be called if the transaction commits or aborts.
 * If the transaction request is "Foo", then the client must implement the following methods:
 * 
 * boolean proposeFoo(UUID txnID, String args);
 * void commitFoo(UUID txnID, String args);
 * void abortFoo(UUID txnID, String args);
 * 
 * These propose and abort handlers should be idempotent and probably will be called multiple times
 * before the transaction is over.
 * 
 * The commit handler will only be called once and should be atomic if you want it to
 * definitely happen.
 */
public class TransactionNode extends RPCNode {
	
	public static final String PROPOSAL_PREFIX = "propose";
	public static final String COMMIT_PREFIX = "commit";
	public static final String ABORT_PREFIX = "abort";
	
	public static final int PROPOSAL_RESPONSE_TIMEOUT = 20;
	public static final int DECISION_TIMEOUT = 20;
	public static final int DECISION_RESEND_TIMEOUT = 20;
	public static final String LOG_FILE = ".txn_log";
	
	public Map<UUID, TxnState> coordinatorTxns;
	public Map<UUID, TxnState> participantTxns;
	private String logFile;
	private TxnLog txnLogger;
	
	@Override
	public void start() {
		super.start();
		this.coordinatorTxns = new HashMap<UUID, TxnState>();
		this.participantTxns = new HashMap<UUID, TxnState>();
		this.logFile = LOG_FILE + addr;
		this.txnLogger = new TxnLog(this.logFile, this);
		try {
			if (Utility.fileExists(this, this.logFile)) {
				PersistentStorageReader reader =  getReader(this.logFile);
				while (reader.ready()) {
					String line = reader.readLine();
					String[] parts = line.split(" ", 2);
					TxnLog.Record r = TxnLog.parseRecordType(parts[0]);
					String txnRecordData = parts[1];
					TxnState txnState;
					switch (r) {
					// We are the coordinator of a txn
					case START:
						txnState = TxnState.fromRecordString(txnRecordData);
						coordinatorTxns.put(txnState.txnID, txnState);
						break;
					// We have reached a decision, could be a coordinator and/or participant
					case COMMIT:
						txnState = TxnState.fromRecordString(txnRecordData);
						if (coordinatorTxns.containsKey(txnState.txnID)) {
							coordinatorTxns.get(txnState.txnID).status = TxnState.TxnStatus.COMMITTED;
						}
						if (participantTxns.containsKey(txnState.txnID)) {
							participantTxns.get(txnState.txnID).status = TxnState.TxnStatus.COMMITTED;
						}
						break;
					// We have reached a decision, could be a coordinator and/or participant
					case ABORT:
						txnState = TxnState.fromRecordString(txnRecordData);
						if (coordinatorTxns.containsKey(txnState.txnID)) {
							coordinatorTxns.get(txnState.txnID).status = TxnState.TxnStatus.ABORTED;
						}
						if (participantTxns.containsKey(txnState.txnID)) {
							participantTxns.get(txnState.txnID).status = TxnState.TxnStatus.ABORTED;
						}
						break;
					// We are the participant of a txn but may have not reached a decision
					case ACCEPT:
						txnState = TxnState.fromRecordString(txnRecordData);
						txnState.status = TxnState.TxnStatus.WAITING;
						participantTxns.put(txnState.txnID, txnState);
						break;
					// I don't think REJECT is logged, but check for good measure
					case REJECT:
						txnState = TxnState.fromRecordString(txnRecordData);
						txnState.status = TxnState.TxnStatus.ABORTED;
						participantTxns.put(txnState.txnID, txnState);
						break;
					case DONE:
						txnState = TxnState.fromRecordString(txnRecordData);
						participantTxns.get(txnState.txnID).status = TxnState.TxnStatus.DONE;
						break;
					}
				}
				// If there are outstanding coordinated txns without decisions then abort them
				for (TxnState txnState : coordinatorTxns.values()) {
					if (txnState.status == TxnState.TxnStatus.UNKNOWN) {
						txnLogger.logAbort(txnState);
						sendTxnAbort(txnState.txnID);
					}
				}
				// If the participant did not log ACCEPT or REJECT then do nothing, this will result
				// 		in abort eventually
				
				// If a participant has made a decision but did not log DONE, then we will run the
				//		appropriate handler again.
				// If the participant logged an ACCEPT but no decision then we must start the
				// 		termination protocol.
				for (TxnState txnState : participantTxns.values()) {
					if (txnState.status == TxnState.TxnStatus.WAITING) {
						sendDecisionRequest(txnState.txnID);
					} else if (txnState.status == TxnState.TxnStatus.ABORTED) {
						recieveTxnAbort(TxnPacket.getAbortPacket(this, txnState.txnID));
					} else if (txnState.status == TxnState.TxnStatus.COMMITTED) {
						recieveTxnCommit(TxnPacket.getCommitPacket(this, txnState.txnID,
								txnState.request, txnState.args));
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
	 * boolean proposeFoo(UUID txnID, String args);
	 * void commitFoo(UUID txnID, String args);
	 * void abortFoo(UUID txnID, String args);
	 */
	public void proposeTransaction(Set<Integer> participant_addrs, String request, String args) {
		if (!participant_addrs.contains(this.addr)) {
			participant_addrs.add(this.addr);
		}
		UUID txnID = UUID.randomUUID();
		TxnState txnState = new TxnState(txnID, participant_addrs, request, args);
		coordinatorTxns.put(txnID, txnState);
		for (int addr : coordinatorTxns.get(txnID).participants) {
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
	public void receiveProposalResponse(int from, byte[] response) {
		TxnPacket pkt = TxnPacket.unpack(response);
		TxnState txnState = coordinatorTxns.get(pkt.getID());
		if (pkt.getProtocol() == TxnProtocol.TXN_ACCEPT) {
			logOutput("Received proposal acceptance from node " + from);
			txnState.accept(from);
		} else {
			logOutput("Received proposal rejection from node " + from + ": " + pkt.getPayload());
			txnState.reject(from);
		}
		if (txnState.allVotesIn()) {
			if (txnState.allAccepted()) {
				txnLogger.logCommit(txnState);
				sendTxnCommit(txnState.txnID);
			} else {
				txnLogger.logAbort(txnState);
				sendTxnAbort(txnState.txnID);
			}
		}
	}
	
	/*
	 * Callback for proposal response timer. Aborts the transaction if we have not heard from all
	 * of the participants.
	 */
	public void proposalTimeoutAbort(UUID txnID) {
		TxnState txnState = coordinatorTxns.get(txnID);
		if (!txnState.allVotesIn()) {
			sendTxnAbort(txnID);
		}
	}
	
	/*
	 * Notifies all participants of a commit decision.
	 */
	private void sendTxnCommit(UUID txnID) {
		TxnState txnState = coordinatorTxns.get(txnID);
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
		for (Integer addr : coordinatorTxns.get(txnID).getAcceptors()) {
			TxnPacket txnPkt = TxnPacket.getAbortPacket(this, txnID);
			makeRequest(Command.TXN, txnPkt.pack(), null, null, addr, "");
		}
	}
	
	////////////////////////////////// Participant Code //////////////////////////////////////////
	
	/*
	 * Returns the number of txns that the participant has not fully finished. A txn is fully
	 * finished if the commit handler or the abort handler is known to have run to completion.
	 */
	public int numUnfinishedTxns() {
		int count = 0;
		for (TxnState txnState : participantTxns.values()) {
			if (txnState.status != TxnState.TxnStatus.DONE) {
				count++;
			}
		}
		return count;
	}
	
	/*
	 * Handles responding to a transaction request. Responds with either an ACK or NACK. If the
	 * transaction proposal request is Foo, the user must implement a method with the following signature
	 * 
	 * boolean proposeFoo(UUID txnID, String args);
	 * 
	 * where the byte array are arguments to the request that the client should know how to parse.
	 * If the client returns true then the transaction proposal is accepted otherwise the proposal
	 * is rejected.
	 */
	private TxnPacket receiveTxnProposal(TxnPacket pkt) {		
		String request = pkt.getRequest();
		try {
			UUID txnID = pkt.getID();
			if (participantTxns.containsKey(txnID)) {
				logError("We should not recieve a second proposal with the same ID.");
				fail();
			}
			TxnState txnState = new TxnState(txnID, pkt.getParticipants(),
					pkt.getRequest(), pkt.getPayload());
			participantTxns.put(txnID, txnState);
			Class<? extends TransactionNode> me = this.getClass();
			Method handler = me.getDeclaredMethod(PROPOSAL_PREFIX + request, java.lang.String.class,
					java.util.UUID.class);
			Boolean accept = (Boolean)handler.invoke(me, pkt.getPayload(), txnState.txnID);
			if (accept) {
				txnLogger.logAccept(txnState);
				txnState.status = TxnState.TxnStatus.WAITING;
				// Request decision status if we don't hear back soon
				Callback decisionTimeout = createCallback("sendDecisionRequest",
						new String[] {"java.util.UUID"}, new Object[] {pkt.getID()});
				addTimeout(decisionTimeout, DECISION_TIMEOUT);
				return TxnPacket.getAcceptPacket(this, pkt.getID());
			} else {
				txnLogger.logReject(txnState);
				txnState.status = TxnState.TxnStatus.ABORTED;
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
	 * void commitFoo(UUID txnID, String args);
	 * 
	 * where the byte array are arguments to the request that the client should know how to parse.
	 * 
	 * NOTE:If the commit handler fails or does not exist as expected then the state of the
	 * transaction is undefined.
	 */
	private void recieveTxnCommit(TxnPacket pkt) {
		TxnState txnState = participantTxns.get(pkt.getID());
		txnLogger.logCommit(txnState);
		txnState.status = TxnState.TxnStatus.COMMITTED;
		String request = pkt.getRequest();
		try {
			Class<? extends TransactionNode> me = this.getClass();
			Method handler = me.getDeclaredMethod(COMMIT_PREFIX + request, java.lang.String.class,
					java.util.UUID.class);
			handler.invoke(me, pkt.getPayload(), txnState.txnID);
			txnLogger.logDone(txnState);
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
	 * void abortFoo(UUID txnID, String args);
	 * 
	 * where the byte array are arguments to the request that the client should know how to parse.
	 * 
	 * NOTE:If the abort handler fails or does not exist as expected then the state of the
	 * transaction is undefined.
	 */
	private void recieveTxnAbort(TxnPacket pkt) {
		TxnState txnState = participantTxns.get(pkt.getID());
		txnLogger.logAbort(txnState);
		txnState.status = TxnState.TxnStatus.ABORTED;
		String request = pkt.getRequest();
		try {
			Class<? extends TransactionNode> me = this.getClass();
			Method handler = me.getDeclaredMethod(ABORT_PREFIX + request, java.lang.String.class,
					java.util.UUID.class);
			handler.invoke(me, pkt.getPayload(), txnState.txnID);
			txnLogger.logDone(txnState);
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
	
	/*
	 * Asks all transaction participants to tell this the decision status.
	 */
	public void sendDecisionRequest(UUID txnID) {
		TxnState txnState = participantTxns.get(txnID);
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
		TxnState txnState = participantTxns.get(txnID);
		if (txnState.status == TxnState.TxnStatus.WAITING) {
			sendDecisionRequest(txnID);
		}
	}
	
	/*
	 * Parses a decision request response, which could be an abort notification, commit notification
	 * or an empty response symbolizing that the participant is waiting.
	 */
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
		TxnState txnState = participantTxns.get(pkt.getID());
		if (txnState.status == TxnState.TxnStatus.ABORTED) {
			return TxnPacket.getAbortPacket(this, txnState.txnID);
		} else if (txnState.status == TxnState.TxnStatus.COMMITTED) {
			return TxnPacket.getCommitPacket(this, txnState.txnID, txnState.request, txnState.args);
		}
		// Return null to RPC layer if we don't know what happened.
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
	
	protected Callback createCallback(String methodName, String[] parameterTypes, Object[] params) {
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
			ABORT("abort"),
			DONE("done");
			
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
		
		public void logAccept(TxnState txnState) {
			logRecord(Record.ACCEPT, txnState.toRecordString());
		}
		
		public void logReject(TxnState txnState) {
			logRecord(Record.REJECT, txnState.toRecordString());
		}
		
		public void logCommit(TxnState txnState) {
			logRecord(Record.COMMIT, txnState.toRecordString());
		}
		
		public void logAbort(TxnState txnState) {
			logRecord(Record.ABORT, txnState.toRecordString());
		}
		
		public void logDone(TxnState txnState) {
			logRecord(Record.DONE, txnState.toRecordString());
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
