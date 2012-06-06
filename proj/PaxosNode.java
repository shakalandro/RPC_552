import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

/**
 * Implements the logic for Paxos. TODO: need some mechanism for initiating the query for command
 * decisions
 * 
 * @author Roy McElmurry (roy.miv@gmail.com)
 */
public abstract class PaxosNode extends RPCNode {
	private static final int STARTING_BACKOFF = 30;
	private static final int RANDOM_BACKOFF_MAX = 20;
	private static final String PAXOS_LOG_FILE = ".paxos";
	private static final String TEMP_PAXOS_LOG_FILE = PAXOS_LOG_FILE + "_temp";
	private static final String PAXOS_STATE_FILE = ".paxos_state";
	private static final String TEMP_PAXOS_STATE_FILE = PAXOS_STATE_FILE + "_temp";
	private static final String COLOR_OUTPUT = "0;34";
	private static final String COLOR_ERROR = "0;31";
	private static final Random r = new Random();
	private static final int MAX_NODES = 1000;
	
	public static final byte[] noopMarker = Utility.stringToByteArray("NOOP_MARKER");

	// State data shared by all roles.
	private TreeMap<Integer, PaxosState> rounds;
	
	private int highestExecutedNum = -1;
	
	protected final static Integer[] REPLICA_ADDRS = {0, 1, 2, 4};
	
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
		if (addrs.size() > MAX_NODES) {
			throw new IllegalArgumentException("This algorithm breaks if you have more than " + MAX_NODES + " replicas.");
		}
		noteOutput("About to attempt to replicate! Payload: " + Utility.byteArrayToString(payload));
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
		return (this.addr + MAX_NODES * ((last / MAX_NODES) + 1));
	}

	private void proposeCommand(List<Integer> addrs, Integer instNum, byte[] payload) {
		PaxosState state = this.rounds.get(instNum);
		proposeCommand(addrs, instNum, state.propNum, payload, STARTING_BACKOFF);
	}

	public void proposeCommand(List<Integer> addrs, Integer instNum, Integer propNum,
			byte[] payload, Integer backoff) {
		PaxosState state = this.rounds.get(instNum);
		
		// If we don't know what the decision was yet...
		if (!state.decided) {
			state.propNum = propNum;
			for (Integer nodeAddr : addrs) {
				PaxosPacket prepare = PaxosPacket.makePrepareMessage(instNum, propNum, payload);
				
				noteOutput("Prepare (" + instNum + "," + propNum + ") sent to " + nodeAddr
							+ " with value: " + (!Arrays.equals(payload, noopMarker) ? Utility.byteArrayToString(payload) : "no-op"));
				
				RIOSend(nodeAddr, Protocol.PAXOS_PKT, prepare.pack());
			}
			try {
				Method m = Callback.getMethod("proposeCommand", this,
						new String[] { List.class.getName(), Integer.class.getName(),
						Integer.class.getName(), byte[].class.getName(), Integer.class.getName()
						});
				Callback retry = new Callback(m, this, new Object[] { addrs, instNum,
						getNextPropNum(propNum), payload,
						backoff * 2 + r.nextInt() % RANDOM_BACKOFF_MAX });
				addTimeout(retry, backoff);
			} catch (Exception e) {
				noteError("(" + instNum + ") Failed to make callback for proposal retry.");
				e.printStackTrace();
				noteError("***************************");
				noteError("FAILING");
				noteError("***************************");
				fail();
			}
			
		// retry if we were a proposer and our value did not win
		} else if (state.value != null && !Arrays.equals(state.value, noopMarker) && !Arrays.equals(state.value, state.decidedValue)) {
			noteOutput("(" + instNum + ") Paxos round already decided, but it wasn't my value, retrying");
			noteOutput("(" + instNum + ") Decided value was: " + Utility.byteArrayToString(state.decidedValue) + 
					" but my value is " + Utility.byteArrayToString(state.value));
			retryPaxosCommand(state.participants, state.instNum, state.value);
		} else {
			noteOutput("(" + instNum + ") Paxos round already decided, and I won, no need to try again");
		}
	}
	
	// Override this in order to implement custom retry behavior
	public void retryPaxosCommand(List<Integer> addrs, Integer instNum, byte[] payload) {
		// Retry as long as it isn't a noop attempt.
		if (payload != null && !Arrays.equals(payload, noopMarker)) {
			replicateCommand(addrs, payload);
		}
	}

	private void handlePromiseResponse(int from, int instNum, int highestAccept, byte[] payload) {
		PaxosState state = this.rounds.get(instNum);
		state.promised.add(from);
		noteOutput("(" + instNum + ") recieved promise from " + from + " with highestAccepted = " + highestAccept);
		
		if (highestAccept > state.highestAcceptedNum) {
			noteOutput("(" + instNum + ") promise came with a higher acceptance value");
			state.setHighest(highestAccept, payload);
		}
		
		if (state.quorumPromised() && !state.acceptRequestsSent) {
			noteOutput("(" + instNum + ") quorum promised");
			for (Integer nodeAddr : state.promised) {
				PaxosPacket accept =
						PaxosPacket.makeAcceptMessage(instNum, state.propNum,
								state.highestAcceptedValue);
				noteOutput("Accept request (" + instNum + "," + state.propNum + ") sent to "
						+ nodeAddr + " with value: " + Utility.byteArrayToString(state.highestAcceptedValue));
				RIOSend(nodeAddr, Protocol.PAXOS_PKT, accept.pack());
			}
			state.acceptRequestsSent = true;
			
		// If there was a quorom but the accept requests weren't already sent, it can't hurt to send them again.
		// This will ensure that the result gets executed on all replicas.
		} else if (state.quorumPromised()) {
			noteOutput("(" + instNum + ") quorum promised, but accept requests already sent");
			for (Integer nodeAddr : state.promised) {
				PaxosPacket accept =
						PaxosPacket.makeAcceptMessage(instNum, state.propNum,
								state.highestAcceptedValue);
				noteOutput("Accept request (" + instNum + "," + state.propNum + ") sent to "
						+ nodeAddr + " with value: " + Utility.byteArrayToString(state.highestAcceptedValue));
				RIOSend(nodeAddr, Protocol.PAXOS_PKT, accept.pack());
			}
		} else {
			noteOutput("(" + instNum + ") " + state.numPromised() + " out of " + state.participants.size() + " promised");
		}
	}

	// This is called when another node says it will accept your proposal.
	private void handleAcceptResponse(int from, int instNum, int n, byte[] payload) {
		PaxosState state = this.rounds.get(instNum);
		state.accepted.add(from);
		if (state.quorumAccepted() && !state.decisionsSent) {
			noteOutput("(" + instNum + ") quorum accepted value: "
					+ Utility.byteArrayToString(payload));
			for (Integer nodeAddr : state.participants) {
				PaxosPacket decision = PaxosPacket.makeDecisionMessage(instNum, n, payload);
				noteOutput("(" + instNum + ") sending decision to " + nodeAddr);
				RIOSend(nodeAddr, Protocol.PAXOS_PKT, decision.pack());
			}
			state.decisionsSent = true;
		} else if (state.quorumAccepted()) {
			noteOutput("(" + instNum + ") quorum accepted, but decisions already sent");
			noteOutput("(" + instNum + ") quorum accepted value: "
					+ Utility.byteArrayToString(payload));
			for (Integer nodeAddr : state.participants) {
				PaxosPacket decision = PaxosPacket.makeDecisionMessage(instNum, n, payload);
				noteOutput("(" + instNum + ") sending decision to " + nodeAddr);
				RIOSend(nodeAddr, Protocol.PAXOS_PKT, decision.pack());
			}
		} else {
			noteOutput("(" + instNum + ") " + state.numAccepted() + " out of " + state.participants.size() + " accepted");
		}
	}

	/******************************** Participant Code ********************************/

	// Returns true if the participant promises not to accept a higher numbered proposal
	// and false otherwise.
	private void handlePrepareRequest(int from, int instNum, int n, byte[] payload) {
		PaxosState state = this.rounds.get(instNum);
		if (state == null) {
			noteOutput("(" + instNum + ") Prepare request for unknown round, creating state for it");
			state = new PaxosState(instNum, n, payload, Arrays.asList(REPLICA_ADDRS));
			this.rounds.put(instNum, state);
		}
		
		if (n > state.promisedPropNum) {
			state.promisedPropNum = n;
			logKnownStates();
			PaxosPacket promise =
					PaxosPacket.makePromiseMessage(state.instNum, state.acceptedPropNum,
							(state.acceptedValue == null) ? payload : state.acceptedValue);
			noteOutput("(" + instNum + ") promise not to accept lower than " + n);
			byte[] packed = promise.pack();
			RIOSend(from, Protocol.PAXOS_PKT, packed);
		} else {
			noteOutput("(" + instNum + ") ignoring prepare request, I already promised higher");
		}
	}

	// Returns true if the participant accepts the proposal and false otherwise.
	// 1) We cannot accept if we promised not to
	// 2) We cannot accept if we already accepted a higher one
	private void handleAcceptRequest(int from, int instNum, int n, byte[] payload) {
		PaxosState state = this.rounds.get(instNum);
		if (state == null) {
			return;
		}
		
		if (n >= state.promisedPropNum && (state.decidedValue == null || Arrays.equals(state.decidedValue, payload))) {
			state.acceptedPropNum = n;
			state.acceptedValue = payload;
			logKnownStates();
			PaxosPacket accepted = PaxosPacket.makeAcceptedMessage(instNum, n, payload);
			noteOutput("(" + instNum + ") Accepted prop " + n + " with payload " + Utility.byteArrayToString(payload));
			RIOSend(from, Protocol.PAXOS_PKT, accepted.pack());
		} else {
			noteOutput("(" + instNum + ") I promised not to accept lower than "
					+ state.promisedPropNum);
		}
	}

	/******************************** Learner Code ********************************/

	/**
	 * Subclasses must implement this method.
	 * @param instNum The paxos instance number the payload was accepted as.
	 * @param payload Data that you must parse and handle.
	 */
	public abstract void handlePaxosCommand(int instNum, byte[] payload);

	// Handle decision message. If the instance number is next in order, then we execute the command. If we have already executed the command, then 
	// note that fact and do nothing.
	// Otherwise, we have detected a gap.
	// If we have detected a gap, attempt to fill the gap by issuing a no-op command (thus temporarily acting as the leader.)
	private void handleDecisionMessage(int instNum, byte[] payload) {
		PaxosState state = this.rounds.get(instNum);
		
		// If we didn't know about this state before, make a new state object and store it in our rounds.
		if (state == null) {
			noteOutput("(" + instNum + ") I was unaware of this round, creating state for it");
			state = new PaxosState(instNum, payload, false, Arrays.asList(REPLICA_ADDRS));
			this.rounds.put(instNum, state);
		}
		
		// If we've already executed this command, we can just check for gaps in our execution and then return.
		if (state.executed) {
			noteOutput("(" + instNum + ") Received decision message, but already executed.");
			catchUpExecution();
			checkAndHandleGaps();
			return;
		}
		
		// In this case, we haven't executed the command yet. So make sure we know that it has been decided. Then check
		// for gaps and catch up execution as far as we can (execute as many commands as we can).
		if (!state.decided) {
			state.decidedValue = payload;
			state.decided = true;
			logKnownCommands();
			logKnownStates();
		}
		
		// Execute as much as we can and find any silly gaps.
		catchUpExecution();
		checkAndHandleGaps();
	}
	
	// Checks for a gap in our knowledge of the rounds. A gap is a slot that we don't know about.
	private void checkAndHandleGaps() {
		int maxKnown = this.rounds.lastKey();
		for (int i = 0; i <= maxKnown; i++) {
			if (!this.rounds.containsKey(i) || i > highestExecutedNum) {
				noteOutput("Detected a gap at slot " + i);
				learnCommand(Arrays.asList(REPLICA_ADDRS), i);
			} 
		}
		
	}
	
	// Executes as many known commands as we can.
	private void catchUpExecution() {
		// See if we can execute the next command.
		int instNum = this.highestExecutedNum + 1;
		while (true) {
			PaxosState nextState = this.rounds.get(instNum);
			if (nextState == null || !nextState.decided) {
				return;
			}
			
			// At this point, we have decided the next state but have not yet executed it. So go ahead and do that.
			noteOutput("(" + instNum + ") Executing command in special command eater loop.");
			if (!Arrays.equals(nextState.decidedValue, noopMarker)) {
				handlePaxosCommand(nextState.instNum, nextState.decidedValue);
				nextState.executed = true;
				logKnownCommands();
			} else {
				noteOutput("(" + instNum + ") No-op command completed");
				nextState.executed = true;
				logKnownCommands();
			}
			
			// Update the state for this instance to note that we have executed it.
			// Also update our in-memory record of the highest executed command number.
			this.highestExecutedNum = instNum;
			instNum++;
		}
	}

	// Use this to propose a no-op in order to either put a no-op in place or ferret out the actual 
	// value selected for a round.
	private void learnCommand(List<Integer> addrs, int instNum) {
		if (this.rounds.get(instNum) == null) {
			this.rounds.put(instNum, new PaxosState(instNum, getNextPropNum(0), noopMarker, addrs));
		}
		this.rounds.get(instNum).highestAcceptedNum = -1;
		proposeCommand(addrs, instNum, noopMarker);
	}

	// logs all of the known commands to persistent storage with the put recovery method
	private void logKnownCommands() {
		try {
			if (Utility.fileExists(this, PAXOS_LOG_FILE)) {

				// Get old file contents into string
				PersistentStorageReader reader = getReader(PAXOS_LOG_FILE);

				char[] buf = new char[MAX_FILE_SIZE];
				reader.read(buf, 0, MAX_FILE_SIZE);

				String oldFileData = new String(buf);

				// Put old file contents into temp file
				PersistentStorageWriter writer = this.getWriter(TEMP_PAXOS_LOG_FILE, false);
				writer.write(oldFileData.trim());
				writer.close();

			}
			
			// Write commands data to log file
			PersistentStorageWriter logFile = this.getWriter(PAXOS_LOG_FILE, false);
			String logData = "";
			for (int instNum : this.rounds.keySet()) {
				if (this.rounds.get(instNum).decided) {
					logData += this.rounds.get(instNum).toLogString() + "\n";
				}
			}
			noteError("Logging commands data: " + logData);
			logFile.write(logData.trim());
			logFile.close();

			// Delete temp file
			PersistentStorageWriter writer = this.getWriter(TEMP_PAXOS_LOG_FILE, false);
			writer.delete();
			writer.close();
			
		} catch (Exception e) {
			noteError("***************************");
			noteError("FAILING! (logging known commands)");
			noteError("***************************");
			fail();
		}
	}
	
	// logs all of the known commands to persistent storage with the put recovery method
	private void logKnownStates() {
		try {
			if (Utility.fileExists(this, PAXOS_STATE_FILE)) {

				// Get old file contents into string
				PersistentStorageReader reader = getReader(PAXOS_STATE_FILE);

				char[] buf = new char[MAX_FILE_SIZE];
				reader.read(buf, 0, MAX_FILE_SIZE);

				String oldFileData = new String(buf);

				// Put old file contents into temp file
				PersistentStorageWriter writer = this.getWriter(TEMP_PAXOS_STATE_FILE, false);
				writer.write(oldFileData.trim());
				writer.close();

			}
			
			// Write commands data to log file
			PersistentStorageWriter logFile = this.getWriter(PAXOS_STATE_FILE, false);
			String logData = "";
			for (int instNum : this.rounds.keySet()) {
				logData += this.rounds.get(instNum).toStateLogString().trim() + "\n";
			}
			noteError("Logging state data: " + logData);
			logFile.write(logData.trim());
			logFile.close();

			// Delete temp file
			PersistentStorageWriter writer = this.getWriter(TEMP_PAXOS_STATE_FILE, false);
			writer.delete();
			writer.close();
			
		} catch (Exception e) {
			noteError("***************************");
			noteError("FAILING! (logging state information)");
			noteError("***************************");
			fail();
		}
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
		// Must be a TreeMap to ensure that commands are logged in order.
		this.rounds = new TreeMap<Integer, PaxosState>();
	}

	private void noteError(String output) {
		log(output, System.err, COLOR_ERROR);
	}

	private void noteOutput(String output) {
		log(output, System.out, COLOR_OUTPUT);
	}

	// Recovers the known decided commands and runs the command handler for each until a gap is
	// found.
	// TODO: recover the old log file in the same way that put does
	@Override
	public void start() {
		super.start();
		
		// Recover old log file if necessary
		if (Utility.fileExists(this, TEMP_PAXOS_LOG_FILE)) {
			try {
				PersistentStorageReader reader = this.getReader(TEMP_PAXOS_LOG_FILE);
				if (!reader.ready()) {
					PersistentStorageWriter deleter = this.getWriter(TEMP_PAXOS_LOG_FILE, false);
					deleter.delete();
				} else {
					noteError("Recovery old log file");
					char[] buf = new char[MAX_FILE_SIZE];
					reader.read(buf, 0, MAX_FILE_SIZE);
					PersistentStorageWriter writer = this.getWriter(PAXOS_LOG_FILE, false);
					String oldFile = new String(buf);
					writer.write(oldFile.trim());

					// Delete temp file.
					PersistentStorageWriter deleter = this.getWriter(TEMP_PAXOS_LOG_FILE, false);
					deleter.delete();
				}
			} catch (IOException e) {
				// Fail ourselves and try again
				noteError("Crashed trying to recover old log file");
				noteError("***************************");
				noteError("FAILING!");
				noteError("***************************");
				fail();
			}
		}
		// Recover the decided commands from the log file.
		try {
			if (!Utility.fileExists(this, PAXOS_LOG_FILE)) {
				noteOutput("Log file doesn't yet exist.");
				return;
			}
			
			PersistentStorageReader in = this.getReader(PAXOS_LOG_FILE);

			if (in.ready()) {
				noteError("Recover decisions from log file");
				char[] data = new char[MAX_FILE_SIZE];
				in.read(data);
				String[] commands = new String(data).split("\n");

				for (String s : commands) {
					if (s.trim().length() > 0) {
						PaxosState state = PaxosState.fromLogString(s, Arrays.asList(REPLICA_ADDRS));
						noteOutput("Found round " + state.instNum + " with value: "
							+ Utility.byteArrayToString(state.value));
														
						this.rounds.put(state.instNum, state);
					
						// Record the highest known executed command.
						if (state.executed) {
							noteOutput("Had already executed round " + state.instNum);
							this.highestExecutedNum = state.instNum;
						} 
					}
				}
			}
		} catch (Exception e) {
			noteError("Crashed trying to recover commands from log file");
			e.printStackTrace();
			noteError("***************************");
			noteError("FAILING");
			noteError("***************************");
			fail();
		}
		
		// Recover old state data file if necessary
		if (Utility.fileExists(this, TEMP_PAXOS_STATE_FILE)) {
			try {
				PersistentStorageReader reader = this.getReader(TEMP_PAXOS_STATE_FILE);
				if (!reader.ready()) {
					PersistentStorageWriter deleter = this.getWriter(TEMP_PAXOS_STATE_FILE, false);
					deleter.delete();
				} else {
					noteError("Recovery old log file");
					char[] buf = new char[MAX_FILE_SIZE];
					reader.read(buf, 0, MAX_FILE_SIZE);
					PersistentStorageWriter writer = this.getWriter(PAXOS_STATE_FILE, false);
					String oldFile = new String(buf);
					writer.write(oldFile.trim());

					// Delete temp file.
					PersistentStorageWriter deleter = this.getWriter(TEMP_PAXOS_STATE_FILE, false);
					deleter.delete();
				}
			} catch (IOException e) {
				// Fail ourselves and try again
				noteError("Crashed trying to recover old log file");
				noteError("***************************");
				noteError("FAILING!");
				noteError("***************************");
				fail();
			}
		}
		
		// Recover PaxosState state data from log file.
		try {
			if (!Utility.fileExists(this, PAXOS_STATE_FILE)) {
				noteOutput("State log file doesn't yet exist.");
				return;
			}
			
			PersistentStorageReader in = this.getReader(PAXOS_STATE_FILE);

			if (in.ready()) {
				noteError("Recovering PaxosState information from state log file");
				char[] data = new char[MAX_FILE_SIZE];
				in.read(data);
				String[] states = new String(data).split("\n");

				for (String s : states) {
					if (s.trim().length() > 0) {
						int instNum = PaxosState.getInstNumFromStateLog(s);
						PaxosState state = this.rounds.get(instNum);
						
						if (state == null) {
							state = PaxosState.getNewPaxosStateFromStateLog(s, Arrays.asList(REPLICA_ADDRS));
							this.rounds.put(instNum, state);
						} else {
							state.updateFromStateLogString(s);
						}
						
						noteOutput("Found round " + instNum + " promised prop number: " + state.promisedPropNum
								+ " and accepted prop num: " + state.acceptedPropNum + " and accepted value: "
							+ Utility.byteArrayToString(state.acceptedValue));
					}
				}
			}
			
			
		} catch (Exception e) {
			noteError("Crashed trying to recover commands from log file");
			e.printStackTrace();
			noteError("***************************");
			noteError("FAILING");
			noteError("***************************");
			fail();
		}
		
		// Now that we've read in from the log, see if we need to clean anything up.
		catchUpExecution();
		checkAndHandleGaps();
	}
}