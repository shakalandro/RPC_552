import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.MessageLayer;
import edu.washington.cs.cse490h.lib.Packet;
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
	private static final String COLOR_OUTPUT = "0;3";
	private static final String COLOR_ERROR = "0;4";
	private static final Random r = new Random();

	// State data shared by all roles.
	private Map<Integer, PaxosState> rounds;
	
	private int highestExecutedNum = -1;
	
	protected final static Integer[] REPLICA_ADDRS = {1, 2, 4, 6};
	
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
		if (!this.rounds.get(instNum).decided) {
			int propNum = this.rounds.get(instNum).propNum;
			for (Integer nodeAddr : addrs) {
				PaxosPacket prepare = PaxosPacket.makePrepareMessage(instNum, propNum, payload);
				
				noteOutput("Prepare (" + instNum + "," + propNum + ") sent to " + nodeAddr
							+ " with value: " + (payload != null ? Utility.byteArrayToString(payload) : "no-op"));
				
				RIOSend(nodeAddr, Protocol.PAXOS_PKT, prepare.pack());
			}
			try {
				Method m =
						Callback.getMethod("proposeCommand", this,
								new String[] { List.class.getName(), Integer.class.getName(),
										byte[].class.getName(), Integer.class.getName() });
				Callback retry =
						new Callback(m, this, new Object[] { addrs, instNum, payload,
								backoff * 2 + r.nextInt() % RANDOM_BACKOFF_MAX });
				addTimeout(retry, backoff);
			} catch (Exception e) {
				noteError("(" + instNum + ") Failed to make callback for proposal retry.");
				e.printStackTrace();
				fail();
			}
		} else {
			noteOutput("(" + instNum + ") Paxos round already decided, no need to propose");
		}
	}

	private void handlePromiseResponse(int from, int instNum, int highestAccept, byte[] payload) {
		PaxosState state = this.rounds.get(instNum);
		state.promised.add(from);
		if (highestAccept > state.highestAcceptedNum) {
			noteOutput("(" + instNum + ") promise came with a higher acceptance value");
			state.setHighest(highestAccept, payload);
		}
		if (state.quorumPromised()) {
			noteOutput("(" + instNum + ") quorum promised");
			for (Integer nodeAddr : state.promised) {
				PaxosPacket accept =
						PaxosPacket.makeAcceptMessage(instNum, state.propNum,
								state.highestAcceptedValue);
				noteOutput("Accept request (" + instNum + "," + state.propNum + ") sent to "
						+ nodeAddr + " with value: " + Utility.byteArrayToString(payload));
				RIOSend(nodeAddr, Protocol.PAXOS_PKT, accept.pack());
			}
		}
	}

	private void handleAcceptResponse(int from, int instNum, int n, byte[] payload) {
		PaxosState state = this.rounds.get(instNum);
		state.accepted.add(from);
		if (state.quorumAccepted()) {
			noteOutput("(" + instNum + ") quorum accepted value: "
					+ Utility.byteArrayToString(payload));
			for (Integer nodeAddr : state.participants) {
				PaxosPacket decision = PaxosPacket.makeDecisionMessage(instNum, n, payload);
				noteOutput("(" + instNum + ") sending decision to " + nodeAddr);
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
			noteOutput("(" + instNum + ") Prepare request for unknown round, creating state for it");
			state = new PaxosState(instNum, n, payload);
			this.rounds.put(instNum, state);
		}
		if (n > state.promisedPropNum) {
			state.promisedPropNum = n;
			PaxosPacket promise =
					PaxosPacket.makePromiseMessage(state.instNum, state.acceptedPropNum,
							state.acceptedValue);
			noteOutput("(" + instNum + ") promise not to accept lower than " + n);
			RIOSend(from, Protocol.PAXOS_PKT, promise.pack());
		} else {
			noteOutput("(" + instNum + ") ignoring prepare request, I already promised higher");
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
			noteOutput("(" + instNum + ") Accepted");
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

	// Handle decision message. If the instance number is next in order, then we execute the command. Otherwise, we have detected a gap.
	// If we have detected a gap, attempt to fill the gap by issuing a no-op command (thus temporarily acting as the leader.)
	private void handleDecisionMessage(int instNum, byte[] payload) {
		PaxosState state = this.rounds.get(instNum);
		
		// If we didn't know about this state before, make a new state object and store it in our rounds.
		if (state == null) {
			noteOutput("(" + instNum + ") I was unaware of this round, creating state for it");
			state = new PaxosState(instNum, payload, false);
			this.rounds.put(instNum, state);
		}
		
		state.value = payload;
		state.decided = true;
		
		// If the command is next in line to be executed, then execute it. Otherwise, we have detected a gap.
		// If we detected a gap, go ahead and propose no-ops for those missing slots.
		if (instNum == highestExecutedNum + 1) {
			noteOutput("(" + instNum + ") Executing command");
			if (state.value != null) {
				handlePaxosCommand(state.instNum, state.value);
			} else {
				noteOutput("(" + instNum + ") No-op command completed");
			}
			
			// Update the state for this instance to note that we have executed it.
			// Also update our in-memory record of the highest executed command number.
			state.executed = true;
			this.highestExecutedNum = instNum;
		
		// We have detected a gap in the commands. Get all instance numbers that we have detected gaps for and 
		// propose no-ops for those slots.
		} else {
			noteOutput("(" + instNum + ") We have detected a gap");
			int gapLength = instNum - this.highestExecutedNum - 1;
			for (int i = 0; i < gapLength; i++) {
				List<Integer> addrs = Arrays.asList(REPLICA_ADDRS);
				learnCommand(addrs, highestExecutedNum + i);
			}
		}
		
		// Update our log to reflect the arrival of this decision message.
		logKnownCommands();
	}

	// Use this to propose a no-op in order to either put a no-op in place or ferret out the actual 
	// value selected for a round.
	private void learnCommand(List<Integer> addrs, int instNum) {
		proposeCommand(addrs, instNum, null);
	}

	// logs all of the known commands to persistent storage with the put recovery method
	private void logKnownCommands() {
		try {
			// Get old file contents into string
			PersistentStorageReader reader = getReader(PAXOS_LOG_FILE);
			char[] buf = new char[MAX_FILE_SIZE];
			reader.read(buf, 0, MAX_FILE_SIZE);
			String oldFileData = new String(buf);

			// Put old file contents into temp file
			PersistentStorageWriter writer = this.getWriter(TEMP_PAXOS_LOG_FILE, false);
			writer.write(oldFileData);
			writer.close();

			// Write commands data to log file
			PersistentStorageWriter logFile = this.getWriter(PAXOS_LOG_FILE, false);
			String logData = "";
			for (int instNum : this.rounds.keySet()) {
				logData += this.rounds.get(instNum).toLogString() + "\n";
			}
			noteError("Logging commands data: " + logData);
			logFile.write(logData.trim());
			logFile.close();

			// Delete temp file
			writer = this.getWriter(TEMP_PAXOS_LOG_FILE, false);
			writer.delete();
			writer.close();
		} catch (Exception e) {
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
					writer.write(buf);

					// Delete temp file.
					PersistentStorageWriter deleter = this.getWriter(PAXOS_LOG_FILE, false);
					deleter.delete();
				}
			} catch (IOException e) {
				// Fail ourselves and try again
				noteError("Crashed trying to recover old log file");
				fail();
			}
		}
		// Recover the decided commands from the log file.
		try {
			PersistentStorageReader in = this.getReader(PAXOS_LOG_FILE);
			if (!in.ready()) {
				noteError("Recover decisions from log file");
				char[] data = new char[MAX_FILE_SIZE];
				in.read(data);
				String[] commands = new String(data).split("\n");

				for (String s : commands) {
					PaxosState state = PaxosState.fromLogString(s);
					noteError("Found round " + state.instNum + " with value: "
							+ Utility.byteArrayToString(state.value));

					state.decided = true;
					this.rounds.put(state.instNum, state);
					
					// Record the highest known executed command.
					// If we haven't executed this command, but it is right after a command we have executed, then execute it.
					if (state.executed) {
						this.highestExecutedNum = state.instNum;
					} else if (state.instNum == this.highestExecutedNum + 1) {
						handlePaxosCommand(state.instNum, state.value);
						state.executed = true;
						this.highestExecutedNum = state.instNum;
						
						// Log the fact that we have executed this command.
						logKnownCommands();
					}
					
//					if (state.instNum != lastInstNum + 1) {
//						gapFound = true;
//					}
//
//					// If a gap has been found, then we haven't executed this command (or any following commands.)
//					// Record this fact in the state.
//					if (!gapFound) {
//						//noteError("Sending " + state.instNum + " to client");
//						//handlePaxosCommand(state.instNum, state.value);
//						this.highestExecutedNum = state.instNum;
//					} else {
//						noteError("Gap detected during recovery");
//					}
//					lastInstNum = state.instNum;
				}
			}
		} catch (Exception e) {
			noteError("Crashed trying to recover commands from log file");
			fail();
		}
	}
}