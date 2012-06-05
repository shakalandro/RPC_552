import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;

import javax.print.attribute.standard.PrinterResolution;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.Utility;

/**
 * Layer above the basic messaging layer that provides reliable, in-order delivery in the absence of
 * faults. This layer does not provide much more than the above.
 * 
 * At a minimum, the student should extend/modify this layer to provide reliable, in-order message
 * delivery, even in the presence of node failures.
 */
public class ReliableInOrderMsgLayer {
	public static int TIMEOUT = 3;

	private HashMap<Integer, InChannel> inConnections;
	private HashMap<Integer, OutChannel> outConnections;
	private RIONode n;

	// Map from node addr to sessionId with that node.
	private Map<Integer, Integer> sessionIds;
	
	private Random rand;

	// The packet number to start at on each channel.
	public static final int START_SEQUENCE_NUM = -1;

	/**
	 * Constructor.
	 * 
	 * @param destAddr The address of the destination host
	 * @param msg The message that was sent
	 * @param timeSent The time that the ping was sent
	 */
	public ReliableInOrderMsgLayer(RIONode n) {
		inConnections = new HashMap<Integer, InChannel>();
		outConnections = new HashMap<Integer, OutChannel>();
		this.n = n;
		this.rand = new Random();

		sessionIds = new HashMap<Integer, Integer>();
	}

	/**
	 * Receive a sync packet.
	 */
	public void RIOSyncReceive(int from, byte[] msg) {				
		// If we don't have a relationship with the node that wants to sync with us, we will start one, placing them in our
		// map.
		int syncId = Integer.parseInt(Utility.byteArrayToString(msg));
		if (!sessionIds.containsKey(from)) {
			sessionIds.put(from, syncId);
			return;
		}
		
		// If we do have them in our map, we need to check if they match. If they don't match, then we'll clear out our current
		// communication state with the sender and set the sessionId correctly.
		int currentSessionId = sessionIds.get(from);
		if (currentSessionId != syncId) {
			clearCommunicationState(from);
			sessionIds.put(from, syncId);
		}
	}
	
	// Clears the communication slate with given node. This involves resetting the sequence numbers. Un-acked packets
	// are kept around so that we keep on sending them.
	private void clearCommunicationState(int nodeAddr) {
		InChannel in = new InChannel();	
		OutChannel out = new OutChannel(this, nodeAddr);
		inConnections.put(nodeAddr, in);
		outConnections.put(nodeAddr, out);
	}

	/**
	 * Receive a data packet.
	 * 
	 * @param from The address from which the data packet came.
	 * @param pkt The Packet of data.
	 */
	public void RIODataReceive(int from, byte[] msg) {
		RIOPacket riopkt = RIOPacket.unpack(msg);
		if (riopkt == null) {
			System.out.println("RIOPKT is null.");
		}

		// If we don't have an alive connection with them, or their sessionId doesn't match the one we have,
		// just ignore this packet and send out a SYNC packet to try and get synced up.
		if (!sessionIds.containsKey(from) || sessionIds.get(from) != riopkt.getSessionId()) {
			int newSessionId = rand.nextInt();
			sessionIds.put(from, newSessionId);
			
			n.send(from, Protocol.SYNC, Utility.stringToByteArray(String.valueOf(newSessionId)));
			return;
		}

		// Verify that we received this sent node.
		byte[] seqNumByteArray = Utility.stringToByteArray("" + riopkt.getSeqNum());
		n.send(from, Protocol.ACK, seqNumByteArray);

		InChannel in = inConnections.get(from);
		if (in == null) {
			in = new InChannel();
			inConnections.put(from, in);
		}

		LinkedList<RIOPacket> toBeDelivered = in.gotPacket(riopkt);
		for (RIOPacket p : toBeDelivered) {
			// deliver in-order the next sequence of packets
			n.onRIOReceive(from, p.getProtocol(), p.getPayload());
		}
	}

	/**
	 * Receive an acknowledgment packet.
	 * 
	 * @param from The address from which the data packet came
	 * @param pkt The Packet of data
	 */
	public void RIOAckReceive(int from, byte[] msg) {
		int seqNum = Integer.parseInt(Utility.byteArrayToString(msg));
		if (outConnections.containsKey(from)) {
			outConnections.get(from).gotACK(seqNum);
		}
	}

	/**
	 * Send a packet using this reliable, in-order messaging layer. Note that this method does not
	 * include a reliable, in-order broadcast mechanism.
	 * 
	 * @param destAddr The address of the destination for this packet
	 * @param protocol The protocol identifier for the packet
	 * @param payload The payload to be sent
	 */
	public void RIOSend(int destAddr, int protocol, byte[] payload) {
		// If we don't have an alive connection with them, just ignore this packet and send out a
		// SYNC packet to try and get synced up.
		if (!sessionIds.containsKey(destAddr)) {
			int newSessionId = rand.nextInt();
			sessionIds.put(destAddr, newSessionId);
			
			n.send(destAddr, Protocol.SYNC, Utility.stringToByteArray(String.valueOf(newSessionId)));
			return;
		}

		OutChannel out = outConnections.get(destAddr);
		if (out == null) {
			out = new OutChannel(this, destAddr);
			outConnections.put(destAddr, out);
		}

		out.sendRIOPacket(n, protocol, sessionIds.get(destAddr), payload);
	}

	/**
	 * Callback for timeouts while waiting for an ACK.
	 * 
	 * This method is here and not in OutChannel because OutChannel is not a public class.
	 * 
	 * @param destAddr The receiving node of the unACKed packet
	 * @param seqNum The sequence number of the unACKed packet
	 */
	public void onTimeout(Integer destAddr, Integer seqNum) {
		outConnections.get(destAddr).onTimeout(n, seqNum);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (Integer i : inConnections.keySet()) {
			sb.append(inConnections.get(i).toString() + "\n");
		}

		return sb.toString();
	}
}

/**
 * Representation of an incoming channel to this node
 */
class InChannel {
	public int lastSeqNumDelivered;
	public HashMap<Integer, RIOPacket> outOfOrderMsgs;

	InChannel() {
		lastSeqNumDelivered = ReliableInOrderMsgLayer.START_SEQUENCE_NUM;
		outOfOrderMsgs = new HashMap<Integer, RIOPacket>();
	}

	/**
	 * Method called whenever we receive a data packet.
	 * 
	 * @param pkt The packet
	 * @return A list of the packets that we can now deliver due to the receipt of this packet
	 */
	public LinkedList<RIOPacket> gotPacket(RIOPacket pkt) {
		LinkedList<RIOPacket> pktsToBeDelivered = new LinkedList<RIOPacket>();
		int seqNum = pkt.getSeqNum();

		if (seqNum == lastSeqNumDelivered + 1) {
			// We were waiting for this packet
			pktsToBeDelivered.add(pkt);
			++lastSeqNumDelivered;
			deliverSequence(pktsToBeDelivered);
		} else if (seqNum > lastSeqNumDelivered + 1) {
			// We received a subsequent packet and should store it
			outOfOrderMsgs.put(seqNum, pkt);
		}
		// Duplicate packets are ignored

		return pktsToBeDelivered;
	}

	/**
	 * Helper method to grab all the packets we can now deliver.
	 * 
	 * @param pktsToBeDelivered List to append to
	 */
	private void deliverSequence(LinkedList<RIOPacket> pktsToBeDelivered) {
		while (outOfOrderMsgs.containsKey(lastSeqNumDelivered + 1)) {
			++lastSeqNumDelivered;
			pktsToBeDelivered.add(outOfOrderMsgs.remove(lastSeqNumDelivered));
		}
	}

	@Override
	public String toString() {
		return "last delivered: " + lastSeqNumDelivered + ", outstanding: " + outOfOrderMsgs.size();
	}
}

/**
 * Representation of an outgoing channel to this node
 */
class OutChannel {
	public HashMap<Integer, RIOPacket> unACKedPackets;
	public int lastSeqNumSent;
	private ReliableInOrderMsgLayer parent;
	private int destAddr;

	OutChannel(ReliableInOrderMsgLayer parent, int destAddr) {
		lastSeqNumSent = ReliableInOrderMsgLayer.START_SEQUENCE_NUM;
		unACKedPackets = new HashMap<Integer, RIOPacket>();
		this.parent = parent;
		this.destAddr = destAddr;
	}

	/**
	 * Send a new RIOPacket out on this channel.
	 * 
	 * @param n The sender and parent of this channel
	 * @param protocol The protocol identifier of this packet
	 * @param payload The payload to be sent
	 */
	protected void sendRIOPacket(RIONode n, int protocol, int sessionId, byte[] payload) {
		try {
			Method onTimeoutMethod =
					Callback.getMethod("onTimeout", parent, new String[] { "java.lang.Integer",
							"java.lang.Integer" });
			RIOPacket newPkt = new RIOPacket(protocol, ++lastSeqNumSent, sessionId, payload);
			unACKedPackets.put(lastSeqNumSent, newPkt);

			n.send(destAddr, Protocol.DATA, newPkt.pack());
			n.addTimeout(new Callback(onTimeoutMethod, parent, new Object[] { destAddr,
					lastSeqNumSent }), ReliableInOrderMsgLayer.TIMEOUT);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Called when a timeout for this channel triggers
	 * 
	 * @param n The sender and parent of this channel
	 * @param seqNum The sequence number of the unACKed packet
	 */
	public void onTimeout(RIONode n, Integer seqNum) {
		if (unACKedPackets.containsKey(seqNum)) {
			resendRIOPacket(n, seqNum);
		}
	}

	/**
	 * Called when we get an ACK back. Removes the outstanding packet if it is still in
	 * unACKedPackets.
	 * 
	 * @param seqNum The sequence number that was just ACKed
	 */
	protected void gotACK(int seqNum) {
		unACKedPackets.remove(seqNum);
	}

	/**
	 * Resend an unACKed packet.
	 * 
	 * @param n The sender and parent of this channel
	 * @param seqNum The sequence number of the unACKed packet
	 */
	private void resendRIOPacket(RIONode n, int seqNum) {
		try {
			Method onTimeoutMethod =
					Callback.getMethod("onTimeout", parent, new String[] { "java.lang.Integer",
							"java.lang.Integer" });
			RIOPacket riopkt = unACKedPackets.get(seqNum);

			n.send(destAddr, Protocol.DATA, riopkt.pack());
			n.addTimeout(new Callback(onTimeoutMethod, parent, new Object[] { destAddr, seqNum }),
					ReliableInOrderMsgLayer.TIMEOUT);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
