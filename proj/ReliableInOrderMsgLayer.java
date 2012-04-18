import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

/**
 * Layer above the basic messaging layer that provides reliable, in-order
 * delivery in the absence of faults. This layer does not provide much more than
 * the above. At a minimum, the student should extend/modify this layer to
 * provide reliable, in-order message delivery, even in the presence of node
 * failures.
 */
public class ReliableInOrderMsgLayer {
    public static int TIMEOUT = 3;

    private HashMap<Integer, InChannel> inConnections;
    private HashMap<Integer, OutChannel> outConnections;
    private RIONode n;

    public static boolean debugging = false;

    /**
     * Constructor.
     * 
     * @param destAddr
     *            The address of the destination host
     * @param msg
     *            The message that was sent
     * @param timeSent
     *            The time that the ping was sent
     */
    public ReliableInOrderMsgLayer(RIONode n) {
        inConnections = new HashMap<Integer, InChannel>();
        outConnections = new HashMap<Integer, OutChannel>();
        this.n = n;

    }

    /**
     * Receive a data packet.
     * 
     * @param from
     *            The address from which the data packet came
     * @param pkt
     *            The Packet of data
     */
    public void RIODataReceive(int from, byte[] msg) {
        RIOPacket riopkt = RIOPacket.unpack(msg);

        // at-most-once semantics
        byte[] seqNumByteArray = Utility.stringToByteArray(""
                + riopkt.getSeqNum());
        n.send(from, Protocol.ACK, seqNumByteArray);

        InChannel in = inConnections.get(from);
        if (in == null) {
            in = new InChannel(n, from);
            inConnections.put(from, in);
        }

        LinkedList<RIOPacket> toBeDelivered = in.gotPacket(riopkt);
        for (RIOPacket p : toBeDelivered) {
            // set the last delivered sequence number
            in.logSequenceNumber(p.getSeqNum());
            n.onRIOReceive(from, p.getProtocol(), p.getPayload());
        }
    }

    /**
     * Receive an acknowledgment packet.
     * 
     * @param from
     *            The address from which the data packet came
     * @param pkt
     *            The Packet of data
     */
    public void RIOAckReceive(int from, byte[] msg) {
        int seqNum = Integer.parseInt(Utility.byteArrayToString(msg));
        if (outConnections.containsKey(from)) {
            OutChannel out = outConnections.get(from);
            out.logSequenceNumber(seqNum);
            out.gotACK(seqNum);
        }
    }

    /**
     * Send a packet using this reliable, in-order messaging layer. Note that
     * this method does not include a reliable, in-order broadcast mechanism.
     * 
     * @param destAddr
     *            The address of the destination for this packet
     * @param protocol
     *            The protocol identifier for the packet
     * @param payload
     *            The payload to be sent
     */
    public void RIOSend(int destAddr, int protocol, byte[] payload) {
        OutChannel out = outConnections.get(destAddr);
        if (out == null) {
            out = new OutChannel(this, n, destAddr);
            outConnections.put(destAddr, out);
        }

        out.sendRIOPacket(protocol, payload);
    }

    /**
     * Callback for timeouts while waiting for an ACK. This method is here and
     * not in OutChannel because OutChannel is not a public class.
     * 
     * @param destAddr
     *            The receiving node of the unACKed packet
     * @param seqNum
     *            The sequence number of the unACKed packet
     */
    public void onTimeout(Integer destAddr, Integer seqNum) {
        outConnections.get(destAddr).onTimeout(seqNum);
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

abstract class Channel {
    public String log_file;
    public String temp_log_file;
    public RIONode n;

    public int lastSeqNum;

    // Recover from a failed log write if necessary else start sequence numbers
    // at 0
    public void setSequenceNumber() {
        debugPrint("Attempting to assign sequence number");
        lastSeqNum = -1;

        // recover temp log file if necessary
        if (Utility.fileExists(n, temp_log_file)) {
            debugPrint("Temp file " + temp_log_file + " existed");

            // Fetch value in temp file
            int oldVal = getSequenceNumFromFile(temp_log_file);

            if (oldVal != -1) {
                // Write to regular log
                if (writeCurrentSequence(log_file, oldVal)) {
                    debugPrint("Successfully wrote old temp seq num: " + oldVal
                            + " to " + log_file);
                }
            }
            // delete temp file
            deleteFile(temp_log_file);
        }

        // Recover sequence number from log if necessary
        if (Utility.fileExists(n, log_file)) {
            debugPrint("Log file " + log_file + " existed");
            lastSeqNum = getSequenceNumFromFile(log_file);
        } else {
            debugPrint("Log file " + log_file + " did not exist");
        }
    }

    // Log a sequence number
    public void logSequenceNumber(int seqNum) {
        debugPrint("Attempting to write seq num: " + seqNum + " to file: "
                + log_file);

        // Flag for whether we'll need to clean up a temp file
        boolean createdTemp = false;

        // Fetch old sequence num from log_file if it exists
        if (Utility.fileExists(n, log_file)) {
            int oldValue = getSequenceNumFromFile(log_file);
            if (oldValue != -1) {
                debugPrint("Successfully read old seq num: " + oldValue
                        + " from " + log_file);

                // Put old file contents into temp file
                if (writeCurrentSequence(temp_log_file, oldValue)) {
                    debugPrint("Successfully wrote seq num: " + seqNum + " to "
                            + temp_log_file);
                    createdTemp = true;
                }
            }
        }

        // Write new sequence number to log file
        if (writeCurrentSequence(log_file, seqNum)) {
            debugPrint("Successfully wrote seq num: " + seqNum + " to "
                    + log_file);
        }

        // Delete temp file if necessary
        if (createdTemp) {
            deleteFile(temp_log_file);
        }
    }

    private int getSequenceNumFromFile(String filename) {
        try {
            PersistentStorageReader reader = n.getReader(filename);
            if (reader.ready()) {
                int num = Integer.parseInt(reader.readLine().trim());
                reader.close();
                return num;
            }
            reader.close();
        } catch (IOException e) {
            // We should never get here
            debugPrint("Could not read log file (" + filename
                    + "), but it should exist");
            e.printStackTrace();
        } catch (NumberFormatException e) {
            // Reaching this means we failed to write to the log
            debugPrint("Could not parse sequence number (" + filename + ")");
            e.printStackTrace();
        }
        return -1;
    }

    private boolean writeCurrentSequence(String filename, int sequence) {
        // Write new contents to file
        try {
            PersistentStorageWriter writer = n.getWriter(filename, false);
            writer.write("" + sequence);
            writer.close();
            return true;
        } catch (IOException e) {
            debugPrint("Failed to log sequence number " + sequence + " to "
                    + filename);
            e.printStackTrace();
        }
        return false;
    }

    private void deleteFile(String filename) {
        try {
            PersistentStorageWriter writer = n.getWriter(filename, false);
            writer.delete();
            writer.close();
        } catch (IOException e) {
            debugPrint("Failed to delete file " + filename);
            e.printStackTrace();
        }
    }

    private void debugPrint(String msg) {
        if (ReliableInOrderMsgLayer.debugging) {
            System.out.println("Node " + n.addr + ": " + msg);
        }
    }
}

/**
 * Representation of an incoming channel to this node
 */
class InChannel extends Channel {
	public static String IN_LOG_FILE = ".rio_in_log";
    public static String IN_TEMP_LOG_FILE = IN_LOG_FILE + "_temp";

    private HashMap<Integer, RIOPacket> outOfOrderMsgs;

    public InChannel(RIONode n, int sender_addr) {
        outOfOrderMsgs = new HashMap<Integer, RIOPacket>();
        this.n = n;
        this.log_file = IN_LOG_FILE + sender_addr;
        this.temp_log_file = IN_TEMP_LOG_FILE + sender_addr;
        setSequenceNumber();
    }

    /**
     * Method called whenever we receive a data packet.
     * 
     * @param pkt
     *            The packet
     * @return A list of the packets that we can now deliver due to the receipt
     *         of this packet
     */
    public LinkedList<RIOPacket> gotPacket(RIOPacket pkt) {

        LinkedList<RIOPacket> pktsToBeDelivered = new LinkedList<RIOPacket>();
        int seqNum = pkt.getSeqNum();

        if (seqNum == lastSeqNum + 1) {
            // We were waiting for this packet
            pktsToBeDelivered.add(pkt);
            ++lastSeqNum;
            deliverSequence(pktsToBeDelivered);
        } else if (seqNum > lastSeqNum + 1) {
            // We received a subsequent packet and should store it
            outOfOrderMsgs.put(seqNum, pkt);
        }

        // Duplicate packets are ignored
        return pktsToBeDelivered;
    }

    /**
     * Helper method to grab all the packets we can now deliver.
     * 
     * @param pktsToBeDelivered
     *            List to append to
     */
    private void deliverSequence(LinkedList<RIOPacket> pktsToBeDelivered) {
        while (outOfOrderMsgs.containsKey(lastSeqNum + 1)) {
            ++lastSeqNum;
            pktsToBeDelivered.add(outOfOrderMsgs.remove(lastSeqNum));
        }
    }

    @Override
    public String toString() {
        return "last delivered: " + lastSeqNum + ", outstanding: "
                + outOfOrderMsgs.size();
    }
}

/**
 * Representation of an outgoing channel to this node
 */
class OutChannel extends Channel {
	public static String OUT_LOG_FILE = ".rio_out_log";
    public static String OUT_TEMP_LOG_FILE = OUT_LOG_FILE + "_temp";

    private static final int MAX_SEND_ATTEMPTS = Integer.MAX_VALUE - 1;

    private HashMap<Integer, RIOPacket> unACKedPackets;
    private HashMap<Integer, Integer> attempts;
    private ReliableInOrderMsgLayer parent;
    private int destAddr;

    public OutChannel(ReliableInOrderMsgLayer parent, RIONode n, int destAddr) {
        unACKedPackets = new HashMap<Integer, RIOPacket>();
        attempts = new HashMap<Integer, Integer>();
        this.parent = parent;
        this.destAddr = destAddr;
        this.n = n;
        this.log_file = OUT_LOG_FILE + destAddr;
        this.temp_log_file = OUT_TEMP_LOG_FILE + destAddr;
        setSequenceNumber();
    }

    /**
     * Send a new RIOPacket out on this channel.
     * 
     * @param n
     *            The sender and parent of this channel
     * @param protocol
     *            The protocol identifier of this packet
     * @param payload
     *            The payload to be sent
     */
    protected void sendRIOPacket(int protocol, byte[] payload) {
        try {
            Method onTimeoutMethod = Callback.getMethod("onTimeout", parent,
                    new String[] { "java.lang.Integer", "java.lang.Integer" });
            RIOPacket newPkt = new RIOPacket(protocol, ++lastSeqNum, payload);
            unACKedPackets.put(lastSeqNum, newPkt);

            n.send(destAddr, Protocol.DATA, newPkt.pack());
            attempts.put(newPkt.getSeqNum(), 1);
            n.addTimeout(new Callback(onTimeoutMethod, parent, new Object[] {
                    destAddr, lastSeqNum }), ReliableInOrderMsgLayer.TIMEOUT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Called when a timeout for this channel triggers
     * 
     * @param n
     *            The sender and parent of this channel
     * @param seqNum
     *            The sequence number of the unACKed packet
     */
    public void onTimeout(Integer seqNum) {
        if (unACKedPackets.containsKey(seqNum)) {
            resendRIOPacket(seqNum);
        }
    }

    /**
     * Called when we get an ACK back. Removes the outstanding packet if it is
     * still in unACKedPackets.
     * 
     * @param seqNum
     *            The sequence number that was just ACKed
     */
    protected void gotACK(int seqNum) {
        unACKedPackets.remove(seqNum);
        attempts.remove(seqNum);
    }

    /**
     * Resend an unACKed packet.
     * 
     * @param n
     *            The sender and parent of this channel
     * @param seqNum
     *            The sequence number of the unACKed packet
     */
    private void resendRIOPacket(int seqNum) {
        try {
            if (attempts.containsKey(seqNum)
                    && attempts.get(seqNum) < MAX_SEND_ATTEMPTS) {
                Method onTimeoutMethod = Callback.getMethod("onTimeout",
                        parent, new String[] { "java.lang.Integer",
                                "java.lang.Integer" });
                RIOPacket riopkt = unACKedPackets.get(seqNum);

                n.send(destAddr, Protocol.DATA, riopkt.pack());
                n.addTimeout(new Callback(onTimeoutMethod, parent,
                        new Object[] { destAddr, seqNum }),
                        ReliableInOrderMsgLayer.TIMEOUT);
            } else {
                System.err.println("Node " + n.addr
                        + ": Reached max send attempts for packet " + seqNum);
                attempts.remove(seqNum);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
