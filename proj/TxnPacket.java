import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import edu.washington.cs.cse490h.lib.Utility;

/**
 * This conveys the header for an RPC request packet. This is carried in the
 * payload of a RPCPacket, and in turn the data being transferred is carried in
 * the payload of the RPCPacket packet. (And the RPCPacket is carried as the
 * payload of a RIOPacket, etc).
 */
public class TxnPacket {

    public static final int MAX_PACKET_SIZE = Math.min(RPCRequestPacket.MAX_PAYLOAD_SIZE,
    													RPCResultPacket.MAX_PAYLOAD_SIZE);
    public static final String NO_PARTICIPANTS_MARKER = "-";

    private UUID txnID;
    private TxnProtocol protocol;
    private Set<Integer> participants;
    private String request;
    private String payload;
    
    public static TxnPacket getDecisionRequestPacket(TransactionNode node, UUID txnID) {
    	return TxnPacket.getPacket(node, txnID, TxnProtocol.TXN_DECISION_REQ, null, "", "");
    }
    
    public static TxnPacket getAbortPacket(TransactionNode node, UUID txnID) {
    	return TxnPacket.getPacket(node, txnID, TxnProtocol.TXN_ABORT, null, "", "");
    }
    
    public static TxnPacket getCommitPacket(TransactionNode node, UUID txnID,
    		String request, String payload) {
    	return TxnPacket.getPacket(node, txnID, TxnProtocol.TXN_COMMIT, null, request, payload);
    }

    public static TxnPacket getAcceptPacket(TransactionNode node, UUID txnID) {
    	return TxnPacket.getPacket(node, txnID, TxnProtocol.TXN_ACCEPT, null, "", "");
    }
    
    public static TxnPacket getRejectPacket(TransactionNode node, UUID txnID, String message) {
    	return TxnPacket.getPacket(node, txnID, TxnProtocol.TXN_REJECT, null, "", "");
    }
    
    public static TxnPacket getPropositionPacket(TransactionNode node, UUID txnID,
    		Set<Integer> participants, String request, String payload) {
    	// Basic validation since request will be used in a method identifier
    	if (!request.matches("[a-zA-Z0-9_]+")) {
    		System.err.println("Invalid transaction request.");
            node.fail();
            return null;
    	}
    	return TxnPacket.getPacket(node, txnID, TxnProtocol.TXN_PROP, participants, request, payload);
    }
    
    private static TxnPacket getPacket(TransactionNode node, UUID txnID, TxnProtocol protocol,
    		Set<Integer> participants, String request, String payload) {
    	TxnPacket result =  new TxnPacket(txnID, protocol, participants, request, payload);
        if (!result.validSizePayload()) {
        	System.err.println("Invalid payload size in TxnPacket");
            node.fail();
            return null;
        }
    	return result;
    }

    private TxnPacket(UUID txnID, TxnProtocol protocol, Set<Integer> participants,
    		String request, String payload) {
        this.txnID = txnID;
        this.protocol = protocol;
        this.participants = participants;
        this.request = request;
        this.payload = payload;
    }

    public boolean validSizePayload() {
        return this.pack().length < MAX_PACKET_SIZE;
    }
    
    public UUID getID() {
    	return this.txnID;
    }
    
    public TxnProtocol getProtocol() {
    	return this.protocol;
    }
    
    public Set<Integer> getParticipants() {
    	return this.participants;
    }

    /**
     * @return The request command type
     */
    public String getRequest() {
        return this.request;
    }

    /**
     * @return The payload
     */
    public String getPayload() {
        return this.payload;
    }

    /**
     * Convert the TxnPacket object into a byte array for sending over
     * the wire.
     * 
     * @return A byte[] for transporting over the wire. Null if failed to pack
     *         for some reason
     */
    public byte[] pack() {
    	try {
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(byteStream);
            out.write(txnID.toString().getBytes());
            out.writeInt(protocol.ordinal());
            String addrsStr = NO_PARTICIPANTS_MARKER;
            if (participants != null && participants.size() > 0) {
            	addrsStr = TxnPacket.addrListStr(participants);
            }
            out.write(Utility.stringToByteArray(addrsStr + " " + request + " " + payload));

            out.flush();
            out.close();
            return byteStream.toByteArray();
        } catch (IOException e) {
            return null;
        }
    }
    
    /**
     * Unpacks a Byte array to create a RPCRequestPacket object. Assumes the
     * array has been formatted using pack method in RPCRequestPacket.
     * 
     * NOTE: Exists because reflection is stupid.
     * 
     * @param packet
     *            String representation of the transport packet
     * @return TxnPacket object created or null if the byte[] representation was
     *         corrupted
     */
    public static TxnPacket unpack(Byte[] data) {
    	byte[] newData = new byte[data.length];
    	for (int i = 0; i < data.length; i++) {
    		newData[i] = data[i];
    	}
    	return unpack(newData);
    }
    
    /**
     * Unpacks a byte array to create a RPCRequestPacket object. Assumes the
     * array has been formatted using pack method in RPCRequestPacket
     * 
     * @param packet
     *            String representation of the transport packet
     * @return TxnPacket object created or null if the byte[] representation was
     *         corrupted
     */
    public static TxnPacket unpack(byte[] data) {
    	try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));

            byte[] uuidBytes = new byte[36];
            for (int i = 0; i < uuidBytes.length; i++) {
            	uuidBytes[i] = in.readByte();
            }
            String uuidStr = new String(uuidBytes);
            UUID txnID = UUID.fromString(uuidStr);
            TxnProtocol protocol = TxnProtocol.values()[in.readInt()];
            
            byte[] theRestBytes = new byte[in.available()];
            in.readFully(theRestBytes);
            String theRest = Utility.byteArrayToString(theRestBytes);
            String[] parts = theRest.split(" ", 3);
            
            Set<Integer> addrs = null;
            if (!parts[0].equals(NO_PARTICIPANTS_MARKER)) {
				addrs = new HashSet<Integer>();
				String[] addrStrs = parts[0].split(",");
				for (String addr : addrStrs) {
					addrs.add(Integer.parseInt(addr));
				}
            }
            String request = parts[1];
            String args = parts[2];
            
            return new TxnPacket(txnID, protocol, addrs, request, args);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * String representation of a TxnPacket
     */
    public String toString() {
        return request + ": " + txnID + " " + protocol;
    }
    
    public static String addrListStr(Set<Integer> addrs) {
    	String addrList = "";
		boolean first = true;
		for (Integer addr : addrs) {
			if (first) {
				addrList += addr;
				first = false;
			} else {
				addrList += "," + addr;
			}
		}
		return addrList;
    }
}