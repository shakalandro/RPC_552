import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import edu.washington.cs.cse490h.lib.Utility;

/**
 * This conveys the header for an RPC request packet. This is carried in the
 * payload of a RPCPacket, and in turn the data being transferred is carried in
 * the payload of the RPCPacket packet. (And the RPCPacket is carried as the
 * payload of a RIOPacket, etc).
 */
public class TxnPacket {

    public static final int MAX_PACKET_SIZE = RIOPacket.MAX_PAYLOAD_SIZE;
    public static final int HEADER_SIZE = 9;
    public static final int MAX_PAYLOAD_SIZE = MAX_PACKET_SIZE - HEADER_SIZE;

    private int protocol;
    private Command c;
    private byte[] payload;
    
    private static enum TxnProtocol {
    	TXN_PROP,
    	TXN_ABORT,
    	TXN_ACCEPT,
    	TXN_COMMIT
    }

    // Builds a packet given the necessary information.
    public static TxnPacket getPacket(RPCNode node, int ID, Command c,byte[] requestPayload) {
        return null;
    }

    private TxnPacket(int serverSessionID, int requestID, Command c, byte[] payload) {
        
    }

    public static boolean validSizePayload(byte[] payload) {
        return payload.length < MAX_PAYLOAD_SIZE;
    }

    /**
     * @return The request command type
     */
    public Command getCommand() {
        return this.c;
    }

    /**
     * @return The payload
     */
    public byte[] getPayload() {
        return this.payload;
    }

    /**
     * Convert the TxnPacket object into a byte array for sending over
     * the wire. Format: protocol = 1 byte, payload <= MAX_PAYLOAD_SIZE bytes
     * 
     * @return A byte[] for transporting over the wire. Null if failed to pack
     *         for some reason
     */
    public byte[] pack() {
        return null;
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
    public static RPCRequestPacket unpack(byte[] packet) {
        return null;
    }

    /**
     * String representation of a TxnPacket
     */
    public String toString() {
        return null;
    }
}