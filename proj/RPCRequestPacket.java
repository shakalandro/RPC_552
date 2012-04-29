import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import edu.washington.cs.cse490h.lib.Utility;

/**
 * This conveys the header for an RPC request packet. This is carried in the
 * payload of a RIOPacket, and in turn the data being transferred is carried in
 * the payload of the RPCPacket packet. (And the RIOPacket is carried as the
 * payload of a Packet, etc).
 */
public class RPCRequestPacket {

    public static final int MAX_PACKET_SIZE = RIOPacket.MAX_PAYLOAD_SIZE;
    public static final int HEADER_SIZE = 9;
    public static final int MAX_PAYLOAD_SIZE = MAX_PACKET_SIZE - HEADER_SIZE;

    private int serverSessionID;
    private int requestID;
    private Command request;
    private byte[] payload;

    public static RPCRequestPacket getPacket(RPCNode node,
            int ID, Command requestType, byte[] requestPayload) {
        if (requestPayload.length > MAX_PAYLOAD_SIZE) {
            System.err.println("Invalid payload size in RPCRequestPacket");
            node.fail();
            return null;
        }
        return new RPCRequestPacket(ID, requestType, requestPayload);
    }

    private RPCRequestPacket(int requestID,
            Command request, byte[] payload) {
        this(0, requestID, request, payload);
    }

    private RPCRequestPacket(int serverSessionID, int requestID,
            Command request, byte[] payload) {
        if (payload.length > MAX_PAYLOAD_SIZE) {
            throw new IllegalArgumentException("Invalid RPC payload");
        }

        this.serverSessionID = serverSessionID;
        this.requestID = requestID;
        this.request = request;
        this.payload = payload;
    }
    
    public static boolean validSizePayload(byte[] payload) {
        return payload.length < MAX_PAYLOAD_SIZE;
    }

    /**
     * @return The server session id
     */
    public int serverSessionID() {
        return this.serverSessionID;
    }

    /**
     * Sets the serverSessionID for this packet
     */
    public void setServerSessionID(int id) {
        this.serverSessionID = id;
    }

    /**
     * @return The request id
     */
    public int getRequestID() {
        return this.requestID;
    }

    /**
     * @return The request command type
     */
    public Command getRequest() {
        return this.request;
    }

    /**
     * @return The payload
     */
    public byte[] getPayload() {
        return this.payload;
    }

    /**
     * Convert the RPCRequestPacket object into a byte array for sending over
     * the wire. Format: protocol = 1 byte, payload <= MAX_PAYLOAD_SIZE bytes
     * 
     * @return A byte[] for transporting over the wire. Null if failed to pack
     *         for some reason
     */
    public byte[] pack() {
        try {
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(byteStream);

            out.writeInt(serverSessionID);
            out.writeInt(requestID);
            out.writeByte(request.ordinal());

            out.write(payload, 0, payload.length);

            out.flush();
            out.close();
            return byteStream.toByteArray();
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Unpacks a byte array to create a RPCRequestPacket object. Assumes the
     * array has been formatted using pack method in RPCRequestPacket
     * 
     * @param packet
     *            String representation of the transport packet
     * @return RIOPacket object created or null if the byte[] representation was
     *         corrupted
     */
    public static RPCRequestPacket unpack(byte[] packet) {

        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(
                    packet));

            int serverSessionID = in.readInt();
            int requestID = in.readInt();
            Command request = Command.getCommand(in.readByte());

            byte[] payload = new byte[packet.length - HEADER_SIZE];
            int bytesRead = in.read(payload, 0, payload.length);

            if (bytesRead != payload.length && bytesRead != -1) {
                return null;
            }
            
            return new RPCRequestPacket(serverSessionID, requestID, request,
                    payload);
        } catch (IllegalArgumentException e) {
            // will return null
        } catch (IOException e) {
            // will return null
        }
        return null;
    }

    /**
     * String representation of a RPCRequestPacket
     */
    public String toString() {
        return "rpc-request:" + this.request + " rpc-server-session-id:"
                + this.serverSessionID + " rpc-id: " + this.requestID
                + " rpc-payload: " + Utility.byteArrayToString(this.payload);
    }
}