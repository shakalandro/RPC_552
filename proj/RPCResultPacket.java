import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import edu.washington.cs.cse490h.lib.Utility;

/**
 * This conveys the header for an RPC result packet. This is carried in the
 * payload of a RIOPacket, and in turn the data being transferred is carried in
 * the payload of the RPCResultPacket packet. (And the RIOPacket is carried as
 * the payload of a Packet, etc).
 */
public class RPCResultPacket {

    public static final int MAX_PACKET_SIZE = RIOPacket.MAX_PAYLOAD_SIZE;
    public static final int HEADER_SIZE = 5;
    public static final int MAX_PAYLOAD_SIZE = MAX_PACKET_SIZE - HEADER_SIZE;

    private int requestID;
    private Status status;
    private byte[] payload;

    public static RPCResultPacket getPacket(RPCNode node, int ID,
            Status resultStatus, byte[] resultPayload) {
        if (resultPayload.length > MAX_PAYLOAD_SIZE) {
            System.err.println("Invalid payload size in RPCResultPacket");
            node.fail();
            return null;
        }
        return new RPCResultPacket(ID, resultStatus, resultPayload);
    }

    private RPCResultPacket(int requestID, Status status, byte[] payload) {
        this.requestID = requestID;
        this.status = status;
        this.payload = payload;
    }

    /**
     * @return The request id
     */
    public int getRequestID() {
        return this.requestID;
    }

    /**
     * Sets this result's status
     */
    public void setStatus(Status status) {
        this.status = status;
    }

    /**
     * @return The request status
     */
    public Status getStatus() {
        return this.status;
    }

    /**
     * Sets this result's payload
     */
    public void setPayload(byte[] payload) {
        this.payload = payload;
    }

    /**
     * @return The payload
     */
    public byte[] getPayload() {
        return this.payload;
    }

    /**
     * Convert the RPCResultPacket object into a byte array for sending over the
     * wire. Format: protocol = 1 byte, payload <= MAX_PAYLOAD_SIZE bytes
     * 
     * @return A byte[] for transporting over the wire. Null if failed to pack
     *         for some reason
     */
    public byte[] pack() {
        try {
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(byteStream);

            out.writeInt(requestID);
            out.writeByte(status.ordinal());
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
    public static RPCResultPacket unpack(byte[] packet) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(
                    packet));

            int requestID = in.readInt();
            Status status = Status.getStatus(in.readByte());

            byte[] payload = new byte[packet.length - HEADER_SIZE];
            int bytesRead = in.read(payload, 0, payload.length);

            if (bytesRead != payload.length && bytesRead != -1) {
                return null;
            }

            return new RPCResultPacket(requestID, status, payload);
        } catch (IllegalArgumentException e) {
            // will return null
        } catch (IOException e) {
            // will return null
        }
        return null;
    }

    /**
     * String representation of a RPCResultPacket
     */
    public String toString() {
        return "rpc-status: " + status + " rpc-result: "
                + Utility.byteArrayToString(this.payload) + " rpc-id: "
                + this.requestID;
    }
}