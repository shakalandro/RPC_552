
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import edu.washington.cs.cse490h.lib.Packet;
import edu.washington.cs.cse490h.lib.Utility;

/**
 * This conveys the header for reliable, in-order message transfer. This is
 * carried in the payload of a Packet, and in turn the data being transferred is
 * carried in the payload of the RIOPacket packet.
 */
public class RIOPacket {

	public static final int MAX_PACKET_SIZE = Packet.MAX_PAYLOAD_SIZE;
	public static final int HEADER_SIZE = 9;
	public static final int MAX_PAYLOAD_SIZE = MAX_PACKET_SIZE - HEADER_SIZE;

	// A unique id identifying the communication session between sender and reciever.
	private int sessionId;
	private int protocol;
	private int seqNum;
	private byte[] payload;

	/**
	 * Constructing a new RIO packet.
	 * @param type The type of packet. Either SYN, ACK, FIN, or DATA
	 * @param seqNum The sequence number of the packet
	 * @param payload The payload of the packet.
	 */
	public RIOPacket(int protocol, int seqNum, int sessionId, byte[] payload) throws IllegalArgumentException {
		if (!Protocol.isRIOProtocolValid(protocol) || payload.length > MAX_PAYLOAD_SIZE) {
			throw new IllegalArgumentException("Illegal arguments given to RIOPacket");
		}

		this.protocol = protocol;
		this.seqNum = seqNum;
		this.sessionId = sessionId;
		this.payload = payload;
	}

	/**
	 * @return The protocol number
	 */
	public int getProtocol() {
		return this.protocol;
	}
	
	/**
	 * @return The session id.
	 */
	public int getSessionId() {
		return this.sessionId;
	}
	
	/**
	 * @return The sequence number
	 */
	public int getSeqNum() {
		return this.seqNum;
	}

	/**
	 * @return The payload
	 */
	public byte[] getPayload() {
		return this.payload;
	}

	/**
	 * Convert the RIOPacket packet object into a byte array for sending over the wire.
	 * Format:
	 *        protocol = 1 byte
	 *        sequence number = 4 bytes
	 *        payload <= MAX_PAYLOAD_SIZE bytes
	 * @return A byte[] for transporting over the wire. Null if failed to pack for some reason
	 */
	public byte[] pack() {
		try {
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(byteStream);

			out.writeByte(protocol);
			out.writeInt(seqNum);
			out.writeInt(sessionId);

			out.write(payload, 0, payload.length);

			out.flush();
			out.close();
			return byteStream.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	/**
	 * Unpacks a byte array to create a RIOPacket object
	 * Assumes the array has been formatted using pack method in RIOPacket
	 * @param packet String representation of the transport packet
	 * @return RIOPacket object created or null if the byte[] representation was corrupted
	 */
	public static RIOPacket unpack(byte[] packet) {
		try {
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(packet));

			int protocol = in.readByte();
			int seqNum = in.readInt();
			int sessionId = in.readInt();

			byte[] payload = new byte[packet.length - HEADER_SIZE];
			int bytesRead = in.read(payload, 0, payload.length);

			if (bytesRead != payload.length) {
				return null;
			}

			return new RIOPacket(protocol, seqNum, sessionId, payload);
		} catch (IllegalArgumentException e) {
			// will return null
		} catch(IOException e) {
			// will return null
		}
		return null;
	}
	
	/**
	 * Reads a byte array from a stream to create a RIOPacket object
	 * Assumes the array has been formatted using pack method in RIOPacket
	 * @param in InputStream from which to read
	 * @return RIOPacket object created or null if the byte[] representation was corrupted
	 */
	public static RIOPacket unpack(InputStream in) {
		return unpack(new DataInputStream(in));
	}
	
	/**
	 * String representation of a RIOPacket
	 */
	public String toString() {
		return "rio-proto:" + this.protocol + " rio-seqNum:" + this.seqNum + " rio-payload:" + Utility.byteArrayToString(this.payload); 
	}
}
