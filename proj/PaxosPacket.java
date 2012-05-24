import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Class to describe the header for Paxos packets. Intended to be carried as the payload
 * of a RIOPacket.
 * 
 * @author jennyabrahamson
 */
public class PaxosPacket {
	
    public static final int MAX_PACKET_SIZE = RIOPacket.MAX_PAYLOAD_SIZE;
    public static final int HEADER_SIZE = 9;
    public static final int MAX_PAYLOAD_SIZE = MAX_PACKET_SIZE - HEADER_SIZE;
	
	private final PaxosMsg msgType; // Message type of this packet (propose, promise, etc)
	private final int instance;     // The round of Paxos
	private final int proposal;     // The proposal num in the given instance
	private final byte[] payload;   // May include users to post to + message
	
	
	public PaxosPacket(PaxosMsg type, int instance, int proposal, byte[] payload) {
        if (payload.length > MAX_PAYLOAD_SIZE) {
            throw new IllegalArgumentException("Invalid PaxosPacket payload");
        }
		this.msgType = type;
		this.instance = instance;
		this.proposal = proposal;
		this.payload = payload;
	}

	/**
	 * Convert the PaxosPacket packet object into a byte array for sending over the wire.
	 * Format:
	 *        msgType = 1 byte
	 *        instance number = 4 bytes
	 *        proposal number = 4 bytes
	 *        payload <= MAX_PAYLOAD_SIZE bytes
	 * @return A byte[] for transporting over the wire. Null if failed to pack for some reason
	 */
	public byte[] pack() {
		try {
			ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(byteStream);
			
			out.writeByte(msgType.ordinal());
            out.writeInt(instance);
            out.writeInt(proposal);
			out.write(payload, 0, payload.length);

			out.flush();
			out.close();
			return byteStream.toByteArray();
		} catch (IOException e) {
			return null;
		}
	}

	/**
	 * Unpacks a byte array to create a PaxosPacket object
	 * Assumes the array has been formatted using pack method in PaxosPacket
	 * @param packet String representation of the transport packet
	 * @return PaxosPacket object created or null if the byte[] representation was corrupted
	 */
	public static PaxosPacket unpack(byte[] packet) {
		try {
			DataInputStream in = new DataInputStream(new ByteArrayInputStream(packet));

			int type = in.readByte();
			int instance = in.readInt();
			int proposal = in.readInt();

			byte[] payload = new byte[packet.length - HEADER_SIZE];
			int bytesRead = in.read(payload, 0, payload.length);

			if (bytesRead != payload.length) {
				return null;
			}

			return new PaxosPacket(PaxosMsg.getMessage(type), instance, proposal, payload);
		} catch (IllegalArgumentException e) {
			// will return null
		} catch(IOException e) {
			// will return null
		}
		return null;
	}
}
