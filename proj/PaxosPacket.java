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
	
	public final PaxosMsg msgType; // Message type of this packet (propose, promise, etc)
	public final int instance;     // The round of Paxos
	public final int proposal;     // The proposal num in the given instance, or the highest accepted in the case of a promise
	public final byte[] payload;   // May include users to post to + message
	
	
	// This factory method is best for a prepare message
	public static PaxosPacket makePrepareMessage(int instanc, int proposa, byte[] payloa) {
		return new PaxosPacket(PaxosMsg.PREPARE, instanc, proposa, payloa);
	}
	
	// This factory method is best for a promise message
	public static PaxosPacket makePromiseMessage(int instanc, int highes, byte[] payloa) {
		return new PaxosPacket(PaxosMsg.PROMISE, instanc, highes, payloa);
	}
	
	// This factory method is best for an accept message
	public static PaxosPacket makeAcceptMessage(int instance, int proposal, byte[] payload) {
		return new PaxosPacket(PaxosMsg.ACCEPT, instance, proposal, payload);
	}
	
	// This factory method is best for an accepted message
	public static PaxosPacket makeAcceptedMessage(int instance, int proposal, byte[] payload) {
		return new PaxosPacket(PaxosMsg.ACCEPTED, instance, proposal, payload);
	}
	
	// This factory method is best for an accepted message
	public static PaxosPacket makeDecisionMessage(int instance, int proposal, byte[] payload) {
		return new PaxosPacket(PaxosMsg.DECISION, instance, proposal, payload);
	}
	
	private PaxosPacket(PaxosMsg type, int instance, int proposal, byte[] payload) {
        if (payload != null && payload.length > MAX_PAYLOAD_SIZE) {
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
            
            if (payload != null) {
            	out.write(payload, 0, payload.length);
            }

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
			System.out.println("read: " + bytesRead);
			if (bytesRead != payload.length) {
				return null;
			}
			return new PaxosPacket(PaxosMsg.getMessage(type), instance, proposal, payload);
		} catch (IllegalArgumentException e) {
			System.out.println("Problem: " + e.getMessage());
			e.printStackTrace();
		} catch(IOException e) {
			System.out.println("Problem: " + e.getMessage());
			e.printStackTrace();
		}
		return null;
	}
}
