
/**
 * <pre>
 * Contains details about the recognized protocols
 * </pre>
 */
public class Protocol {
    // Protocols for the Reliable in-order message layer
    // These should be Packet protocols
    // TODO: make these enums so it's clearer when to use RIOPacket, or
    // something else
    public static final int DATA = 0;
    public static final int ACK = 1;

    // Protocols for Testing Reliable in-order message delivery
    // These should be RIOPacket protocols
    public static final int RIOTEST_PKT = 10;

    // Protocol for RPC nodes
    public static final int RPC_REQUEST_PKT = 25;
    public static final int RPC_RESULT_PKT = 26;

    public static final int MAX_PROTOCOL = 127;

    /**
     * Tests if this is a valid protocol for a Packet
     * 
     * @param protocol
     *            The protocol in question
     * @return true if the protocol is valid, false otherwise
     */
    public static boolean isPktProtocolValid(int protocol) {
        return (protocol == DATA || protocol == ACK);
    }

    /**
     * Tests if the given protocol is valid for a RIOPacket. Note that the
     * current implementation of RIOPacket actually uses this to test validity
     * of packets.
     * 
     * @param protocol
     *            The protocol to be checked
     * @return True if protocol is valid, else false
     */
    public static boolean isRIOProtocolValid(int protocol) {
        return protocol == RIOTEST_PKT || protocol == RPC_REQUEST_PKT
                || protocol == RPC_RESULT_PKT;
    }

    /**
     * Tests if the given protocol is valid for a RPCRequestPacket. Note that
     * the current implementation of RPCPacket actually uses this to test
     * validity of packets.
     * 
     * @param protocol
     *            The protocol to be checked
     * @return True if protocol is valid, else false
     */
    public static boolean isRPCRequestProtocolValid(int protocol) {
        return protocol == RPC_REQUEST_PKT;
    }

    /**
     * Tests if the given protocol is valid for a RPCResultPacket. Note that the
     * current implementation of RPCPacket actually uses this to test validity
     * of packets.
     * 
     * @param protocol
     *            The protocol to be checked
     * @return True if protocol is valid, else false
     */
    public static boolean isRPCResultProtocolValid(int protocol) {
        return protocol == RPC_RESULT_PKT;
    }

    /**
     * Returns a string representation of the given protocol. Can be used for
     * debugging
     * 
     * @param protocol
     *            The protocol whose string representation is desired
     * @return The string representation of the given protocol.
     *         "Unknown Protocol" if the protocol is not recognized
     */
    public static String protocolToString(int protocol) {
        switch (protocol) {
        case DATA:
            return "RIO Data Packet";
        case ACK:
            return "RIO Acknowledgement Packet";
        case RIOTEST_PKT:
            return "RIO Testing Packet";
        case RPC_REQUEST_PKT:
            return "RPC Request Packet";
        case RPC_RESULT_PKT:
            return "RPC Reply Packet";
        default:
            return "Unknown Protocol";
        }
    }
}
