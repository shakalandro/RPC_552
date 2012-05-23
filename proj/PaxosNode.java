
public class PaxosNode extends FacebookNode {

    /**
     * This node has a packet to process
     */
    @Override
    public void onRIOReceive(Integer from, int protocol, byte[] msg) {
        if (protocol == Protocol.PAXOS_PKT) {
            PaxosPacket pkt = PaxosPacket.unpack(msg);
            // TODO: Handle packet
        } else {
        	super.onRIOReceive(from, protocol, msg);
        }
    }
}
