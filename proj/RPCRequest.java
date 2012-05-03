import edu.washington.cs.cse490h.lib.Callback;

    /**
     * Holds state information for RPC file requests -- used to queue
     * requests and to store callbacks for eventual replies.
     */
    public class RPCRequest {
        final Callback success;
        final Callback failure;
        final RPCRequestPacket pckt;
        final int serverAddr;
        final String filename;

        public RPCRequest(Callback success, Callback failure,
                RPCRequestPacket pckt, int serverAddr, String filename) {
            this.success = success;
            this.failure = failure;
            this.pckt = pckt;
            this.serverAddr = serverAddr;
            this.filename = filename;
        }

        public String toString() {
            return "RPC Request: " + pckt;
        }
    }