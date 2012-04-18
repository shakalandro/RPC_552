import edu.washington.cs.cse490h.lib.Callback;

/**
 * Class to hold state information for RPC file requests -- used to queue
 * requests and to store callbacks for eventual replies.
 * 
 * @author jennyabrahamson
 */
public class RPCRequest {

    private Callback success;

    private Callback failure;

    private RPCRequestPacket pckt;

    private int serverAddr;

    private String filename;

    public RPCRequest(Callback success, Callback failure,
            RPCRequestPacket pckt, int serverAddr, String filename) {
        this.success = success;
        this.failure = failure;
        this.pckt = pckt;
        this.serverAddr = serverAddr;
        this.filename = filename;
    }

    public String getFilename() {
        return filename;
    }

    public RPCRequestPacket getPacket() {
        return pckt;
    }

    public int getServerAddr() {
        return serverAddr;
    }

    public Callback getSuccess() {
        return this.success;
    }

    public Callback getFailure() {
        return this.failure;
    }

    public String toString() {
        return "RPC Request: " + pckt;
    }
}
