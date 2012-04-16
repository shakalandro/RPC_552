import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.Queue;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.Utility;

public class RPCNode extends RIONode {

    // Session ID -- on start up, Servers initialize this value using the
    // current time. Client invoke an RPC call to fetch this value from the
    // server
    private int serverSessionID;

    // Counter for the next available request id -- used only by the client
    private int requestID;

    // Queue of requests for the client to send (client processes at most one
    // request at a time)
    private Queue<RPCRequest> requestQueue;

    // Number of steps to wait before re-sending requests
    public static final int TIMEOUT = 5;

    // Static assignment of the "server" node -- whichever node has id 0
    private static final int SERVER = 0;

    // Fields used by Server to store the last computed result
    // TODO: can this ever be requested?
    private int lastReceivedRequestID;
    private RPCResultPacket lastComputedResult;

    @Override
    public void start() {
        if (addr == SERVER) {
            // If server, need to initialize a new session id
            serverSessionID = (int) System.currentTimeMillis();
        } else {
            requestID = 0;
            requestQueue = new LinkedList<RPCRequest>();

            // If client, need to send RPC request to server requesting current
            // session id

            // Method method = null;
            // try {
            // method = Callback.getMethod("attemptToSend", this,
            // new String[] {});
            // } catch (Exception e) {
            // // TODO Auto-generated catch block
            // e.printStackTrace();
            // }
            // Callback callback = new Callback(method, this, new Object[0]);

            session(SERVER);
        }
    }

    /**
     * There is a user or input file command to process. The command String
     * needs to be in the following format: command server filename
     * {additionalInfo}
     */
    @Override
    public void onCommand(String command) {
        String[] request = command.split(" ", 4);

        if (request.length >= 3) {
            String type = request[0];
            int server = Integer.parseInt(request[1]);
            String filename = request[2];

            if (type.equals("create")) {
                create(server, filename);
            } else if (type.equals("get")) {
                get(server, filename);
            } else if (type.equals("delete")) {
                delete(server, filename);
            }

            if (request.length == 4) {
                String file = request[3];

                if (type.equals("put")) {
                    put(server, filename, file);
                } else if (type.equals("append")) {
                    append(server, filename, file);
                }
            }
        }
        // Else invalid request, ignore
    }

    /**
     * This node has a packet to process
     */
    @Override
    public void onRIOReceive(Integer from, int protocol, byte[] msg) {

        if (protocol == Protocol.RPC_REQUEST_PKT) {
            RPCRequestPacket pkt = RPCRequestPacket.unpack(msg);
            logOutput("JUST RECEIVED: " + pkt.toString());
            handleRPCrequest(from, pkt);
        } else if (protocol == Protocol.RPC_RESULT_PKT) {
            RPCResultPacket pkt = RPCResultPacket.unpack(msg);
            logOutput("JUST RECEIVED: " + pkt.toString());
            handleRPCresult(from, pkt);
        } else {
            logError("unknown protocol: " + protocol);
            return;
        }
    }

    // /////////////////////////////
    // CLIENT STUBS
    // /////////////////////////////

    public void session(int serverAddr) {
        makeRequest(Command.SESSION, "session request", null, null, serverAddr,
                "");
    }

    public void create(int serverAddr, String filename) {
        create(serverAddr, filename, null, null);
    }

    public void create(int serverAddr, String filename, Callback success,
            Callback failure) {
        makeRequest(Command.CREATE, filename, success, failure, serverAddr,
                filename);
    }

    public void get(int serverAddr, String filename) {
        get(serverAddr, filename, null, null);
    }

    public void get(int serverAddr, String filename, Callback success,
            Callback failure) {
        makeRequest(Command.GET, filename, success, failure, serverAddr,
                filename);
    }

    public void put(int serverAddr, String filename, String contents) {
        put(serverAddr, filename, contents, null, null);
    }

    public void put(int serverAddr, String filename, String contents,
            Callback success, Callback failure) {
        makeRequest(Command.PUT, filename + " " + contents, success, failure,
                serverAddr, filename);
    }

    private void append(int serverAddr, String filename, String contents) {
        append(serverAddr, filename, contents, null, null);
    }

    public void append(int serverAddr, String filename, String contents,
            Callback success, Callback failure) {
        makeRequest(Command.APPEND, filename + " " + contents, success,
                failure, serverAddr, filename);
    }

    private void delete(int serverAddr, String filename) {
        delete(serverAddr, filename, null, null);
    }

    public void delete(int serverAddr, String filename, Callback success,
            Callback failure) {
        makeRequest(Command.DELETE, filename, success, failure, serverAddr,
                filename);
    }

    // /////////////////////////////
    // CLIENT SIDE RPC HANDLER CODE
    // /////////////////////////////

    /**
     * Adds an RPC request to the client's queue of requests. Sends the request
     * immediately if it is the first request in the queue.
     * 
     * @param command
     *            Request type
     * @param payload
     *            Payload for request packet
     * @param success
     *            Optional success callback function
     * @param failure
     *            Optional failure callback function
     * @param serverAddr
     *            Address of server to receive request
     * @param filename
     *            Name of file for request
     */
    private void makeRequest(Command command, String payload, Callback success,
            Callback failure, int serverAddr, String filename) {
        byte[] payloadBytes = Utility.stringToByteArray(payload);

        if (RPCRequestPacket.validSizePayload(payloadBytes)) {
            RPCRequestPacket pkt = new RPCRequestPacket(serverSessionID,
                    requestID++, command, payloadBytes);
            RPCRequest request = new RPCRequest(success, failure, pkt,
                    serverAddr, filename);
            requestQueue.add(request);
            attemptToSend(request);
        } else {
            // Cannot handle requests with payloads larger than (packet size -
            // headers)
            logError("Node " + this.addr + ": Error: " + command
                    + " on server " + serverAddr + " and file " + filename
                    + " returned error code " + Status.TOO_LARGE.toString());
        }
    }

    /**
     * Initializes sending the next request in the requestQueue, if it exists
     */
    public void sendNextRequest() {
        if (!requestQueue.isEmpty()) {
            attemptToSend(requestQueue.peek());
        }
    }

    /**
     * Sends the given RPC request if it is at the head of the requestQueue
     */
    public void attemptToSend(RPCRequest request) {
        if (requestQueue.peek() == request) {
            RPCRequestPacket pkt = request.getPacket();
            pkt.setServerSessionID(serverSessionID);

            RIOSend(request.getServerAddr(), Protocol.RPC_REQUEST_PKT,
                    pkt.pack());

            // Set timeout to retry this method in 5 steps, will triggor
            // infinite timeouts
            Method method = null;
            try {
                method = Callback.getMethod("attemptToSend", this,
                        new String[] { "RPCRequest" });
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            Object[] params = { request };
            addTimeout(new Callback(method, this, params), TIMEOUT);
        }
    }

    /**
     * Client-side handler for RPC Result packets. Logs a message to the console
     * (unless this is a reply to a session id request) and calls the relevant
     * callback (if it exists) dependent on whether status of the result is
     * SUCCESS. The method assumes RPC Result packets will only be received for
     * the request currently at the head of the request queue.
     * 
     * @param from
     * @param pkt
     */
    private void handleRPCresult(Integer from, RPCResultPacket pkt) {

        // The original request, will remove from queue unless a failed session
        // ID request
        RPCRequest request = requestQueue.peek();

        Status status = pkt.getStatus();
        Command requestType = request.getPacket().getRequest();

        // First handle session requests -- if successful, set serverSessionID
        // and move on to next request, else must make session request again
        if (requestType == Command.SESSION) {
            if (status == Status.SUCCESS) {
                serverSessionID = Integer.parseInt(Utility
                        .byteArrayToString(pkt.getPayload()));
                requestQueue.poll();
            }
        } else {
            requestQueue.poll();
            Callback callback;

            if (status == Status.SUCCESS) {
                callback = request.getSuccess();

                // Log success message
                logOutput("Node " + this.addr + ": Successfully completed: "
                        + requestType + " on server " + request.getServerAddr()
                        + " and file " + request.getFilename());

                // If GET command result, print contents of file to console
                if (requestType == Command.GET) {
                    logOutput(Utility.byteArrayToString(pkt.getPayload()));
                }

            } else {
                callback = request.getFailure();
                // Log that error occurred
                logError("Node " + this.addr + ": Error: " + requestType
                        + " on server " + request.getServerAddr()
                        + " and file " + request.getFilename()
                        + " returned error code " + status.toString());

            }

            // If appropriate callback not null, invoke it
            if (callback != null) {
                try {
                    callback.invoke();
                } catch (IllegalAccessException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        // Begin next request
        sendNextRequest();
    }

    // Client side result handlers

    // /////////////////////////////
    // SERVER SIDE RPC HANDLER CODE
    // /////////////////////////////

    /*
     * Server receives an RPC request.
     * 
     * 1. Checks that embedded session id matches the current session id (if not
     * returns CRASH with the new id)
     * 
     * 2. If RPC ID matches the saved result, re-transmits result
     * 
     * 3. Otherwise we will process the new request
     */
    private void handleRPCrequest(Integer from, RPCRequestPacket pkt) {

        Command request = pkt.getRequest();

        RPCResultPacket result;

        if (request == Command.SESSION) {
            result = new RPCResultPacket(pkt.getRequestID(), Status.SUCCESS,
                    Utility.stringToByteArray(serverSessionID + ""));
        } else if (pkt.serverSessionID() != serverSessionID) {
            result = new RPCResultPacket(pkt.getRequestID(), Status.CRASH,
                    Utility.stringToByteArray(serverSessionID + ""));
        } else if (pkt.getRequestID() == lastReceivedRequestID) {
            if (lastComputedResult != null) {
                result = lastComputedResult;
            } else {
                result = new RPCResultPacket(pkt.getRequestID(), Status.CRASH,
                        new byte[0]);
            }
        } else {
            lastReceivedRequestID = pkt.getRequestID();
            String payload = Utility.byteArrayToString(pkt.getPayload());
            switch (request) {
            case GET:
                result = get(payload, pkt.getRequestID());
                break;
            case CREATE:
                result = create(payload, pkt.getRequestID());
                break;
            case DELETE:
                result = delete(payload, pkt.getRequestID());
                break;
            default:
                String[] contents = payload.split(" ", 2);
                switch (request) {
                case PUT:
                    result = put(contents[0], contents[1], pkt.getRequestID());
                    break;
                case APPEND:
                    result = append(contents[0], contents[1],
                            pkt.getRequestID());
                    break;
                default:
                    // TODO: unknown request
                    return;
                }
            }
        }

        lastReceivedRequestID = pkt.getRequestID();
        lastComputedResult = result;

        RIOSend(from, Protocol.RPC_RESULT_PKT, result.pack());
    }

    // /////////////////////////////
    // SERVER HANDLERS
    // /////////////////////////////

    private RPCResultPacket get(String filename, int id) {
        // TODO
        return new RPCResultPacket(id, Status.SUCCESS,
                Utility.stringToByteArray("getting: " + filename));

    }

    private RPCResultPacket create(String filename, int id) {
        // TODO
        return new RPCResultPacket(id, Status.SUCCESS,
                Utility.stringToByteArray("creating: " + filename));
    }

    private RPCResultPacket put(String filename, String contents, int id) {
        if (!Utility.fileExists(this, filename)) {
            // TODO: return error 10

        }
        try {
            PersistentStorageReader reader = getReader(filename);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // TOOD
        // 1: String oldFile = read foo.txt
        // 2: PSWriter temp = getWriter(.temp, false)
        // 3: temp.write("foo.txt\n" + oldFile)
        // 4: PSWriter newFile = getWriter(foo.txt, false)
        // 5: newFile.write(contents)
        // 6: delete temp
        //
        // then on a server restart:
        //
        // if .temp exists
        // PSReader temp = getReader(.temp)
        // if (!temp.ready())
        // delete temp
        // else
        // filename = temp.readLine()
        // oldContents = read rest of temp
        // PSWriter revertFile = getWriter(filename, false)
        // revertFile.write(oldContents)
        // delete temp
        //
        // so essentially there are 3 cases:
        // no temp file (state is consistent)
        // empty temp file, which means that the server failed between lines 2
        // and 3 (file has not been changed)
        // temp file with some content, which means that the server failed
        // between lines 3 and 6 (possibly empty file)
        //
        // You should think about why this works for a crash between any two
        // lines (including crashes within the recovery mechanism)
        return new RPCResultPacket(id, Status.SUCCESS,
                Utility.stringToByteArray("putting to: " + filename));
    }

    private RPCResultPacket append(String filename, String contents, int id) {
        // TODO
        return new RPCResultPacket(id, Status.SUCCESS,
                Utility.stringToByteArray("appending to: " + filename));
    }

    private RPCResultPacket delete(String filename, int id) {
        // TODO
        return new RPCResultPacket(id, Status.SUCCESS,
                Utility.stringToByteArray("deleting: " + filename));
    }

    // MISC
    public void logError(String output) {
        log(output, System.err);
    }

    public void logOutput(String output) {
        log(output, System.out);
    }

    public void log(String output, PrintStream stream) {
        stream.println("Node " + addr + ": " + output);
    }
}
