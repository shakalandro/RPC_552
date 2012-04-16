import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.Queue;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
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
    public static final int TIMEOUT_INTERVAL = 5;

    // Static assignment of the "server" node -- whichever node has id 0
    private static final int SERVER = 0;

    // Fields used by Server to store the last computed result
    // TODO: can this ever be requested?
    private int lastReceivedRequestID;
    private RPCResultPacket lastComputedResult;

    private final int MAX_FILE_SIZE = Math
            .min(RPCRequestPacket.MAX_PAYLOAD_SIZE,
                    RPCResultPacket.MAX_PAYLOAD_SIZE);
    private final String TEMP_PUT_FILE = ".temp_put_file";

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

            // Send request
            session(SERVER);
        }

        // Recover from a failed put
        if (Utility.fileExists(this, TEMP_PUT_FILE)) {
            try {
                PersistentStorageReader reader = this.getReader(TEMP_PUT_FILE);
                if (!reader.ready()) {
                    PersistentStorageWriter deleter = this.getWriter(
                            TEMP_PUT_FILE, false);
                    deleter.delete();
                } else {
                    String filename = reader.readLine();
                    char[] buf = new char[MAX_FILE_SIZE];
                    reader.read(buf);
                    PersistentStorageWriter writer = this.getWriter(filename,
                            false);
                    writer.write(buf);

                    // delete temp file
                    PersistentStorageWriter deleter = this.getWriter(filename,
                            false);
                    deleter.delete();
                }
            } catch (IOException e) {
                // fail ourselves and try again
                fail();
            }
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

    /* Requests the server's current session id */
    public void session(int serverAddr) {
        makeRequest(Command.SESSION, "session request", null, null, serverAddr,
                "");
    }

    /* Creates the file filename on server serverAddr */
    public void create(int serverAddr, String filename) {
        create(serverAddr, filename, null, null);
    }

    /*
     * Creates the file filename on server serverAddr, attaches callbacks for
     * the eventual reply
     */
    public void create(int serverAddr, String filename, Callback success,
            Callback failure) {
        makeRequest(Command.CREATE, filename, success, failure, serverAddr,
                filename);
    }

    /* Fetches the file filename on server serverAddr */
    public void get(int serverAddr, String filename) {
        get(serverAddr, filename, null, null);
    }

    /*
     * Fetches the file filename on server serverAddr, attaches callbacks for
     * the eventual reply
     */
    public void get(int serverAddr, String filename, Callback success,
            Callback failure) {
        makeRequest(Command.GET, filename, success, failure, serverAddr,
                filename);
    }

    /* Puts contents into file filename on server serverAddr */
    public void put(int serverAddr, String filename, String contents) {
        put(serverAddr, filename, contents, null, null);
    }

    /*
     * Puts contents into file filename on server serverAddr, attaches callbacks
     * for the eventual reply
     */
    public void put(int serverAddr, String filename, String contents,
            Callback success, Callback failure) {
        makeRequest(Command.PUT, filename + " " + contents, success, failure,
                serverAddr, filename);
    }

    /* Appends contents onto file filename on server serverAddr */
    private void append(int serverAddr, String filename, String contents) {
        append(serverAddr, filename, contents, null, null);
    }

    /*
     * Appends contents onto file filename on server serverAddr, attaches
     * callbacks for the eventual reply
     */
    public void append(int serverAddr, String filename, String contents,
            Callback success, Callback failure) {
        makeRequest(Command.APPEND, filename + " " + contents, success,
                failure, serverAddr, filename);
    }

    /* Deletes the file filename on server serverAddr */
    private void delete(int serverAddr, String filename) {
        delete(serverAddr, filename, null, null);
    }

    /*
     * Deletes the file filename on server serverAddr, attaches callbacks for
     * the eventual reply
     */
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
     * Sends the next request in the requestQueue, if it exists
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
            addTimeout(new Callback(method, this, params), TIMEOUT_INTERVAL);
        }
    }

    /**
     * Client-side handler for RPC Result packets. Logs a message to the console
     * (unless this is a reply to a session id request) and calls the relevant
     * callback (if it exists) dependent on whether status of the result is
     * SUCCESS.
     * 
     * @param from
     * @param pkt
     */
    private void handleRPCresult(Integer from, RPCResultPacket pkt) {

        // The original request, will remove from queue unless a failed session
        // ID request
        RPCRequest request = requestQueue.peek();

        if (pkt.getRequestID() != request.getPacket().getRequestID()) {
            // This reply is not for the current request, let's ignore it
            return;
        }

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

                // May need to update our serverSessionID
                if (status == Status.CRASH) {
                    serverSessionID = Integer.parseInt(Utility
                            .byteArrayToString(pkt.getPayload()));
                }

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

    // /////////////////////////////
    // SERVER SIDE RPC HANDLER CODE
    // /////////////////////////////

    /*
     * Server receives an RPC request.
     * 
     * 1. Checks that embedded session id matches the current session id (if not
     * returns CRASH with the new id) or is a request for the current session id
     * 
     * 2. If RPC ID matches the saved result, re-transmits result
     * 
     * 3. If RPC ID is lower than the saved result, this is an old request that
     * we should ignore
     * 
     * 4. Otherwise process the new request
     */
    private void handleRPCrequest(Integer from, RPCRequestPacket pkt) {

        Command request = pkt.getRequest();

        RPCResultPacket result;

        if (request == Command.SESSION) {
            // Request for session ID
            result = new RPCResultPacket(pkt.getRequestID(), Status.SUCCESS,
                    Utility.stringToByteArray(serverSessionID + ""));
        } else if (pkt.serverSessionID() != serverSessionID) {
            // Session IDs don't match
            result = new RPCResultPacket(pkt.getRequestID(), Status.CRASH,
                    Utility.stringToByteArray(serverSessionID + ""));
        } else if (pkt.getRequestID() == lastReceivedRequestID) {
            // Identical to last request
            if (lastComputedResult != null) {
                result = lastComputedResult;
            } else {
                // TODO: since the results (and record of last request id) are
                // only stored in memory and the server is single-threaded, I
                // don't think we can ever get here. If somehow we did, we'd
                // want to return CRASH
                result = new RPCResultPacket(pkt.getRequestID(), Status.CRASH,
                        Utility.stringToByteArray(serverSessionID + ""));
            }
        } else if (pkt.getRequestID() < lastReceivedRequestID) {
            // "Old" request
            // TODO: I don't think this can happen given the semantics that the
            // client waits for at most one result at a time, but if this were
            // to happen, we'd want to ignore this "old" request
            return;
        } else {
            // "New" request -- compute it!
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
                    // TODO: unknown request type, we'll ignore it
                    return;
                }
            }
        }

        lastReceivedRequestID = pkt.getRequestID();
        lastComputedResult = result;

        // Send response
        RIOSend(from, Protocol.RPC_RESULT_PKT, result.pack());
    }

    // /////////////////////////////
    // SERVER HANDLERS
    // /////////////////////////////

    /**
     * Gets and returns the contents of filename.
     * 
     * @param filename
     * @param id
     * @return NOT_EXIST status if the file does not exist. TOO_LARGE status if
     *         the file contents are too big. FAILURE if the get fails in
     *         process.
     */
    private RPCResultPacket get(String filename, int id) {
        if (!Utility.fileExists(this, filename)) {
            logError("Node " + this.addr + ": could not get " + filename
                    + ", does not exist.");
            return new RPCResultPacket(id, Status.NOT_EXIST,
                    Utility.stringToByteArray(Status.NOT_EXIST.toString()));
        }
        try {
            PersistentStorageReader getter = this.getReader(filename);
            char[] buf = new char[MAX_FILE_SIZE * 2];
            int size = getter.read(buf);
            if (size > MAX_FILE_SIZE) {
                logError("Node " + this.addr + ": could not get " + filename
                        + ", file is too large to transmit.");
                return new RPCResultPacket(id, Status.TOO_LARGE,
                        Utility.stringToByteArray(Status.TOO_LARGE.toString()));
            } else {
                return new RPCResultPacket(id, Status.SUCCESS,
                        Utility.stringToByteArray(new String(buf)));
            }
        } catch (IOException e) {
            logError("Node " + this.addr + ": failed to get " + filename
                    + " because a system IOException occurred.");
            return new RPCResultPacket(id, Status.FAILURE,
                    Utility.stringToByteArray(e.getMessage()));
        }
    }

    /**
     * Creates a file.
     * 
     * @param filename
     * @param id
     * @return ALREADY_EXISTS status if the file exists, FAILURE status if the
     *         creation fails in process
     */
    private RPCResultPacket create(String filename, int id) {
        if (Utility.fileExists(this, filename)) {
            logError("Node " + this.addr + ": could not create " + filename
                    + ", already exists.");
            return new RPCResultPacket(id, Status.ALREADY_EXISTS,
                    Utility.stringToByteArray(Status.ALREADY_EXISTS.toString()));
        }
        try {
            PersistentStorageWriter creator = this.getWriter(filename, false);
            creator.close();
            return new RPCResultPacket(id, Status.SUCCESS,
                    Utility.stringToByteArray("creating: " + filename));
        } catch (IOException e) {
            logError("Node " + this.addr + ": failed to create " + filename
                    + " because a system IOException occurred.");
            return new RPCResultPacket(id, Status.FAILURE,
                    Utility.stringToByteArray(e.getMessage()));
        }
    }

    /**
     * Puts contents into the file.
     * 
     * @param filename
     * @param contents
     * @param id
     * @return NOT_EXIST status if the file does not exist. FAILURE status if
     *         the put fails in process.
     */
    private RPCResultPacket put(String filename, String contents, int id) {
        if (!Utility.fileExists(this, filename)) {
            logError("Node " + this.addr + ": could not put " + filename
                    + ", does not exist.");
            return new RPCResultPacket(id, Status.NOT_EXIST,
                    Utility.stringToByteArray(Status.NOT_EXIST.toString()));
        }
        try {
            // Get old file contents into string
            PersistentStorageReader reader = getReader(filename);
            char[] buf = new char[MAX_FILE_SIZE];
            reader.read(buf);
            String oldFileData = new String(buf);

            // Put old file contents into temp file
            PersistentStorageWriter writer = this.getWriter(TEMP_PUT_FILE,
                    false);
            writer.write(filename + "/n" + oldFileData);

            // Write new contents to file
            writer = this.getWriter(filename, false);
            writer.write(contents);

            // Delete temp file
            writer = this.getWriter(filename, false);
            writer.delete();
            return new RPCResultPacket(id, Status.SUCCESS,
                    Utility.stringToByteArray("putting to: " + filename));
        } catch (IOException e) {
            logError("Node " + this.addr + ": failed to put " + filename
                    + " because a system IOException occurred.");
            return new RPCResultPacket(id, Status.FAILURE,
                    Utility.stringToByteArray(e.getMessage()));
        }
    }

    /**
     * Appends the contents to the file.
     * 
     * @param filename
     * @param contents
     * @param id
     * @return NOT_EXIST status if the file does not exist. TOO_LARGE status if
     *         the resulting file would be too large. FAILURE if the append
     *         fails in process.
     */
    private RPCResultPacket append(String filename, String contents, int id) {
        if (!Utility.fileExists(this, filename)) {
            logError("Node " + this.addr + ": could not append to " + filename
                    + ", does not exist.");
            return new RPCResultPacket(id, Status.NOT_EXIST,
                    Utility.stringToByteArray(Status.NOT_EXIST.toString()));
        }
        try {
            PersistentStorageReader reader = this.getReader(filename);
            char[] dummy_buf = new char[MAX_FILE_SIZE];

            // read can return -1 if the file is empty, make sure we mark size
            // as 0
            int size = Math.max(reader.read(dummy_buf), 0);
            if (size + contents.length() > MAX_FILE_SIZE) {
                int overflow = size + contents.length() - MAX_FILE_SIZE;
                logError("Node " + this.addr + ": could not append to "
                        + filename + ", contents was " + overflow
                        + " characters too long.");
                return new RPCResultPacket(id, Status.TOO_LARGE,
                        Utility.stringToByteArray(Status.TOO_LARGE.toString()));
            } else {
                PersistentStorageWriter appender = this.getWriter(filename,
                        true);
                appender.append(contents);
                return new RPCResultPacket(id, Status.SUCCESS,
                        Utility.stringToByteArray("appending to: " + filename));
            }
        } catch (IOException e) {
            logError("Node " + this.addr + ": failed to delete " + filename
                    + " because a system IOException occurred.");
            return new RPCResultPacket(id, Status.FAILURE,
                    Utility.stringToByteArray(e.getMessage()));
        }
    }

    /**
     * Deletes a file from the current node.
     * 
     * @param filename
     *            The name of the file to delete
     * @param id
     *            The request id for this rpc call
     * @return NOT_EXIST status if the file does not exist. FAILURE status if
     *         the delete fails in process.
     **/
    private RPCResultPacket delete(String filename, int id) {
        if (!Utility.fileExists(this, filename)) {
            logError("Node " + this.addr + ": " + filename + " does not exist.");
            return new RPCResultPacket(id, Status.NOT_EXIST,
                    Utility.stringToByteArray(Status.NOT_EXIST.toString()));
        }
        try {
            PersistentStorageWriter deleter = this.getWriter(filename, false);
            deleter.delete();
            return new RPCResultPacket(id, Status.SUCCESS,
                    Utility.stringToByteArray("deleting: " + filename));
        } catch (IOException e) {
            logError("Node " + this.addr + ": failed to delete " + filename
                    + " because a system IOException occurred.");
            return new RPCResultPacket(id, Status.FAILURE,
                    Utility.stringToByteArray(e.getMessage()));
        }
    }

    // //////////
    // MISC
    // //////////

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
