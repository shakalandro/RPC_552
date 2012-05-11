import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import org.apache.commons.lang.exception.ExceptionUtils;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.MessageLayer;
import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

/**
 * This class provides RPC functionality, including implementation of client and server stubs and
 * handlers as well as the file commands that are provider to users.
 * 
 * @author Jenny Abrahamson
 */
public class RPCNode extends RIONode {

	/** Set failure/recovery/delay/drop rates based on command line options, default == 0% */
//	 public static double getFailureRate() { return MessageLayer.rpcFail / 100.0; }
//	 public static double getRecoveryRate() { return MessageLayer.rpcRecover / 100.0; }
//	 public static double getDropRate() { return MessageLayer.rpcDrop / 100.0; }
//	 public static double getDelayRate() { return MessageLayer.rpcDelay / 100.0; }

	public static double getFailureRate() {
		return 5 / 100.0;
	}

	public static double getRecoveryRate() {
		return 60.0 / 100.0;
	}

	public static double getDropRate() {
		return 20.0 / 100.0;
	}

	public static double getDelayRate() {
		return 30.0 / 100.0;
	}

	/** Colors for console logging */
	public static final boolean USE_COLORS = true;
	private static final String COLOR_CYAN = "0;36";
	private static final String COLOR_GREEN = "0;32";

	// ------------ SERVER VARIABLES ------------ //

	// Session ID -- on start up, Servers initialize this value using the
	// current time. Client invoke an RPC call to fetch this value from the
	// server
	private int mySessionID;

	// Map for servers from client id to last computed result
	private Map<Integer, RPCResultPacket> storedResults;

	// Name of temp file used by put commands
	private final String TEMP_PUT_FILE = ".temp_put_file";

	// ------------ CLIENT VARIABLES ------------ //

	// Counter for the next available request id -- used only by the client
	private int requestID;

	// Queue of requests for the client to send (client processes at most one
	// request at a time)
	private Queue<RPCRequest> requestQueue;

	// Map from server id to current session id
	private Map<Integer, Integer> serverSessionIDs;

	// ------------------------------------------- //

	// Number of steps to wait before re-sending requests
	public static final int TIMEOUT_INTERVAL = 10;

	// Max file size for RPC layer
	protected final int MAX_FILE_SIZE = Math.min(RPCRequestPacket.MAX_PAYLOAD_SIZE,
			RPCResultPacket.MAX_PAYLOAD_SIZE);

	@Override
	public void start() {
		// Initialize server variables
		mySessionID = (int) System.currentTimeMillis();
		storedResults = new HashMap<Integer, RPCResultPacket>();

		// Initialize client variables
		requestID = 0;
		requestQueue = new LinkedList<RPCRequest>();
		serverSessionIDs = new HashMap<Integer, Integer>();

		// Recover from a failed put
		if (Utility.fileExists(this, TEMP_PUT_FILE)) {
			try {
				PersistentStorageReader reader = this.getReader(TEMP_PUT_FILE);
				if (!reader.ready()) {
					PersistentStorageWriter deleter = this.getWriter(TEMP_PUT_FILE, false);
					deleter.delete();
				} else {
					String filename = reader.readLine();
					char[] buf = new char[MAX_FILE_SIZE];
					reader.read(buf, 0, MAX_FILE_SIZE);
					PersistentStorageWriter writer = this.getWriter(filename, false);
					writer.write(buf);

					// delete temp file
					PersistentStorageWriter deleter = this.getWriter(filename, false);
					deleter.delete();
				}
			} catch (IOException e) {
				// fail ourselves and try again
				logError("Could not recover log file for put, failing now.");
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
        } else {
            // Else invalid request, ignore
            logError("Received invalid request: \"" + command + "\"");
        }
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
    
    // ------------ CLIENT STUBS ------------ //

    /** Creates the file filename on server serverAddr */
    protected void create(int serverAddr, String filename) {
        create(serverAddr, filename, null, null);
    }

	/**
	 * Creates the file filename on server serverAddr, attaches callbacks for the eventual reply
	 */
	protected void create(int serverAddr, String filename, Callback success, Callback failure) {
		makeRequest(Command.CREATE, filename, success, failure, serverAddr, filename);
	}

	/** Fetches the file filename on server serverAddr */
	protected void get(int serverAddr, String filename) {
    	System.out.println("About to call makeRequest wrapper");
		get(serverAddr, filename, null, null);
	}

    /**
     * Fetches the file filename on server serverAddr, attaches callbacks for
     * the eventual reply
     */
    protected void get(int serverAddr, String filename, Callback success, Callback failure) {
    	System.out.println("About to call makeRequest");
        makeRequest(Command.GET, filename, success, failure, serverAddr, filename);
    }

    /** Puts contents into file filename on server serverAddr */
    protected void put(int serverAddr, String filename, String contents) {
        put(serverAddr, filename, contents, null, null);
    }

    /**
     * Puts contents into file filename on server serverAddr, attaches callbacks
     * for the eventual reply
     */
    protected void put(int serverAddr, String filename, String contents,
            Callback success, Callback failure) {
        makeRequest(Command.PUT, filename + " " + contents, success, failure,
                serverAddr, filename);
    }

    /** Appends contents onto file filename on server serverAddr */
    protected void append(int serverAddr, String filename, String contents) {
        append(serverAddr, filename, contents, null, null);
    }

    /**
     * Appends contents onto file filename on server serverAddr, attaches
     * callbacks for the eventual reply
     */
    protected void append(int serverAddr, String filename, String contents,
            Callback success, Callback failure) {
        makeRequest(Command.APPEND, filename + " " + contents, success,
                failure, serverAddr, filename);
    }

    /** Deletes the file filename on server serverAddr */
    protected void delete(int serverAddr, String filename) {
        delete(serverAddr, filename, null, null);
    }

    /**
     * Deletes the file filename on server serverAddr, attaches callbacks for
     * the eventual reply
     */
    protected void delete(int serverAddr, String filename, Callback success,
            Callback failure) {
        makeRequest(Command.DELETE, filename, success, failure, serverAddr,
                filename);
    }

    // ------------ CLIENT RPC HANDLER CODE ------------ //

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
    protected void makeRequest(Command command, String payload, Callback success,
            Callback failure, int serverAddr, String filename) {
    	this.makeRequest(command, Utility.stringToByteArray(payload), success, failure,
    			serverAddr, filename);
    }

    protected void makeRequest(Command command, byte[] payload, Callback success, Callback failure,
			int serverAddr, String filename) {

		if (!serverSessionIDs.containsKey(serverAddr)) {
			if (serverAddr == addr) {
				serverSessionIDs.put(serverAddr, mySessionID);
			} else {
				serverSessionIDs.put(serverAddr, -1);
				// May need to send RPC request to server requesting current session id
				if (command != Command.SESSION) {
					session(serverAddr);
				}
			}
		}

		if (RPCRequestPacket.validSizePayload(payload)) {
			RPCRequestPacket pkt = RPCRequestPacket.getPacket(this, requestID++, command, payload);
			RPCRequest request = new RPCRequest(success, failure, pkt, serverAddr, filename);
			requestQueue.add(request);
			attemptToSend(request);
		} else {
			// Cannot handle requests with payloads larger than (packet size -
			// headers)
			logError("Error: " + command + " on server " + serverAddr + " and file " + filename
					+ " returned error code " + Status.TOO_LARGE.getMsg());
		}
	}

    /* Requests the server's current session id */
    private void session(int serverAddr) {
        makeRequest(Command.SESSION, "session request", null, null, serverAddr, "");
    }

    /* Sends the next request in the requestQueue, if it exists */
    private void sendNextRequest() {
        if (!requestQueue.isEmpty()) {
            attemptToSend(requestQueue.peek());
        }
    }

    /** Sends the given RPC request if it is at the head of the requestQueue */
    public void attemptToSend(RPCRequest request) {
        if (requestQueue.peek() == request) {
        	send(request);
        }
    }
    
    /** Sends the given RPC request */
    private void send(RPCRequest request) {
        RPCRequestPacket pkt = request.pckt;
        
        pkt.setServerSessionID(serverSessionIDs.get(request.serverAddr));
        
        logOutput("SENDING to Node " + request.serverAddr + ": " + pkt.toString());
        RIOSend(request.serverAddr, Protocol.RPC_REQUEST_PKT, pkt.pack());

        // Set timeout to retry this method in TIMEOUT steps, will trigger
        // infinite timeouts
        Method method = null;
        try {
        	method = Callback.getMethod("attemptToSend", this, new String[] { "RPCRequest" });
        } catch (Exception e) {
        	// TODO Auto-generated catch block
        	e.printStackTrace();
        }
        Object[] params = { request };
        addTimeout(new Callback(method, this, params), TIMEOUT_INTERVAL);
    }

	/**
	 * Client-side handler for RPC Result packets. Logs a message to the console (unless this is a
	 * reply to a session id request) and calls the relevant callback (if it exists) dependent on
	 * whether status of the result is SUCCESS.
	 * 
	 * @param from
	 * @param pkt
	 */
	private void handleRPCresult(Integer from, RPCResultPacket pkt) {

		// The original request, will remove from queue unless a failed session
		// ID request
		RPCRequest request = requestQueue.peek();

		if (request == null || pkt.getRequestID() != request.pckt.getRequestID()) {
			// This reply is not for the current request, let's ignore it
			return;
		}

		Status status = pkt.getStatus();
		Command requestType = request.pckt.getRequest();

		// First handle session requests -- if successful, set serverSessionID
		// and move on to next request, else must make session request again
		if (requestType == Command.SESSION) {
			if (status == Status.SUCCESS) {
				serverSessionIDs.put(from,
						Integer.parseInt(Utility.byteArrayToString(pkt.getPayload())));
				requestQueue.poll();
			}
		} else {
			requestQueue.poll();
			Callback callback;

			if (status == Status.SUCCESS) {
				callback = request.success;

				// Log success message
				logOutput("Successfully completed: " + requestType + " on server "
						+ request.serverAddr);

				// Fill success handlers with node address and payload.
				String payload = Utility.byteArrayToString(pkt.getPayload());
				logOutput(payload);
				if (callback != null) {
					Object[] params = callback.getParams();
					params[0] = from;
					// Only set the payload if the callback can support it.
					if (pkt != null && params.length >= 2) {
						params[1] = pkt.getPayload();
					}
				}

			} else {

				// May need to update our serverSessionID
				if (status == Status.CRASH) {
					serverSessionIDs.put(from,
							Integer.parseInt(Utility.byteArrayToString(pkt.getPayload())));
					logOutput("Received crash message from Node " + from +
							", updating Node " + from + " session id to " + serverSessionIDs.get(from));
				}

				callback = request.failure;
				// Log that error occurred
				logError("Error: " + requestType + " on server " + request.serverAddr
						+ " and file " + request.filename + " returned error code "
						+ status.getMsg());
				if (callback != null) {
					Object[] params = callback.getParams();
					params[0] = status.getCode();
				}

			}

			// If appropriate callback not null, invoke it
			if (callback != null) {
				try {
					logOutput("Callback " + callback.toString());
					callback.invoke();
				} catch (Exception e) {
					ExceptionUtils.printRootCauseStackTrace(e);
				}
			}
		}

		// Begin next request
		sendNextRequest();
	}
    // ------------ SERVER HANDLER CODE ------------ //

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
    protected void handleRPCrequest(Integer from, RPCRequestPacket pkt) {
    	if (!storedResults.containsKey(from)) {
    		storedResults.put(from, null);
    	}
    	
    	RPCResultPacket lastResult = storedResults.get(from);
    	
    	Command request = pkt.getRequest();

        RPCResultPacket result;

        if (request == Command.SESSION) {
            // Request for session ID
            result = RPCResultPacket.getPacket(this, pkt.getRequestID(),
                    Status.SUCCESS,
                    Utility.stringToByteArray(mySessionID + ""));
        } else if (pkt.serverSessionID() != mySessionID) {
        	logOutput("Crash detected, sending new session id: " + mySessionID);
            // Session IDs don't match
            result = RPCResultPacket.getPacket(this, pkt.getRequestID(),
                    Status.CRASH,
                    Utility.stringToByteArray(mySessionID + ""));
        } else if (lastResult != null && pkt.getRequestID() == lastResult.getRequestID()) {
            // Identical to last request
        	result = lastResult;
        } else if (lastResult != null && pkt.getRequestID() < lastResult.getRequestID()) {
            // "Old" request -- ignore it
        	logError("Received stale RPC Request ID, ignored");
            return;
        } else {
            // "New" request -- compute it!
            result = handleRPCCommand(request, from, pkt);
        }

        storedResults.put(from, result);


        // Send response
        logOutput("SENDING to Node " + from + ": " + pkt.toString());
        RIOSend(from, Protocol.RPC_RESULT_PKT, result.pack());
    }
    
    /**
     * This method responds to an RPC command. This can be overriden in order to add new
     * functionality to a subclass that understands other commands.
     * @param request
     * @param pkt
     * @return The RPCResultPacket for the given command request.
     */
    protected RPCResultPacket handleRPCCommand(Command request, int senderAddr, RPCRequestPacket pkt) {
	    String payload = Utility.byteArrayToString(pkt.getPayload());
	    RPCResultPacket result;
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
	            logError("Node " + addr + ": received unknown request "
	                    + request);
	            result = RPCResultPacket.getPacket(this, pkt.getRequestID(),
	                    Status.UNKNOWN_REQUEST,
	                    Utility.stringToByteArray(mySessionID + ""));
	        }
	    }
	    return result;
    }
	
    // ------------ SERVER HANDLER IMPLEMENTATIONS ------------ //

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
            return RPCResultPacket.getPacket(this, id, Status.NOT_EXIST,
                    Utility.stringToByteArray(Status.NOT_EXIST.getMsg()));
        }
        try {
            PersistentStorageReader getter = this.getReader(filename);
            char[] buf = new char[MAX_FILE_SIZE * 2];
            int size = getter.read(buf, 0, MAX_FILE_SIZE * 2);
            if (size > MAX_FILE_SIZE) {
                logError("could not get " + filename
                        + ", file is too large to transmit.");
                return RPCResultPacket.getPacket(this, id, Status.TOO_LARGE,
                        Utility.stringToByteArray(Status.TOO_LARGE.getMsg()));
            }
            return RPCResultPacket.getPacket(this, id, Status.SUCCESS, Utility
                    .stringToByteArray(new String(buf, 0, Math.max(0, size))));
        } catch (IOException e) {
            logError("failed to get " + filename
                    + " because a system IOException occurred.");
            return RPCResultPacket.getPacket(this, id, Status.FAILURE,
                    Utility.stringToByteArray(e.getMessage()));
        }
    }
    
	/**
	 * Creates a file.
	 * 
	 * @param filename
	 * @param id
	 * @return ALREADY_EXISTS status if the file exists, FAILURE status if the creation fails in
	 *         process
	 */
	private RPCResultPacket create(String filename, int id) {
		if (Utility.fileExists(this, filename)) {
			logError("could not create " + filename + ", already exists.");
			return RPCResultPacket.getPacket(this, id, Status.ALREADY_EXISTS,
					Utility.stringToByteArray(Status.ALREADY_EXISTS.getMsg()));
		}
		try {
			PersistentStorageWriter creator = this.getWriter(filename, false);
			creator.close();
			return RPCResultPacket.getPacket(this, id, Status.SUCCESS,
					Utility.stringToByteArray("creating: " + filename));
		} catch (IOException e) {
			logError("failed to create " + filename + " because a system IOException occurred.");
			return RPCResultPacket.getPacket(this, id, Status.FAILURE,
					Utility.stringToByteArray(e.getMessage()));
		}
	}

	/**
	 * Puts contents into the file.
	 * 
	 * @param filename
	 * @param contents
	 * @param id
	 * @return NOT_EXIST status if the file does not exist. FAILURE status if the put fails in
	 *         process.
	 */
	private RPCResultPacket put(String filename, String contents, int id) {
		if (!Utility.fileExists(this, filename)) {
			logError("could not put " + filename + ", does not exist.");
			return RPCResultPacket.getPacket(this, id, Status.NOT_EXIST,
					Utility.stringToByteArray(Status.NOT_EXIST.getMsg()));
		}
		try {
			// Get old file contents into string
			PersistentStorageReader reader = getReader(filename);

			char[] buf = new char[MAX_FILE_SIZE];
			reader.read(buf, 0, MAX_FILE_SIZE);
			String oldFileData = new String(buf);

			// Put old file contents into temp file
			PersistentStorageWriter writer = this.getWriter(TEMP_PUT_FILE, false);
			writer.write(filename + "/n" + oldFileData);
			writer.close();

			// Write new contents to file
			writer = this.getWriter(filename, false);
			writer.write(contents);
			writer.close();

			// Delete temp file
			writer = this.getWriter(TEMP_PUT_FILE, false);
			writer.delete();
			writer.close();
			return RPCResultPacket.getPacket(this, id, Status.SUCCESS,
					Utility.stringToByteArray("putting to: " + filename));
		} catch (IOException e) {
			logError("failed to put " + filename + " because a system IOException occurred.");
			return RPCResultPacket.getPacket(this, id, Status.FAILURE,
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
            logError("could not append to " + filename + ", does not exist.");
            return RPCResultPacket.getPacket(this, id, Status.NOT_EXIST,
                    Utility.stringToByteArray(Status.NOT_EXIST.getMsg()));
        }

        try {
            PersistentStorageReader reader = this.getReader(filename);
            char[] dummy_buf = new char[MAX_FILE_SIZE];

            // read can return -1 if the file is empty, make sure we mark size
            // as 0
            int size = Math.max(reader.read(dummy_buf), 0);
            if (size + contents.length() > MAX_FILE_SIZE) {
                int overflow = size + contents.length() - MAX_FILE_SIZE;
                logError("could not append to " + filename + ", contents was "
                        + overflow + " characters too long.");
                return RPCResultPacket.getPacket(this, id, Status.TOO_LARGE,
                        Utility.stringToByteArray(Status.TOO_LARGE.getMsg()));
            }
            PersistentStorageWriter appender = this.getWriter(filename, true);
            appender.append(contents);
            appender.close();
            return RPCResultPacket.getPacket(this, id, Status.SUCCESS,
                    Utility.stringToByteArray("appending to: " + filename));

        } catch (IOException e) {
            logError("failed to delete " + filename
                    + " because a system IOException occurred.");
            return RPCResultPacket.getPacket(this, id, Status.FAILURE,
                    Utility.stringToByteArray(e.getMessage()));
        }
    }

	/**
	 * Deletes a file from the current node.
	 * 
	 * @param filename The name of the file to delete
	 * @param id The request id for this rpc call
	 * @return NOT_EXIST status if the file does not exist. FAILURE status if the delete fails in
	 *         process.
	 **/
	private RPCResultPacket delete(String filename, int id) {
		if (!Utility.fileExists(this, filename)) {
			logError(filename + " does not exist.");
			return RPCResultPacket.getPacket(this, id, Status.NOT_EXIST,
					Utility.stringToByteArray(Status.NOT_EXIST.getMsg()));
		}
		try {
			PersistentStorageWriter deleter = this.getWriter(filename, false);
			deleter.delete();
			deleter.close();
			return RPCResultPacket.getPacket(this, id, Status.SUCCESS,
					Utility.stringToByteArray("deleting: " + filename));
		} catch (IOException e) {
			logError("failed to delete " + filename + " because a system IOException occurred.");
			return RPCResultPacket.getPacket(this, id, Status.FAILURE,
					Utility.stringToByteArray(e.getMessage()));
		}
	}

    // ------------ LOGGING ------------ //

    private void logError(String output) {
    	if (MessageLayer.rpcLog) {
    		log(output, System.err, COLOR_CYAN);
    	}
    }

    private void logOutput(String output) {
    	if (MessageLayer.rpcLog) {
    		log(output, System.out, COLOR_GREEN);
    	}
    }

    public void log(String output, PrintStream stream, String colorCommand) {
    	if (USE_COLORS) {
    		stream.println((char)27 + "[" + colorCommand + "m" + "Node " + addr + ": " + output + (char)27 + "[m");
    	} else {
    		stream.println("Node " + addr + ": " + output);
    	}
    }
}
