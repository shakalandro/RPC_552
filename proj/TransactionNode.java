import java.io.Serializable;
import java.lang.reflect.*;

import edu.washington.cs.cse490h.lib.Callback;

/*
 * This class provides distributed transactional operations. The operations are generic.
 * 
 * Thoughts:
 * 	1) Transaction RPC proposal holds a serialized Method object or a method's name
 * 	2) If the receiver can instantiate the described method then the call goes through
 * 	3) The Method must return a Boolean
 * 
 */


public class TransactionNode extends RPCNode {

	@Override
	public void start() {
		super.start();
		
	}
	
	/**
	 * This method takes a method that will be executed by all participants or none.
	 * @param participant_addrs An array of int addresses.
	 * @param method_name The method that you would like to propose as a transaction.
	 * 		Implemented by this or any subclass.
	 * @param params A string representing the arguments to the method.
	 * @return Whether the transaction was committed or aborted.
	 */
	public void sendTxnCommand(int[] participant_addrs, Command c,
			String params, Callback success, Callback failure) {
		
		// send rpc calls with TXN commands for handling commands
		
	}
	
	public void sendTxnGet(int[] participant_addrs, String filename,
			Callback success, Callback failure) {
		sendTxnCommand(participant_addrs, Command.GET, filename, success, failure);
	}
	
	public void sendTxnPut(int[] participant_addrs, String filename, String contents,
            Callback success, Callback failure) {
		sendTxnCommand(participant_addrs, Command.PUT, filename + " " + contents, success, failure);
	}
	
	public void sendTxnAppend(int[] participant_addrs, String filename, String contents,
            Callback success, Callback failure) {
		sendTxnCommand(participant_addrs, Command.APPEND, filename + " " + contents, success, failure);
	}
	
	public void sendTxnDelete(int[] participant_addrs, String filename,
            Callback success, Callback failure) {
		sendTxnCommand(participant_addrs, Command.DELETE, filename, success, failure);
	}
	
	@Override
	protected void handleRPCrequest(Integer from, RPCRequestPacket pkt) {
		
		// Build a TxnPacket class that knows how to parse itself from an RPCRequestPacket payload
		// Consider changing these commands to be protocols of this packet class instead
		
		Command request = pkt.getRequest();
        RPCResultPacket result;

        if (request == Command.TXN_PROP) {
        	// call method and respond with ack or nack
        } else if (request == Command.TXN_ACK) {
        	// log ack if thing being acked is valid
        } else if (request == Command.TXN_NACK) {
        	// send abort notices to participants
        } else if (request == Command.TXN_ABORT) {
        	// erase stuff from log
        } else if (request == Command.TXN_COMMIT) {
        	// execute command and delete from log on callback
        } else if (request == Command.TXN_LOG_TRIM) {
        	// delet the appropriate parts of the log
        }
        // If we get here then the command must not have been txn related
        super.handleRPCrequest(from, pkt);
	}
}
