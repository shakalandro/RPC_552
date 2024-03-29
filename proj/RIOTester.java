import java.io.PrintStream;
import java.util.HashMap;
import java.util.Random;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.Utility;

/**
 * Class that tests the reliable, in-order message layer. Randomly sends
 * messages to other nodes and receives them in order from the RIO layer. This
 * continues until we send at least 100 messages to another node.
 */
public class RIOTester extends RIONode {
    // The RIO layer is not correct in the presence of node failures.
    public static double getFailureRate() {
        return 1 / 100.0;
    }

    public static double getDropRate() {
        return 25 / 100.0;
    }

    public static double getDelayRate() {
        return 50 / 100.0;
    }

    private HashMap<Integer, Integer> receivedNums;
    private HashMap<Integer, Integer> nextNum;
    private Random randNumGen;
    private int numFinished;

    public static int NUM_NODES = 10;
    public static int NUM_MESSAGES = 1000;

    private boolean failed = false;

    @Override
    public String packetBytesToString(byte[] bytes) {
        RIOPacket packet = RIOPacket.unpack(bytes);
        if (packet == null) {
            return super.packetBytesToString(bytes);
        }
        return packet.toString();
    }

    @Override
    public void start() {
        logOutput("Starting up...");

        // Generate a user-level synoptic event to indicate that the node
        // started.
        logSynopticEvent("started");

        receivedNums = new HashMap<Integer, Integer>();
        nextNum = new HashMap<Integer, Integer>();
        for (int i = 0; i < RIOTester.NUM_NODES; ++i) {
            nextNum.put(i, 0);
            receivedNums.put(i, -1);
        }

        randNumGen = new Random();
        numFinished = 0;
    }

    @Override
    public void onCommand(String command) {
        if (command.equals("begin")) {
            sendNextNum();
        }
        return;
    }

    @Override
    public void onRIOReceive(Integer from, int protocol, byte[] msg) {
        if (protocol != Protocol.RIOTEST_PKT) {
            logError("unknown protocol: " + protocol);
            return;
        }
        Integer i = Integer.parseInt(Utility.byteArrayToString(msg));
        Integer receivedNum = receivedNums.get(from);
        if (i <= receivedNum) {
            failure(from, i);
        }
    }

    /**
     * Called when we have received a packet in the correct order.
     * 
     * @param from
     *            The address of the sender
     * @param i
     *            The number of the packet received.
     */
    public void correctReceive(int from, int i) {
        logOutput("Correctly Received " + i + " from " + from);
        receivedNums.put(from, i);
    }

    /**
     * Called when we have received a packet from the underlying layer out of
     * order.
     * 
     * @param from
     *            The address of the sender
     * @param i
     *            The number of the packet received.
     */
    public void failure(int from, int i) {
        logError("FAILURE OF THE RIO MESSAGE LAYER!!  Received " + i
                + " instead of " + (receivedNums.get(from)  + 1) + " from " + from);
        receivedNums.put(from, i);
        failed = true;
        fail();
    }

    /**
     * Method that chooses a random receiver to send the next message, and
     * schedules itself to execute again in the next time step
     */
    public void sendNextNum() {
        int destAddr;
        int next;
        boolean doAgain;

        do {
            // choose a destination that we have not sent NUM_MESSAGES messages
            // to yet
            doAgain = false;
            destAddr = randNumGen.nextInt(RIOTester.NUM_NODES);
            next = nextNum.get(destAddr);
            if (next == -1) {
                doAgain = true;
            }
            if (next == NUM_MESSAGES + 1) {
                // We just sent NUM_MESSAGES to this destination
                nextNum.put(destAddr, -1);
                if (++numFinished == RIOTester.NUM_NODES) {
                    // If we are finished with everything then stop and don't
                    // schedule another timeout
                    return;
                }
                doAgain = true;
            }
        } while (doAgain);

        // Send the message
        RIOSend(destAddr, Protocol.RIOTEST_PKT,
                Utility.stringToByteArray("" + next));
        nextNum.put(destAddr, next + 1);

        // Schedule another send for the next time step
        try {
            Callback cb = new Callback(Callback.getMethod("sendNextNum", this,
                    new String[0]), this, new Object[0]);
            addTimeout(cb, 1);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void logError(String output) {
        log(output, System.err);
    }

    public void logOutput(String output) {
        log(output, System.out);
    }

    public void log(String output, PrintStream stream) {
        stream.println("Node " + addr + ": " + output);
    }

    @Override
    public String toString() {
        if (failed) {
            return "FAILED!!!\n" + super.toString();
        }
        return super.toString();
    }
}
