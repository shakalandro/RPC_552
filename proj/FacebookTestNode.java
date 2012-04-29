import edu.washington.cs.cse490h.lib.Callback;

/*
 * This class allows us to script the facebook node by listening for the end of an rpc call and
 * then issuing a new request.
 * 
 * @author: Roy McElmurry
 * */

public class FacebookTestNode extends FacebookNode {
	public static double getFailureRate() { return 1/200.0; }
	public static double getRecoveryRate() { return 50/100.0; }
	public static double getDropRate() { return 10/100.0; }
	public static double getDelayRate() { return 10/100.0; }
	
	public static String BEGIN_COMMAND = "begin";
	public static int SERVER = 0;
	
	public static final String USER1 = "roy";
	public static final String USER2 = "stinkypete";
	
	public static enum State {
		START,
		CREATE1,
		CREATE2,
		SHOW_USERS,
		LOGIN1,
		REQUEST2,
		LOGOUT1,
		LOGIN2,
		SHOW_REQUESTS,
		ACCEPT_REQUEST,
		POST,
		LOGOUT2,
		LOGIN1_AGAIN,
		READ_POSTS,
		END;
	}
	
	private static State state = State.START;
	private boolean justBooted;
	
	
	@Override
	public void start() {
		super.start();
		printOutput("starting FacebookTestNode " + addr);
		printOutput("STATE is " + FacebookTestNode.state.name());
		if (addr != SERVER) {
			onCommand(BEGIN_COMMAND);
		}
		justBooted = true;
	}
	
	@Override
	public void onCommand(String command) {
		if (addr == SERVER) {
			printError("You cannot test as the server");
		} else if (command.equals(BEGIN_COMMAND)) {
			changeState();
		}
	}
	
	// Registers a callback to possibly make a new request
	public void registerCallback() {
		try {
			Callback cb = new Callback(Callback.getMethod("changeState", this, new String[0]),
									   this, new Object[0]);
			addTimeout(cb, 5);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	// Switch state a make a new request if the old one is done
	public void changeState() {
		if (!doingWork) {
			if (!justBooted) {
				FacebookTestNode.state = State.values()[Math.min(state.ordinal() + 1, State.END.ordinal())];
				printOutput("STATE is " + FacebookTestNode.state.name());
			}
			try {
				switch (FacebookTestNode.state) {
					case CREATE1:
						this.createNewUser(USER1); break;
					case CREATE2:
						this.createNewUser(USER2); break;
					case SHOW_USERS:
						this.showUsers(); break;
					case LOGIN1:
						this.loginUser(USER1); break;
					case REQUEST2:
						this.requestFriend(USER2); break; 
					case LOGOUT1:
						this.logoutUser(); break;
					case LOGIN2:
						this.loginUser(USER2); break;
					case SHOW_REQUESTS:
						this.showRequests(); break;
					case ACCEPT_REQUEST:
						this.acceptFriend(USER1); break;
					case POST:
						this.postMessage("HI " + USER1 + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"); break;
					case LOGOUT2:
						this.logoutUser();
					case LOGIN1_AGAIN:
						this.loginUser(USER1); break;
					case READ_POSTS:
						this.readPosts(); break;
					default:
						break;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
			justBooted = false;
		}
		if (state != State.END) {
			registerCallback();
		}
	}
}
