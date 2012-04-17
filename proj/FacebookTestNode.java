import edu.washington.cs.cse490h.lib.Callback;

/*
 * This class builds on the RPCNode to provide a set of commands that together represent a 
 * Facebook-like application.
 * 
 * @author: Greg Bigelow
 * */

public class FacebookTestNode extends FacebookNode {
	public static String BEGIN = "begin";
	public static int SERVER = 0;
	
	public static String USER1 = "roy";
	public static String USER2 = "stinkypete";
	
	public enum State {
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
		END;
	}
	
	private State state;
	
	@Override
	public void onCommand(String command) {
		if (addr == SERVER) {
			logError("You cannot test as the server");
		} else if (command.equals(BEGIN)) {
			state = State.START;
			changeState();
			registerCallback();					
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
			try {
				switch (state) {
					case START:
						this.createNewUser(USER1); break;
					case CREATE1:
						this.createNewUser(USER2); break;
					case CREATE2:
						this.showUsers(); break;
					case SHOW_USERS:
						this.loginUser(USER1); break;
					case LOGIN1:
						this.requestFriend(USER2); break; 
					case REQUEST2:
						this.logoutUser(); break;
					case LOGOUT1:
						this.loginUser(USER2); break;
					case LOGIN2:
						this.showRequests(); break;
					case SHOW_REQUESTS:
						this.acceptFriend(USER1); break;
					case ACCEPT_REQUEST:
						this.postMessage("HI " + USER1 + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"); break;
					case POST:
						this.logoutUser();
					case LOGOUT2:
						this.loginUser(USER1); break;
					case LOGIN1_AGAIN:
						this.readPosts(); break;
					default:
						break;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		state = State.values()[state.ordinal() + 1];
		if (state != State.END) {
			registerCallback();
		}
	}
}
