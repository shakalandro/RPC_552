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
	
	@Override
	public void onCommand(String command) {
		if (addr == SERVER) {
			logError("You cannot test as the server");
		} else if (command.equals(BEGIN)) {
			try {
				this.createNewUser(USER1);
				this.createNewUser(USER2);

				this.showUsers();
				this.loginUser(USER1);
				
				this.loggedInUser = USER1;
				this.requestFriend(USER2);
		//		this.logoutUser();
			//	this.loginUser(USER2);

				this.loggedInUser = USER2;
				this.showRequests();
				this.acceptFriend(USER1);
				this.postMessage("HI " + USER1 + "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
				//this.logoutUser();
				//this.loginUser(USER1);
				this.loggedInUser = USER1;
				this.readPosts();
			} catch (Exception e) {
				e.printStackTrace();
			}			
		}
	}
}
