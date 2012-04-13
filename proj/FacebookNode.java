import java.util.Scanner;


public class FacebookNode extends RIONode {
	// The available facebook commands that can be entered by the user.
	private static final String CREATE_COMMAND = "create";
	private static final String LOGIN_COMMAND = "login";
	private static final String LOGOUT_COMMAND = "logout";
	private static final String FRIEND_COMMAND = "friend";
	private static final String VIEW_REQUESTS_COMMAND = "requests";
	private static final String ACCEPT_COMMAND = "accept";
	private static final String MESSAGE_COMMAND = "post";
	private static final String READ_COMMAND = "read";
		
	@Override
	public void onRIOReceive(Integer from, int protocol, byte[] msg) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onCommand(String command) {
		// Parse the commands and see if we have any matches to available commands.
		Scanner commandScanner = new Scanner(command);
		
		if (!commandScanner.hasNext()) {
			System.err.println("No command given.");
			return;
		}
		
		String commandName = commandScanner.next();
		if (commandName.equals(CREATE_COMMAND)) {
			if (!commandScanner.hasNext()) {
				System.err.println("Usage: create user_name");
				return;
			}
			
			String userName = commandScanner.next();
			createNewUser(userName);	
		} 
		
		else if (commandName.equals(LOGIN_COMMAND)) {
			if (!commandScanner.hasNext()) {
				System.err.println("Usage: login user_name password");
				return;
			}
			String userName = commandScanner.next();
			
			if (!commandScanner.hasNext()) {
				System.err.println("Usage: login user_name password");
				return;
			}
			String password = commandScanner.next();
			
			loginUser(userName, password);
		}
		
		else if (commandName.equals(LOGOUT_COMMAND)) {		
			logoutUser();
		}
		
		else if (commandName.equals(FRIEND_COMMAND)) {
			if (!commandScanner.hasNext()) {
				System.err.println("Usage: friend user_name");
				return;
			}
			String userName = commandScanner.next();

			requestFriend(userName);
		}
		
		else if (commandName.equals(VIEW_REQUESTS_COMMAND)) {
			showRequests();
		}
		
		else if (commandName.equals(ACCEPT_COMMAND)) {
			if (!commandScanner.hasNext()) {
				System.err.println("Usage: accept user_name");
				return;
			}
			
			String userName = commandScanner.next();
			acceptFriend(userName);
		}
		
		else if (commandName.equals(MESSAGE_COMMAND)) {
			if (!commandScanner.hasNext()) {
				System.err.println("Usage: post message_contents");
				return;
			}
			
			StringBuilder messageBuilder = new StringBuilder();
			while (commandScanner.hasNext()) {
			String messageContents = commandScanner.next();
			acceptFriend(userName);
		}
		
		
		
	}
}
Create a user
- 4 files for each user (posts, friends, friend-requests, password)
- write the password file last

Login/logout as user
- check if password file exists. if so, read the password and make sure matches with user
- if logged-in, save the user name as a string.
- when logged out, just set this to null
- confirm that the user name actually exists
- assuming that node failure should result in the user being automagically logged out

Request a friend
- check to make sure that they aren’t already on your list of friends
- append to the end of the request file for the person you want to be your friend

Accept a friend
- do de-duping so that people who have requested you multiple times still only show up once on the list.
- command to get a list of outstanding friend requests
- if say accept X:
- write your name to their list of friends
- write their name to your list.
- remove that name (if it is there) and then write the whole file again without that name there

Post a message to all friends
- each user also has a file of “posts”
- get each and be sure to de-dupe it.
- when post a message to all friends, goes through the list of friends and writes into each of their “posts” files. 

Read all messages posted
- just read the list.