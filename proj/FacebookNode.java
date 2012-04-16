import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import java.lang.reflect.*;

import edu.washington.cs.cse490h.lib.Callback;

public class FacebookNode extends RPCNode {
	public static double getFailureRate() { return 1/100.0; }
	public static double getDropRate() { return 25/100.0; }
	public static double getDelayRate() { return 50/100.0; }
	
	// The available facebook commands that can be entered by the user.
	private static final String CREATE_COMMAND = "create";
	private static final String LOGIN_COMMAND = "login";
	private static final String LOGOUT_COMMAND = "logout";
	private static final String FRIEND_COMMAND = "friend";
	private static final String VIEW_REQUESTS_COMMAND = "requests";
	private static final String ACCEPT_COMMAND = "accept";
	private static final String MESSAGE_COMMAND = "post";
	private static final String READ_COMMAND = "read";
	private static final String SHOW_USERS_COMMAND = "users";
	private static final String SHOW_FRIENDS_COMMAND = "mypals";	

	// File prefixes. Each of these is followed by the name of the user they belong to.
	private static final String FRIENDS_PREFIX = ".friends_";
	private static final String REQUESTS_PREFIX = ".requests_";
	private static final String MESSAGES_PREFIX = ".messages_";

	// Error codes for file RPC methods.
	private static final Integer CRASH = 1;
	private static final Integer FILE_NO_EXIST = 10;
	private static final Integer FILE_EXISTS = 11;
	private static final Integer TIMEOUT = 20;
	private static final Integer FILE_TOO_LARGE = 30;

	private static final int SERVER_ID = 0;

	private static final String ALL_USERS_FILE = ".users";

	public String loggedInUser = null;

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
				System.err.println("Usage: create user_name\nUser names must be a single word.");
				return;
			}

			String userName = commandScanner.next();

			// Check to make sure that the user name is only one word. This will help not confuse
			// users.
			if (commandScanner.hasNext()) {
				System.err.println("Usage: create user_name\nUser names must be a single word.");
				return;
			}

			try {
				createNewUser(userName);
			} catch (Exception e) {
				System.err.println("Exception: " + e.toString());
			}
		}

		else if (commandName.equals(LOGIN_COMMAND)) {
			if (!commandScanner.hasNext()) {
				System.err.println("Usage: login user_name");
				return;
			}
			String userName = commandScanner.next();

			try {
				loginUser(userName);
			} catch (Exception e) {
				e.printStackTrace();
			}
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

			try {
				requestFriend(userName);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		else if (commandName.equals(VIEW_REQUESTS_COMMAND)) {
			try {
				showRequests();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		else if (commandName.equals(ACCEPT_COMMAND)) {
			if (!commandScanner.hasNext()) {
				System.err.println("Usage: accept user_name");
				return;
			}

			String userName = commandScanner.next();
			try {
				acceptFriend(userName);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		else if (commandName.equals(MESSAGE_COMMAND)) {
			if (!commandScanner.hasNext()) {
				System.err.println("Usage: post message_contents");
				return;
			}

			StringBuilder messageBuilder = new StringBuilder();
			while (commandScanner.hasNext()) {
				messageBuilder.append(commandScanner.next());
			}
			try {
				postMessage(messageBuilder.toString());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		else if (commandName.equals(READ_COMMAND)) {
			try {
				readPosts();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		else if (commandName.equals(SHOW_USERS_COMMAND)) {
			try {
				showUsers();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		else if (commandName.equals(SHOW_FRIENDS_COMMAND)) {
			try {
				showFriends();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		else {
			System.err.println("Unrecognized command. Enter 'myface+ help' for available options.");
		}
	}

	// -------------------------CREATING NEW USER ----------------------//


	// Creates a new user on the system. 3 files are written for each user. Posts, friends, and
	// friend-requests.
	public void createNewUser(String userName) throws SecurityException, ClassNotFoundException,
			NoSuchMethodException {
		// Create a callback to be called if the user doesn't exist.
		String[] paramTypes = { "java.lang.Integer", "java.lang.String" };
		Method createFiles = Callback.getMethod("createPostsFile", this, paramTypes);
		Object[] params = { null, userName };
		Callback userNoExistCallback = new Callback(createFiles, this, params);

		// Create a callback to call if the user already exists.
		Method showWarning = Callback.getMethod("userAlreadyExists", this, new String[] {});
		Callback userExistsCallback = new Callback(showWarning, this, params);

		// Check to make sure that the user doesn't already exist. Look up a password file by this
		// name.
		userExists(null, userName, userExistsCallback, userNoExistCallback);
	}

	// Checks to see if userName is already in use. If so, calls the userExistsCallback. Otherwise,
	// calls the userNoExistsCallback.
	public void userExists(Integer errorCode, String userName, Callback userExistsCallback,
			Callback userNoExistsCallback) throws SecurityException, ClassNotFoundException, NoSuchMethodException {

		// Create a failure callback that just calls this method again.
		String[] failParamTypes =
				{ "java.lang.Integer", "java.lang.String",
						"edu.washington.cs.cse490h.lib.Callback",
						"edu.washington.cs.cse490h.lib.Callback" };
		Method tryAgain = Callback.getMethod("userExists", this, failParamTypes);
		Object[] failParams = { null, userName, userExistsCallback, userNoExistsCallback };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// If we failed because the .users file doesn't exist, then we must create it.
		if (errorCode != null && errorCode.equals(FILE_NO_EXIST)) {
			Object[] failureParams = tryAgainCallback.getParams();
			System.out.println("HOT DAMN");
			createUsersFile(null, userNoExistsCallback);
			return;
		}

		// We need to fetch the contents of the .users file. If we fail in making this fetch, just
		// repeat this method.
		// If we succeed, then we need to go on to a method that reads the contents of the file and
		// then fires off the appropriate callback.
		String[] pTypes =
				{ "java.lang.String", "java.lang.String", "edu.washington.cs.cse490h.lib.Callback",
						"edu.washington.cs.cse490h.lib.Callback" };
		Method checkForName = Callback.getMethod("checkFileForName", this, pTypes);
		Object[] p = { null, userName, userExistsCallback, userNoExistsCallback };
		Callback checkForNameCallback = new Callback(checkForName, this, p);
		get(SERVER_ID, ALL_USERS_FILE, checkForNameCallback, tryAgainCallback);
	}

	// Creates the meta .users file if hasn't been created yet. Then calls the
	// createUsersFilesCallback.
	public void createUsersFile(Integer errorCode, Callback createUserFilesCallback)
			throws SecurityException, ClassNotFoundException, NoSuchMethodException {

		// Create a failure callback that just calls this method again.
		String[] failParamTypes = { "java.lang.Integer", "edu.washington.cs.cse490h.lib.Callback" };
		Method tryAgain = Callback.getMethod("createUsersFile", this, failParamTypes);
		Object[] failParams = { null, createUserFilesCallback };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Try to create the users file.
		create(SERVER_ID, ALL_USERS_FILE, createUserFilesCallback, tryAgainCallback);
	}

	// Called if an attempt was made to create a user that already exists. We just spit out a
	// warning to the console.
	public void userAlreadyExists() {
		System.out.println("Sorry, a user by that name already exists. Might we suggest "
				+ "you be more creative?");
	}

	// Creates the posts file for a user.
	public void createPostsFile(Integer errorCode, String userName) throws SecurityException,
			ClassNotFoundException, NoSuchMethodException {

		// If we're calling because we failed, check if the failure is because the file already
		// exists.
		// If so, we can just move on to making the friends file.
		if (errorCode != null && errorCode.equals(FILE_EXISTS)) {
			createFriendsFile(null, userName);
		}

		// Create a failure callback that just tries this method once-more.
		String[] failParamTypes = { "java.lang.Integer", "java.lang.String" };
		Method tryAgain = Callback.getMethod("createPostsFile", this, failParamTypes);
		Object[] failParams = { null, userName };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Create a success callback that forwards control to creating the friends file.
		String[] successParamTypes = { "java.lang.Integer", "java.lang.String" };
		Method makeFriendsFile = Callback.getMethod("createFriendsFile", this, successParamTypes);
		Object[] successParams = { null, userName };
		Callback successCallback = new Callback(makeFriendsFile, this, successParams);

		// Invoke the create RPC call.
		String filename = MESSAGES_PREFIX + userName;
		create(SERVER_ID, filename, successCallback, tryAgainCallback);
	}

	public void createFriendsFile(Integer errorCode, String userName) throws SecurityException, ClassNotFoundException, NoSuchMethodException {

		// If we're calling because we failed, check if the failure is because the file already
		// exists.
		// If so, we can just move on to making the requests file.
		if (errorCode != null && errorCode.equals(FILE_EXISTS)) {
			createRequestsFile(null, userName);
		}

		// Create a failure callback that just tries this method once-more.
		String[] failParamTypes = { "java.lang.Integer", "java.lang.String" };
		Method tryAgain = Callback.getMethod("createFriendsFile", this, failParamTypes);
		Object[] failParams = { null, userName };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Create a success callback that forwards control to creating the requets file.
		String[] successParamTypes = { "java.lang.Integer", "java.lang.String" };
		Method makeFriendsFile = Callback.getMethod("createRequestsFile", this, successParamTypes);
		Object[] successParams = { null, userName };
		Callback successCallback = new Callback(makeFriendsFile, this, successParams);

		// Invoke the create RPC call.
		String filename = FRIENDS_PREFIX + userName;
		create(SERVER_ID, filename, successCallback, tryAgainCallback);
	}

	public void createRequestsFile(Integer errorCode, String userName) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		// If we're calling because we failed, check if the failure is because the file already
		// exists.
		// If so, we can just move on to appending the name to the list of existing users.
		if (errorCode != null && errorCode.equals(FILE_EXISTS)) {
			addUserToList(null, userName);
		}

		// Create a failure callback that just tries this method once-more.
		String[] failParamTypes = { "java.lang.Integer", "java.lang.String" };
		Method tryAgain = Callback.getMethod("createRequestsFile", this, failParamTypes);
		Object[] failParams = { null, userName };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Create a success callback that forwards control to appending to the users list.
		String[] successParamTypes = { "java.lang.Integer", "java.lang.String" };
		Method showSuccess = Callback.getMethod("addUserToList", this, successParamTypes);
		Object[] successParams = { null, userName };
		Callback successCallback = new Callback(showSuccess, this, successParams);

		// Invoke the create RPC call.
		String filename = REQUESTS_PREFIX + userName;
		create(SERVER_ID, filename, successCallback, tryAgainCallback);
	}

	// Adds the given user name to the secret list of all user names.
	public void addUserToList(Integer errorCode, String userName) throws SecurityException,
			ClassNotFoundException, NoSuchMethodException {
		
		// If we're getting back an error code indicating that the user list is too big, let the client know
		if (errorCode != null && errorCode.equals(FILE_TOO_LARGE)) {
			System.out.println("Sorry, the system can't handle any more users. " +
					"You're out of luck, friend.");
			return;
		}
		
		// Create a failure callback that just tries this method once-more.
		String[] failParamTypes = { "java.lang.Integer", "java.lang.String" };
		Method tryAgain = Callback.getMethod("addUserToList", this, failParamTypes);
		Object[] failParams = { null, userName };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Create a success callback that forwards control to a success message.
		String[] successParamTypes = { "java.lang.String" };
		Method showSuccess = Callback.getMethod("createSuccess", this, successParamTypes);
		Object[] successParams = { userName };
		Callback successCallback = new Callback(showSuccess, this, successParams);

		// Now attempt to append to the users file.
		String appendContents = " " + userName;
		append(SERVER_ID, ALL_USERS_FILE, appendContents, successCallback, tryAgainCallback);
	}

	// Prints a message confirming success in creating a new user.
	public void createSuccess(String userName) {
		System.out.println("Welcome to myface+, " + userName);
	}

	// ------------------------ USER LOGIN --------------------------------------//

	public void loginUser(String userName) throws SecurityException, ClassNotFoundException,
			NoSuchMethodException {
		// If already logged in.
		if (loggedInUser != null) {
			System.out.println("Sorry. Already logged-in as " + loggedInUser);
			return;
		}

		// Create a callback to use if the user doesn't exist.
		String[] noExistParamTypes = { "java.lang.String" };
		Method noExist = Callback.getMethod("noSuchUserReport", this, noExistParamTypes);
		Object[] noExistParams = { userName };
		Callback noExistCallback = new Callback(noExist, this, noExistParams);

		// Create a callback to use if the user does already exist.
		Method doesExist = Callback.getMethod("setLoggedInUser", this, noExistParamTypes);
		Callback doesExistCallback = new Callback(doesExist, this, noExistParams);

		userExists(null, userName, doesExistCallback, noExistCallback);
	}

	// Sets the logged in user to the given string.
	public void setLoggedInUser(String username) {
		loggedInUser = username;
	}

	public void noSuchUserReport(String username) {
		System.out.println("Bad move, bro. No such user by the name of " + username);
	}

	// ------------------------- USER LOGOUT ------------------------------//

	public void logoutUser() {
		if (loggedInUser == null) {
			System.out.println("In order to log OUT, young one, you must first log IN.");
		} else {
			System.out.println("Logging out, " + loggedInUser);
		}

		loggedInUser = null;
	}

	// ------------------------- REQUEST FRIENDS --------------------------//

	public void requestFriend(String userName) throws SecurityException, ClassNotFoundException,
			NoSuchMethodException {
		if (!confirmLoggedIn()) {
			return;
		}

		// Create a callback to do if person requested them first.
		String[] onListParamTypes = { "java.lang.String" };
		Method onList = Callback.getMethod("reportAlreadyRequested", this, onListParamTypes);
		Object[] onListParams = { userName };
		Callback onListCallback = new Callback(onList, this, onListParams);

		// Create a callback to do if the person hasn't requested this person yet.
		Method notOnList = Callback.getMethod("checkNotOnFriendList", this, onListParamTypes);
		Callback notOnListCallback = new Callback(notOnList, this, onListParams);

		// Check to make sure that user isn't trying to request a friend who has already requested
		// them.
		String filename = REQUESTS_PREFIX + loggedInUser;
		checkForNameInList(null, userName, filename, onListCallback, notOnListCallback);
	}
	
	public void reportAlreadyRequested(String user) {
		logOutput("There is already a friend request between you and " + user + " in existence.");
	}

	// Check to make sure that the person is not already on this user's list of friends.
	public void checkNotOnFriendList(String userName) throws SecurityException,
			ClassNotFoundException, NoSuchMethodException {
		// Create a callback to do if they are already on user's list of friends.
		String[] onListParamTypes = { "java.lang.String" };
		Method onList = Callback.getMethod("reportAlreadyFriend", this, onListParamTypes);
		Object[] onListParams = { userName };
		Callback onListCallback = new Callback(onList, this, onListParams);

		// Create a callback to do if they aren't already on user's list of friends.
		String[] newParamTypes = { "java.lang.Integer", "java.lang.String" };
		Method notOnList = Callback.getMethod("addToRequestsFile", this, newParamTypes);
		Object[] newParams = { null, userName };
		Callback notOnListCallback = new Callback(notOnList, this, newParams);

		String filename = FRIENDS_PREFIX + loggedInUser;
		checkForNameInList(null, userName, filename, onListCallback, notOnListCallback);
	}

	public void reportAlreadyFriend(String userName) {
		System.out.println(userName + " is already your buddy. BFF's forever!");
	}

	// Adds the name of the currently logged-in user to the requests file for the user specified
	// by userName.
	public void addToRequestsFile(Integer errorCode, String userName) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		if (errorCode != null && errorCode.equals(FILE_TOO_LARGE)) {
			System.out.println("Sorry, " + userName + " has too many pending friend requests.");
			return;
		}
		
		// Create a failure callback that just tries this method once-more.
		String[] failParamTypes = { "java.lang.Integer", "java.lang.String" };
		Method tryAgain = Callback.getMethod("addToRequestsFile", this, failParamTypes);
		Object[] failParams = { null, userName };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Create a success callback that forwards control to a success message.
		String[] successParamTypes = { "java.lang.String" };
		Method showSuccess = Callback.getMethod("requestSuccess", this, successParamTypes);
		Object[] successParams = { userName };
		Callback successCallback = new Callback(showSuccess, this, successParams);

		// Now attempt to append to the requests file for the person with the userName.
		String appendContents = " " + loggedInUser;
		String filename = REQUESTS_PREFIX + userName;
		append(SERVER_ID, filename, appendContents, successCallback, tryAgainCallback);
	}

	// Show this message when we've succesfully completed a friend request.
	public void requestSuccess(String userName) {
		System.out.println("Proposal for intimate, life-long friendship submitted to " + userName);
	}

	// ------------------------------------------ READ POSTS ----------------------- //

	public void readPosts() throws SecurityException, ClassNotFoundException,
			NoSuchMethodException {

		if (!confirmLoggedIn()) {
			return;
		}

		readAllPosts(null);
	}

	// Prints out the contents of the current user's posts file.
	public void readAllPosts(Integer errorCode) throws SecurityException, ClassNotFoundException,
			NoSuchMethodException {
		// Create a callback to try again in the case of failure.
		String[] failParamTypes = { "java.lang.Integer" };
		Method tryAgain = Callback.getMethod("readAllPosts", this, failParamTypes);
		Object[] failParams = { null };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Create a callback to call in the case of success.
		String[] goodParamTypes = { "java.lang.String" };
		Method printAllPosts = Callback.getMethod("printAllPosts", this, goodParamTypes);
		Object[] goodParams = { null };
		Callback printResultsCallback = new Callback(printAllPosts, this, goodParams);

		String filename = MESSAGES_PREFIX + loggedInUser;
		get(SERVER_ID, filename, printResultsCallback, tryAgainCallback);
	}
	
	public void printAllPosts(String fileContents) {
		Scanner in = new Scanner(fileContents);
		while (in.hasNextLine()) {
			System.out.println(in.nextLine());
		}
	}

	// ------------------------- POST MESSAGE ------------------------------------ //

	public void postMessage(String message) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		if (!confirmLoggedIn()) {
			return;
		}

		// First step to posting a message is to get a list of all of this user's friends.
		getFriendList(null, message);
	}

	// Fetches the friends list for this user. On success, passes control off to
	// addMessageToAllWalls.
	public void getFriendList(Integer errorCode, String message) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		// Create a callback to try again in the case of failure.
		String[] failParamTypes = { "java.lang.Integer", "java.lang.String" };
		Method tryAgain = Callback.getMethod("getFriendList", this, failParamTypes);
		Object[] failParams = { null, message };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Create a callback to call in the case of success.
		String[] goodParamTypes = { "java.lang.String", "java.lang.String" };
		Method addToWalls = Callback.getMethod("addMessageToAllWalls", this, goodParamTypes);
		Object[] goodParams = { null, message };
		Callback addToWallsCallback = new Callback(addToWalls, this, goodParams);

		String filename = FRIENDS_PREFIX + loggedInUser;
		get(SERVER_ID, filename, addToWallsCallback, tryAgainCallback);
	}

	public void addMessageToAllWalls(String friendList, String message) throws SecurityException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		// Get a non-duped set of all of my friends.
		Set<String> friends = new HashSet<String>();
		Scanner friendScanner = new Scanner(friendList);
		while (friendScanner.hasNext()) {
			friends.add(friendScanner.next());
		}

		// Convert this set of friends to a list.
		List<String> friendListList = new ArrayList<String>(friends);

		// We'll iteratively append this post on to each of my friend's post files.
		// Start with the first friend in the friendListList.
		addMessage(null, friendListList, 0, message);
	}

	public void addMessage(Integer errorCode, List<String> friendList, Integer friendIndex,
			String message) throws SecurityException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		// Create a callback to try again if we fail.
		String[] failParamTypes =
				{ "java.lang.Integer", "java.util.List<String>", "java.lang.Integer",
						"java.lang.String" };
		Method tryAgain = Callback.getMethod("addMessage", this, failParamTypes);
		Object[] failParams = { null, friendList, friendIndex, message };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// If this is the last friend in the list, create a callback to print a success message.
		// Otherwise, print a callback that will do this same method with the next friend in the
		// list.
		Callback successCallback;
		if (friendIndex.equals(friendList.size() - 1)) {
			String[] endParamTypes = {};
			Method printSuccess = Callback.getMethod("messagePostSuccess", this, endParamTypes);
			Object[] successParams = {};
			successCallback = new Callback(printSuccess, this, successParams);
		} else {
			Object[] nextFriendParams = { null, friendList, friendIndex + 1, message };
			successCallback = new Callback(tryAgain, this, nextFriendParams);
		}
		
		// If the current friend was unable to receive the post b/c they already have too many,
		// let the user know and move on to the next person.
		if (errorCode != null && errorCode.equals(FILE_TOO_LARGE)) {
			System.out.println("Sorry, " + friendList.get(friendIndex) + " has too many messages already.");
			
			// If we failed on the last person, invoke the success callback to move on to the completion messsage.
			// Otherwise, increment the friendIndex so that we can try with the next person.
			if (friendIndex.equals(friendList.size()-1)) {
				successCallback.invoke();
			} else {
				friendIndex++;
			}
		}

		// Format the message so that we append the user's name to the front and add a blank line
		// after it.
		message = loggedInUser + " says... " + message + "\n\n";
		String filename = MESSAGES_PREFIX + friendList.get(friendIndex);
		append(SERVER_ID, filename, message, successCallback, tryAgainCallback);
	}

	// ----------------------------------- LIST FRIEND REQUESTS-------------------------------- //

	public void showRequests() throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		if (!confirmLoggedIn()) {
			return;
		}

		// First step to posting a message is to get a list of all of this user's friends.
		String filename = FRIENDS_PREFIX + loggedInUser;
		showAllOfList(null, filename);
	}

	// ----------------------------------- ACCEPT FRIEND REQUEST -------------------------------- //

	public void acceptFriend(String userName) throws SecurityException, ClassNotFoundException,
			NoSuchMethodException {
		if (!confirmLoggedIn()) {
			return;
		}

		// Make a callback to print out a scolding message letting the user know that the person
		// isn't making
		// a friend request of them.
		String[] failParamTypes = { "java.lang.String" };
		Method failMessage = Callback.getMethod("scoldUser", this, failParamTypes);
		Object[] failParams = { userName };
		Callback notOnListCallback = new Callback(failMessage, this, failParams);

		// Make a callback that processes the friend acceptance.
		String[] goodParamTypes = {"java.lang.Integer", "java.lang.String" };
		Method addToFriends = Callback.getMethod("addToTheirFriends", this, goodParamTypes);
		Object[] goodParams = { null, userName };
		Callback onListCallback = new Callback(addToFriends, this, goodParams);

		String filename = REQUESTS_PREFIX + loggedInUser;
		checkForNameInList(null, userName, filename, onListCallback, notOnListCallback);
	}

	// Prints a message to let user know that can't accept a non-existent friend request.
	public void scoldUser(String username) {
		System.out.println(username + " don't wanna be yo friend.");
	}

	// Adds the current user to the list of friends for username. In case of success, control passes
	// on to addToMyFriends.
	public void addToTheirFriends(Integer errorCode, String username) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		
		// Check to see if appending would make file too large.
		if (errorCode != null && errorCode.equals(FILE_TOO_LARGE)) {
			System.out.println("Sorry, but " + username + " has too many friends already.");
			return;
		}
		
		// Create a callback to try again in the case of failure.
		String[] failParamTypes = { "java.lang.Integer", "java.lang.String" };
		Method tryAgain = Callback.getMethod("addToTheirFriends", this, failParamTypes);
		Object[] failParams = { null, username };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Create a callback to pass control onto addToMyFriends.
		Method continueMethod = Callback.getMethod("addToMyFriends", this, failParamTypes);
		Callback continuationCallback = new Callback(continueMethod, this, failParams);

		String filename = FRIENDS_PREFIX + username;
		String newContent = loggedInUser + " ";
		append(SERVER_ID, filename, newContent, continuationCallback, tryAgainCallback);
	}

	// Adds username to list of friends for current user.
	public void addToMyFriends(Integer errorCode, String username) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		// Check to see if appending would make file too large.
		if (errorCode != null && errorCode.equals(FILE_TOO_LARGE)) {
			System.out.println("Sorry, but you have too many friends.");
			return;
		}
		
		// Create a callback to try again in the case of failure.
		String[] failParamTypes = { "java.lang.Integer", "java.lang.String" };
		Method tryAgain = Callback.getMethod("addToMyFriends", this, failParamTypes);
		Object[] failParams = { null, username };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Create a callback to pass control onto removeFromRequestList.
		Method continueMethod = Callback.getMethod("getRequestList", this, failParamTypes);
		Callback continuationCallback = new Callback(continueMethod, this, failParams);

		String filename = FRIENDS_PREFIX + loggedInUser;
		String newContent = username + " ";
		append(SERVER_ID, filename, newContent, continuationCallback, tryAgainCallback);
	}

	public void getRequestList(Integer errorCode, String username) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		// Create a callback to try again in the case of failure.
		String[] failParamTypes = { "java.lang.Integer", "java.lang.String" };
		Method tryAgain = Callback.getMethod("getRequestList", this, failParamTypes);
		Object[] failParams = { null, username };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Create a callback to pass control onto removeFromRequestList.
		String[] goodParamTypes = { "java.lang.String", "java.lang.String" };
		Method continueMethod = Callback.getMethod("removeFromRequestList", this, goodParamTypes);
		Object[] goodParams = {null, username};
		Callback continuationCallback = new Callback(continueMethod, this, goodParams);

		String filename = REQUESTS_PREFIX + loggedInUser;
		get(SERVER_ID, filename, continuationCallback, tryAgainCallback);
	}

	public void removeFromRequestList(Integer errorCode, String fileContents, String username) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		Set<String> pendingRequests = new HashSet<String>();
		Scanner friendScanner = new Scanner(fileContents);
		while (friendScanner.hasNext()) {
			pendingRequests.add(friendScanner.next());
		}
		
		// Remove the person that we just accepted as a friend from our friend request list.
		pendingRequests.remove(username);

		// Write the new list of friend requests back out to a string.
		StringBuilder fixedFriendList = new StringBuilder();
		Iterator<String> listIterator = pendingRequests.iterator();
		while (listIterator.hasNext()) {
			fixedFriendList.append(listIterator.next() + " ");
		}
				
		// Create a callback to try again in the case of failure.
		String[] failParamTypes = { "java.lang.Integer", "java.lang.String", "java.lang.String" };
		Method tryAgain = Callback.getMethod("removeFromRequestList", this, failParamTypes);
		Object[] failParams = { null, fileContents, username };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Create a callback to pass control onto acceptedFriendSuccess.
		String[] goodParamTypes = {"java.lang.String"};
		Method continueMethod = Callback.getMethod("acceptedFriendSuccess", this, failParamTypes);
		Object[] goodParams = { username };
		Callback continuationCallback = new Callback(continueMethod, this, goodParams);

		// Now attempt to write over the old requests file with the cleaned up one.
		
		String filename = REQUESTS_PREFIX + loggedInUser;
		String newFileContents = fixedFriendList.toString();
		put(SERVER_ID, filename, newFileContents, continuationCallback, tryAgainCallback);
	}
	
	public void acceptedFriendSuccess(String username) {
		System.out.println("Happy Day! We've made " + username + " our special friend!");
	}

	// ----------------------------------- LIST ALL USERS -------------------------------- //

	public void showUsers() throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		showAllOfList(null, ALL_USERS_FILE);
	}
	
	// ----------------------------------- LIST ALL FRIENDS --------------------------- //
	public void showFriends() throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		if (!confirmLoggedIn()) {
			return;
		}
		
		String filename = FRIENDS_PREFIX + loggedInUser;
		showAllOfList(null, filename);
	}
	
	// ----------------------------------- UTILITY -------------------------------- //

	// Prints out a de-duped list of all friend requests that the current user has.
	public void showAllOfList(Integer errorCode, String filename) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		// Create a callback to try again in the case of failure.
		String[] failParamTypes = { "java.lang.Integer", "java.lang.String" };
		Method tryAgain = Callback.getMethod("showAllOfList", this, failParamTypes);
		Object[] failParams = { null, filename };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Create a callback to print the results once we've got 'em.
		String[] goodParamTypes = { "java.lang.String" };
		Method printResults = Callback.getMethod("printList", this, goodParamTypes);
		Object[] goodParams = { null };
		Callback printCallback = new Callback(printResults, this, goodParams);

		get(SERVER_ID, filename, printCallback, tryAgainCallback);
	}

	// Prints out the list with duplicates removed.
	public void printList(String happyList) {
		Set<String> pendingRequests = new HashSet<String>();
		Scanner friendScanner = new Scanner(happyList);
		while (friendScanner.hasNext()) {
			pendingRequests.add(friendScanner.next());
		}

		List<String> niceRequestList = new ArrayList<String>(pendingRequests);
		if (niceRequestList.isEmpty()) {
			System.out.println("The list is empty. Probably your own fault?");
		} else {
			System.out.println("\n");
			for (String person : niceRequestList) {
				System.out.println(person);
			}
		}
	}
	
	
	// If userName is present in fileContents, then calls the userExistsCallback. Otherwise calls
	// the userNoExistsCallback.
	public void checkFileForName(String fileContents, String userName,
			Callback userExistsCallback, Callback userNoExistsCallback)
			throws IllegalAccessException, InvocationTargetException {
		Scanner userScanner = new Scanner(fileContents);
		while (userScanner.hasNext()) {
			String nextName = userScanner.next();
			if (nextName.equals(userName)) {
				userExistsCallback.invoke();
			}
		}

		// User doesn't already exist.
		userNoExistsCallback.invoke();
	}

	// If not logged in, prints a helpful message and returns false.
	public boolean confirmLoggedIn() {
		if (loggedInUser == null) {
			System.out.println("Turn on. LOG IN. Drop out. Then you can do this kind of thing.");
			return false;
		}

		return true;
	}

	// Checks file by name of filename for userName. If userName is present, calls
	// userExistsCallback. Otherwise calls userNoExistsCallback.
	public void checkForNameInList(Integer errorCode, String userName, String filename,
			Callback userExistsCallback, Callback userNoExistsCallback) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		// Create a failure callback that just calls this method again.
		String[] failParamTypes =
				{ "java.lang.Integer", "java.lang.String", "java.lang.String",
						"edu.washington.cs.cse490h.lib.Callback",
						"edu.washington.cs.cse490h.lib.Callback" };
		Method tryAgain = Callback.getMethod("checkForNameInList", this, failParamTypes);
		Object[] failParams =
				{ null, userName, filename, userExistsCallback, userNoExistsCallback };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Create a success callback that checks the retrieved file for userName;
		String[] pTypes =
				{ "java.lang.String", "java.lang.String", "edu.washington.cs.cse490h.lib.Callback",
						"edu.washington.cs.cse490h.lib.Callback" };
		Method checkForName = Callback.getMethod("checkFileForName", this, pTypes);
		Object[] p = { null, userName, userExistsCallback, userNoExistsCallback };
		Callback checkForNameCallback = new Callback(checkForName, this, p);

		// We need to fetch the contents of the friends file for this user.
		get(SERVER_ID, filename, checkForNameCallback, tryAgainCallback);
	}
}
