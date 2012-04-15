import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import java.lang.reflect.*;

import edu.washington.cs.cse490h.lib.Callback;

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

	// File prefixes. Each of these is followed by the name of the user they belong to.
	private static final String FRIENDS_PREFIX = ".friends_;";
	private static final String REQUESTS_PREFIX = ".requests_";
	private static final String MESSAGES_PREFIX = ".messages_";

	// Error codes for file RPC methods.
	private static final Integer FILE_NO_EXIST = 10;
	private static final Integer FILE_EXISTS = 11;
	private static final Integer TIMEOUT = 20;
	private static final Integer CRASH = 40;

	private static final String SERVER_ID = "5";

	private static final String ALL_USERS_FILE = ".users";

	private String loggedInUser = null;

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

			loginUser(userName);
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
				messageBuilder.append(commandScanner.next());
			}
			postMessage(messageBuilder.toString());
		}

		else if (commandName.equals(READ_COMMAND)) {
			readPosts();
		}

		else {
			System.err.println("Unrecognized command. Enter 'myface help' for available options.");
		}
	}

	// -------------------------CREATING NEW USER ----------------------//

	// Creates a new user on the system. 3 files are written for each user. Posts, friends, and
	// friend-requests.
	private void createNewUser(String userName) throws SecurityException, ClassNotFoundException,
			NoSuchMethodException {
		// Create a callback to be called if the user doesn't exist.
		String[] paramTypes = { "java.lang.Integer", "java.lang.String" };
		Method createFiles = Callback.getMethod("createPostsFile", this, paramTypes);
		Object[] params = { null, userName };
		Callback userNoExistCallback = new Callback(createFiles, this, params);

		// Create a callback to call if the user already exists.
		Method showWarning = Callback.getMethod("userAlreadyExists", this, paramTypes);
		Callback userExistsCallback = new Callback(showWarning, this, params);

		// Check to make sure that the user doesn't already exist. Look up a password file by this
		// name.
		userExists(null, userName, userExistsCallback, userNoExistCallback);
	}

	// Checks to see if userName is already in use. If so, calls the userExistsCallback. Otherwise,
	// calls the userNoExistsCallback.
	private void userExists(Integer errorCode, String userName, Callback userExistsCallback,
			Callback userNoExistsCallback) {

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
	private void createUsersFile(Integer errorCode, Callback createUserFilesCallback)
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
	private void userAlreadyExists() {
		System.out.println("Sorry, a user by that name already exists. Might we suggest "
				+ "you be more creative?");
	}

	// Creates the posts file for a user.
	private void createPostsFile(Integer errorCode, String userName) throws SecurityException,
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

	private void createFriendsFile(Integer errorCode, String userName) {

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

	private void createRequestsFile(Integer errorCode, String userName) {
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
	private void addUserToList(Integer errorCode, String userName) throws SecurityException,
			ClassNotFoundException, NoSuchMethodException {
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
	private void createSuccess(String userName) {
		System.out.println("Welcome to MyFace, " + userName);
	}

	// ------------------------ USER LOGIN --------------------------------------//

	private void loginUser(String userName) throws SecurityException, ClassNotFoundException,
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
	private void setLoggedInUser(String username) {
		loggedInUser = username;
	}

	private void noSuchUserReport(String username) {
		System.out.println("Bad move, bro. No such user by the name of " + username);
	}

	// ------------------------- USER LOGOUT ------------------------------//

	private void logoutUser() {
		if (loggedInUser == null) {
			System.out.println("In order to log OUT, young one, you must first log IN.");
		} else {
			System.out.println("Logging out, " + loggedInUser);
		}

		loggedInUser = null;
	}

	// ------------------------- REQUEST FRIENDS --------------------------//

	private void requestFriend(String userName) throws SecurityException, ClassNotFoundException,
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

	// Check to make sure that the person is not already on this user's list of friends.
	private void checkNotOnFriendList(String userName) throws SecurityException,
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

	private void reportAlreadyFriend(String userName) {
		System.out.println(userName + " is already your buddy. BFF's forever!");
	}

	// Adds the name of the currently logged-in user to the requests file for the user specified
	// by userName.
	private void addToRequestsFile(Integer errorCode, String userName) {

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
	private void requestSuccess(String userName) {
		System.out.println("Proposal for intimate, life-long friendship submitted to " + userName);
	}

	// ------------------------------------------ READ POSTS ----------------------- //

	private void readPosts() throws SecurityException, ClassNotFoundException,
			NoSuchMethodException {
		
		if (!confirmLoggedIn()) {
			return;
		}

		readAllPosts(null);
	}

	// Prints out the contents of the current user's posts file.
	private void readAllPosts(Integer errorCode) throws SecurityException, ClassNotFoundException,
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
	
	
	// ------------------------- POST MESSAGE ------------------------------------ //

	private void postMessage(String string) {
		if (!confirmLoggedIn()) {
			return;
		}
	}
	
	
	
	
	// - each user also has a file of “posts”
	// - get each and be sure to de-dupe it.
	// - when post a message to all friends, goes through the list of friends and writes into each of
	// their “posts” files.
	// Print the user name along with the post.
	// Append a blank line after each post.

	
	
	
	
	
	
	

	private void acceptFriend(String userName) {
		// TODO Auto-generated method stub

	}

	private void showRequests() {
		// TODO Auto-generated method stub

	}

	// If userName is present in fileContents, then calls the userExistsCallback. Otherwise calls
	// the userNoExistsCallback.
	private void checkFileForName(String fileContents, String userName,
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
	private boolean confirmLoggedIn() {
		if (loggedInUser == null) {
			System.out.println("Turn on. LOG IN. Drop out. Then you can do this kind of thing.");
			return false;
		}

		return true;
	}

	// Checks file by name of filename for userName. If userName is present, calls
	// userExistsCallback. Otherwise calls userNoExistsCallback.
	private void checkForNameInList(Integer errorCode, String userName, String filename,
			Callback userExistsCallback, Callback userNoExistsCallback) {
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

//
// Accept a friend
// - do de-duping so that people who have requested you multiple times still only show up once on
// the list.

// - command to get a list of outstanding friend requests

// - if say accept X:
// - write your name to their list of friends
// - write their name to your list.
// - remove that name (if it is there) and then write the whole file again without that name there
//
// Post a message to all friends
// - each user also has a file of “posts”
// - get each and be sure to de-dupe it.
// - when post a message to all friends, goes through the list of friends and writes into each of
// their “posts” files.
// Print the user name along with the post.
// Append a blank line after each post.

//
