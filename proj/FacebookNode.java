import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;

import edu.washington.cs.cse490h.lib.Callback;
import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

/*
 * This class builds on the RPCNode to provide a set of commands that together represent a 
 * Facebook-like application.
 * 
 * @author: Greg Bigelow
 * */

public class FacebookNode extends TransactionNode {
	public static double getFailureRate() { return 0/100.0; }
	public static double getRecoveryRate() { return 100/100.0; }
	public static double getDropRate() { return 0/100.0; }
	public static double getDelayRate() { return 0/100.0; }
	
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
	
	// Temp file prefix for a wall-post transaction.
	private static final String WALL_POST_TEMP_PREFIX = ".wall_temp_";

	// Error codes for file RPC methods.
	private static final Integer CRASH = 1;
	private static final Integer FILE_NO_EXIST = 10;
	private static final Integer FILE_EXISTS = 11;
	private static final Integer TIMEOUT = 20;
	private static final Integer FILE_TOO_LARGE = 30;

	// The id of the node from which commands will be sent.
	public static final int CLIENT_ID = 1;
	
	// The number of servers we'll have on the system. By convention, they have even numbered id's
	// starting at 0. Also by convention, the .users file is stored on server 0.
	private static final int NUM_SERVERS = 3;
	private static final int ALL_USERS_LOCATION = 0;

	// Name of the file, stored on the client machine, that specifies the names of each user and the id of the
	// server storing their files.
	private static final String ALL_USERS_FILE = ".users";
	
	// Fields determining which server to next store a new user's data on and a map from usernames to 
	// the server storing their data. This is populated on start-up of the client node.
	private Integer nextServer = null;
	private Map<String, Integer> userDataLocations = null;

	public String loggedInUser = null;
	
	public boolean doingWork = false;

	@Override
	public void start() {
		super.start();
		
		// If I'm the client, I need to figure out where each user's data is stored.
		if (addr == CLIENT_ID) {
			getDataLocations(null);
		}
	}
	
	// Fetches meta-data about the locations of each user's data (which server is the data located on.)
	public void getDataLocations(Integer errorCode) {
		// Do a get on the .users file.
		// Create a failure callback that just calls this method again.
		String[] failParamTypes = {"java.lang.Integer"};
		Object[] failParams = { null };
		Callback tryAgainCallback = createCallback("getDataLocations", failParamTypes, failParams);

		// If we failed because the .users file doesn't exist, then set up everything up in a fresh state.
		if (errorCode != null && errorCode.equals(FILE_NO_EXIST)) {
			nextServer = 0;
			userDataLocations = new HashMap<String, Integer>();
			printOutput("Client Node Initialized and Accepting Your Commands!");
			return;
		}

		// We need to fetch the contents of the .users file. If we fail in making this fetch, just
		// repeat this method.
		// If we succeed, then we need to go on to a method that reads the contents of the file and
		// populates the meta-data fields correctly.
		String[] goodParamTypes = { "java.lang.String" };
		Object[] p = { null };
		Callback goodCallback = createCallback("populateDataInfo", goodParamTypes, p);
		get(ALL_USERS_LOCATION, ALL_USERS_FILE, goodCallback, tryAgainCallback);
	}

	// Adds mappings from user's to the servers that are storing their data into our local storage.
	public void populateDataInfo(String usersFile) {
		int currentServer = 0;
		userDataLocations = new HashMap<String, Integer>();
		Scanner fileReader = new Scanner(usersFile);
		while (fileReader.hasNext()) {
			String userName = fileReader.next();
			userDataLocations.put(userName, currentServer);
			
			// Servers are assigned in round-robin fashion.
			currentServer = (currentServer + 2) % (NUM_SERVERS * 2);
		}
		
		// The next server that we'll use when storing the meta-dta for a new user will be the next server in the round-robin
		// distribution.
		nextServer = currentServer;
		
		printOutput("Client Node Initialized and Accepting Your Commands!");
	}
	
	@Override
	public void onCommand(String command) {
		// Parse the commands and see if we have any matches to available commands.
		Scanner commandScanner = new Scanner(command);

		// If we haven't figured out the data locations yet, we can't accept commands.
		if (userDataLocations == null) {
			printError("System not yet started. Try again later.");
			return;
		}
		
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
			
			// Find the message content.
			int messageStart = command.indexOf(' ');
			String messageContent = command.substring(messageStart+1);
			messageContent = loggedInUser + " says... " + messageContent + "\n\n";

			try {
				postMessage(messageContent);
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
			System.err.println("Unrecognized command.");
		}
	}

	// -------------------------CREATING NEW USER ----------------------//


	// Creates a new user on the system. 3 files are written for each user. Posts, friends, and
	// friend-requests.
	public void createNewUser(String userName) throws SecurityException, ClassNotFoundException,
			NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		doingWork = true;
		
		// Create a callback to be called if the user doesn't exist.
		String[] paramTypes = { "java.lang.Integer", "java.lang.String" };
		Method createFiles = Callback.getMethod("createPostsFile", this, paramTypes);
		Object[] params = { null, userName };
		Callback userNoExistCallback = new Callback(createFiles, this, params);

		// Create a callback to call if the user already exists.
		String[] noParamTypes = {};
		Object[] noParams = {};
		Method showWarning = Callback.getMethod("userAlreadyExists", this, noParamTypes);
		Callback userExistsCallback = new Callback(showWarning, this, noParams);

		// Check to make sure that the user doesn't already exist. Look up a password file by this
		// name.
		userExists(null, userName, userExistsCallback, userNoExistCallback);
	}

	// Checks to see if userName is already in use. If so, calls the userExistsCallback. Otherwise,
	// calls the userNoExistsCallback.
	public void userExists(Integer errorCode, String userName, Callback userExistsCallback,
			Callback userNoExistsCallback) throws SecurityException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

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
			//logOutput(".user file doesn't exist. Creating now.");
			createUsersFile(null, userNoExistsCallback);
			return;
		}
		
		// We might not have to do an RPC. If we have information about some user's already read in to memory
		// and the current username is not present, then we know the user doesn't exist.
		// If the user name is read into memory, then we know the user does exist.
		if (userDataLocations.keySet().size() > 0) {
			if (!userDataLocations.containsKey(userName)) {
				userNoExistsCallback.invoke();
			} else {
				userExistsCallback.invoke();
			}
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
		get(ALL_USERS_LOCATION, ALL_USERS_FILE, checkForNameCallback, tryAgainCallback);
	}

	// Creates the meta .users file if hasn't been created yet. Then calls the
	// createUsersFilesCallback.
	public void createUsersFile(Integer errorCode, Callback createUserFilesCallback)
			throws SecurityException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

		// If we failed because the user's file is already created, just continue
		// onwards with the createUserFilesCallback.
		if (errorCode != null && errorCode.equals(FILE_EXISTS)) {
			// Calls createPostsFile
			createUserFilesCallback.invoke();
			return;
		}
				
		// Create a failure callback that just calls this method again.
		String[] failParamTypes = { "java.lang.Integer", "edu.washington.cs.cse490h.lib.Callback" };
		Method tryAgain = Callback.getMethod("createUsersFile", this, failParamTypes);
		Object[] failParams = { null, createUserFilesCallback };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Try to create the users file.
		create(ALL_USERS_LOCATION, ALL_USERS_FILE, createUserFilesCallback, tryAgainCallback);
	}

	// Called if an attempt was made to create a user that already exists. We just spit out a
	// warning to the console.
	public void userAlreadyExists() {
		doingWork = false;
		printError("Sorry, a user by that name already exists. Might we suggest "
				+ "you be more creative?");
	}

	// Creates the posts file for a user.
	public void createPostsFile(Integer errorCode, String userName) throws SecurityException,
			ClassNotFoundException, NoSuchMethodException {

		// We need to figure out which server we're going to store this user's meta-data on.
		if (!userDataLocations.containsKey(userName)) {
			userDataLocations.put(userName, nextServer);
			nextServer = (nextServer + 2) % (2 * NUM_SERVERS);
		}
		
		// If we're calling because we failed, check if the failure is because the file already
		// exists.
		// If so, we can just move on to making the friends file.
		if (errorCode != null && errorCode.equals(FILE_EXISTS)) {
			createFriendsFile(null, userName);
			return;
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
		create(userDataLocations.get(userName), filename, successCallback, tryAgainCallback);
	}

	public void createFriendsFile(Integer errorCode, String userName) throws SecurityException, ClassNotFoundException, NoSuchMethodException {

		// If we're calling because we failed, check if the failure is because the file already
		// exists.
		// If so, we can just move on to making the requests file.
		if (errorCode != null && errorCode.equals(FILE_EXISTS)) {
			createRequestsFile(null, userName);
			return;
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
		create(userDataLocations.get(userName), filename, successCallback, tryAgainCallback);
	}

	public void createRequestsFile(Integer errorCode, String userName) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		// If we're calling because we failed, check if the failure is because the file already
		// exists.
		// If so, we can just move on to appending the name to the list of existing users.
		if (errorCode != null && errorCode.equals(FILE_EXISTS)) {
			addUserToList(null, userName);
			return;
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
		create(userDataLocations.get(userName), filename, successCallback, tryAgainCallback);
	}

	// Adds the given user name to the secret list of all user names.
	public void addUserToList(Integer errorCode, String userName) throws SecurityException,
			ClassNotFoundException, NoSuchMethodException {
		
		// If we're getting back an error code indicating that the user list is too big, let the client know
		if (errorCode != null && errorCode.equals(FILE_TOO_LARGE)) {
			printError("Sorry, the system can't handle any more users. " +
					"You're out of luck, friend.");
			doingWork = false;
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
		append(ALL_USERS_LOCATION, ALL_USERS_FILE, appendContents, successCallback, tryAgainCallback);
	}

	// Prints a message confirming success in creating a new user.
	public void createSuccess(String userName) {
		doingWork = false;
		printOutput("Welcome to myface+, " + userName);
	}

	// ------------------------ USER LOGIN --------------------------------------//

	public void loginUser(String userName) throws SecurityException, ClassNotFoundException,
			NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		doingWork = true;
		// If already logged in.
		if (loggedInUser != null) {
			printError("Sorry. Already logged-in as " + loggedInUser);
			doingWork = false;
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
		//loggedInUser = userName;
	}

	// Sets the logged in user to the given string.
	public void setLoggedInUser(String username) {
		loggedInUser = username;
		printOutput("Have a nice myface+ session, " + username);
		doingWork = false;
	}

	public void noSuchUserReport(String username) {
		doingWork = false;
		printError("Bad move, bro. No such user by the name of " + username);
	}

	// ------------------------- USER LOGOUT ------------------------------//

	public void logoutUser() {
		doingWork = true;
		if (loggedInUser == null) {
			printError("In order to log OUT, young one, you must first log IN.");
		} else {
			printOutput("Logging out, " + loggedInUser);
		}

		loggedInUser = null;
		doingWork = false;
	}

	// ------------------------- REQUEST FRIENDS --------------------------//

	public void requestFriend(String userName) throws SecurityException, ClassNotFoundException,
			NoSuchMethodException {
		doingWork = true;
		if (!confirmLoggedIn()) {
			doingWork = false;
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
		checkForNameInList(null, userName, filename, userDataLocations.get(loggedInUser), onListCallback, notOnListCallback);
	}
	
	public void reportAlreadyRequested(String user) {
		doingWork = false;
		printOutput("There is already a friend request between you and " + user + " in existence.");
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
		checkForNameInList(null, userName, filename, userDataLocations.get(loggedInUser), onListCallback, notOnListCallback);
	}

	public void reportAlreadyFriend(String userName) {
		doingWork = false;
		printError(userName + " is already your buddy. BFF's forever!");
	}

	// Adds the name of the currently logged-in user to the requests file for the user specified
	// by userName.
	public void addToRequestsFile(Integer errorCode, String userName) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		if (errorCode != null && errorCode.equals(FILE_TOO_LARGE)) {
			printError("Sorry, " + userName + " has too many pending friend requests.");
			doingWork = false;
			return;
		}
		
		// If file doesn't exist, then just say that the person doesn't exist and move on.
		if (errorCode != null && errorCode.equals(FILE_NO_EXIST)) {
			printError("Sorry, there is no such user named " + userName);
			doingWork = false;
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
		append(userDataLocations.get(userName), filename, appendContents, successCallback, tryAgainCallback);
	}

	// Show this message when we've succesfully completed a friend request.
	public void requestSuccess(String userName) {
		doingWork = false;
		printOutput("Proposal for intimate, life-long friendship submitted to " + userName);
	}

	// ------------------------------------------ READ POSTS ----------------------- //

	public void readPosts() throws SecurityException, ClassNotFoundException,
			NoSuchMethodException {
		doingWork = true;
		if (!confirmLoggedIn()) {
			doingWork = false;
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
		get(userDataLocations.get(loggedInUser), filename, printResultsCallback, tryAgainCallback);
	}
	
	public void printAllPosts(String fileContents) {
		Scanner in = new Scanner(fileContents);
		while (in.hasNextLine()) {
			System.out.println(in.nextLine());
		}
		doingWork = false;
	}

	// ------------------------- POST MESSAGE ------------------------------------ //

	public void postMessage(String message) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		doingWork = true;
		if (!confirmLoggedIn()) {
			doingWork = false;
			return;
		}

		// First step to posting a message is to get a list of all of this user's friends.
		getFriendList(null, message);
	}
	
	/**
	 * Called by the transaction manager to determine if we want to commit or abort a wall post.
	 * If there is a current transaction already in progress, then we will reject. Otherwise, accept.
	 */
	public boolean proposeWallPost(UUID txnId, String args) {
		return numUnfinishedTxns() == 0;
	}
	
	// Temp file names take the form of WALL_POST_TEMP_PREFIX + name_of_user + || + txnId.
	private static String getWallTempName(UUID txnId, String userName) {
		return WALL_POST_TEMP_PREFIX + userName + "||" + txnId;
	}
			
	/**
	 * Called if we have decided to go ahead with posting onto this wall. We'll do the posting
	 * such that it is guaranteed to eventually succeed as long as this method is called until completion.
	 * @throws IOException 
	 */
	public void commitWallPost(UUID txnId, String args) throws IOException {
		Object[] objArgs = parseArgs(args);
		
		@SuppressWarnings("unchecked")
		List<String> nameList = (List<String>) objArgs[0];
		String message = (String) objArgs[1];
		
		// Delete any temp files from previous transactions.
		List<File> oldTemps = Utility.getMatchingFiles(addr, WALL_POST_TEMP_PREFIX);
		for (File f : oldTemps) {
			String txnIdString = txnId.toString();
			String fileTxnString = f.getName().substring(f.getName().indexOf("||"));
			if (!fileTxnString.equals(txnIdString)) {
				// It's a temp file for a previous transaction. We can get rid of it.
				System.out.println("Deleting temp file " + f.getName());
				this.getWriter(f.getName(), false).delete();
			}
		}
		
		// For each of the target names that I have stored on my disk...
		for (String name : nameList) {
			if (Utility.fileExists(this, MESSAGES_PREFIX + name)) {
				
				// See if a temp file for this transaction already exists and has content.
				String tempFileName = getWallTempName(txnId, name);
				boolean tempFileWritten;
				PersistentStorageReader tempReader = null;
				if (Utility.fileExists(this, tempFileName)) {
					tempReader = this.getReader(tempFileName);
					tempFileWritten = tempReader.ready();
				} else {
					tempFileWritten = false;
				}
				
				if (tempFileWritten) {
					// Read in the temp file contents and then write those contents to the old posts
					// file.
					assert(tempReader != null);
					char[] buf = new char[MAX_FILE_SIZE];
					tempReader.read(buf, 0, MAX_FILE_SIZE);
					PersistentStorageWriter postsFileWriter = this.getWriter(MESSAGES_PREFIX + name, false);
					postsFileWriter.write(buf);
				} else {
					// The temp file is either created but not written or not created at all. In either case, 
					// it does not contain a version with the appended message. So we need to append the message ont
					// the wall posts file and then write that to a temp file before then writing it to the actual 
					// file.
					assert(Utility.fileExists(this, MESSAGES_PREFIX + name));
					PersistentStorageReader masterFileReader = this.getReader(MESSAGES_PREFIX + name);
					char[] buf = new char[MAX_FILE_SIZE];
					int length = masterFileReader.read(buf, 0, MAX_FILE_SIZE);
					
					String contents;
					if (length == -1) {
						contents = "";
					} else {
						contents = new String(buf, 0, length);
					}
					contents += message;
					
					// Write message contents to the temp file.
					// Then write the message contents to the master file. If we go down in-between, we can
					// still recover nicely!
					PersistentStorageWriter tempWriter = this.getWriter(tempFileName, false);
					tempWriter.write(contents);
					PersistentStorageWriter masterWriter = this.getWriter(MESSAGES_PREFIX + name, false);
					masterWriter.write(contents);
				}
			}
		}
		
		// If I'm the client, then print a success message.
		if (addr == CLIENT_ID) {
			printOutput("Writing to walls in progress!");
		}
	}
	
	/**
	 * Called when the distributed transaction decides to abort. If we are the client node,
	 * then print a failure message to output and try the entire operation again.
	 * @throws InvocationTargetException 
	 * @throws IllegalAccessException 
	 * @throws NoSuchMethodException 
	 * @throws ClassNotFoundException 
	 * @throws SecurityException 
	 */
	public void abortWallPost(UUID txnId, String args) throws SecurityException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
		if (addr == CLIENT_ID) {
			printError("Failure to write to walls. Trying again now.");
			
			// Since we already have the friend list and such, we can actually skip ahead and
			// use the info we already have without having to do a GET for the friend list.
			Object[] info = parseArgs(args);
			
			@SuppressWarnings("unchecked")
			List<String> friends = (List<String>) info[0];
			String friendString = StringUtils.join(friends, ' ');
			String message = (String) info[1];
			addMessageToAllWalls(friendString, message);
		}
	}
	
	// Return an arg String for wall posting that is a packaged up combination of a list of 
	// names of people to post to and the message that the client wants to post.
	private String packageArgs(List<String> names, String message) {
		String[] nameArray = (String[]) names.toArray();
		String gluedNames = StringUtils.join(nameArray, ";");
		return gluedNames + "||" + message;
	}
	
	/**
	 * Returns an object array of length 2 where the first element is a List<String> containing
	 * a list of all the users to post the message to and the second element is a String with the message
	 * to post.
	 */
	public Object[] parseArgs(String args) {
		Object[] result = new Object[2];
		String[] parts = args.split("||");
		String[] names = parts[0].split(";");
		List<String> nameList = new ArrayList<String>(Arrays.asList(names));
		
		result[0] = nameList;
		result[1] = parts[1];
		return result;
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
		get(userDataLocations.get(loggedInUser), filename, addToWallsCallback, tryAgainCallback);
	}

	public void addMessageToAllWalls(String friendList, String message) throws SecurityException {
		// Get a non-duped set of all of my friends.
		Set<String> friends = new HashSet<String>();
		Scanner friendScanner = new Scanner(friendList);
		while (friendScanner.hasNext()) {
			friends.add(friendScanner.next());
		}

		// Convert this set of friends to a list.
		List<String> friendListList = new ArrayList<String>(friends);
		
		if (friendListList.isEmpty()) {
			printError("Bummer. You don't have any friends! Might as well not be posting at all.");
			doingWork = false;
			return;
		}

		// Prepare the arguments for propose transaction.
		Set<Integer> participantAddrs = new HashSet<Integer>();
		for (String friend : friendListList) {
			participantAddrs.add(userDataLocations.get(friend));
		}
		String requestName = "WallPost";
		
		String args = packageArgs(friendListList, message);
		proposeTransaction(participantAddrs, requestName, args);
	}

	// ----------------------------------- LIST FRIEND REQUESTS-------------------------------- //

	public void showRequests() throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		doingWork = true;
		if (!confirmLoggedIn()) {
			doingWork = false;
			return;
		}

		// First step to posting a message is to get a list of all of this user's friends.
		String filename = REQUESTS_PREFIX + loggedInUser;
		showAllOfList(null, filename, userDataLocations.get(loggedInUser));
	}

	// ----------------------------------- ACCEPT FRIEND REQUEST -------------------------------- //

	public void acceptFriend(String userName) throws SecurityException, ClassNotFoundException,
			NoSuchMethodException {
		doingWork = true;
		if (!confirmLoggedIn()) {
			doingWork = false;
			return;
		}

		// Make a callback to print out a scolding message letting the user know that the person
		// isn't making a friend request of them.
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
		checkForNameInList(null, userName, filename, userDataLocations.get(loggedInUser), onListCallback, notOnListCallback);
	}

	// Prints a message to let user know that can't accept a non-existent friend request.
	public void scoldUser(String username) {
		doingWork = false;
		printError(username + " don't wanna be yo friend.");
	}

	// Adds the current user to the list of friends for username. In case of success, control passes
	// on to addToMyFriends.
	public void addToTheirFriends(Integer errorCode, String username) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		// Check to see if appending would make file too large.
		if (errorCode != null && errorCode.equals(FILE_TOO_LARGE)) {
			printError("Sorry, but " + username + " has too many friends already.");
			doingWork = false;
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
		append(userDataLocations.get(username), filename, newContent, continuationCallback, tryAgainCallback);
	}

	// Adds username to list of friends for current user.
	public void addToMyFriends(Integer errorCode, String username) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		// Check to see if appending would make file too large.
		if (errorCode != null && errorCode.equals(FILE_TOO_LARGE)) {
			printError("Sorry, but you have too many friends.");
			doingWork = false;
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
		append(userDataLocations.get(loggedInUser), filename, newContent, continuationCallback, tryAgainCallback);
	}

	public void getRequestList(Integer errorCode, String username) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		// Create a callback to try again in the case of failure.
		String[] failParamTypes = { "java.lang.Integer", "java.lang.String" };
		Method tryAgain = Callback.getMethod("getRequestList", this, failParamTypes);
		Object[] failParams = { null, username };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Create a callback to pass control onto removeFromRequestList.
		String[] goodParamTypes = {"java.lang.String", "java.lang.String" };
		Method continueMethod = Callback.getMethod("removeFromRequestList", this, goodParamTypes);
		Object[] goodParams = {null, username};
		Callback continuationCallback = new Callback(continueMethod, this, goodParams);

		String filename = REQUESTS_PREFIX + loggedInUser;
		get(userDataLocations.get(loggedInUser), filename, continuationCallback, tryAgainCallback);
	}

	public void removeFromRequestList(String fileContents, String username) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
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
				
		// Create a callback to try from the requests method in the case of failure.
		String[] failParamTypes = { "java.lang.Integer", "java.lang.String" };
		Method tryAgain = Callback.getMethod("getRequestList", this, failParamTypes);
		Object[] failParams = { null, username };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Create a callback to pass control onto acceptedFriendSuccess.
		String[] goodParamTypes = {"java.lang.String"};
		Method continueMethod = Callback.getMethod("acceptedFriendSuccess", this, goodParamTypes);
		Object[] goodParams = { username };
		Callback continuationCallback = new Callback(continueMethod, this, goodParams);

		// Now attempt to write over the old requests file with the cleaned up one.
		
		String filename = REQUESTS_PREFIX + loggedInUser;
		String newFileContents = fixedFriendList.toString();
		put(userDataLocations.get(loggedInUser), filename, newFileContents, continuationCallback, tryAgainCallback);
	}
	
	public void acceptedFriendSuccess(String username) {
		doingWork = false;
		printOutput("Happy Day! We've made " + username + " our special friend!");
	}

	// ----------------------------------- LIST ALL USERS -------------------------------- //

	public void showUsers() throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		doingWork = true;
		showAllOfList(null, ALL_USERS_FILE, ALL_USERS_LOCATION);
	}
	
	// ----------------------------------- LIST ALL FRIENDS --------------------------- //
	public void showFriends() throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		doingWork = true;
		if (!confirmLoggedIn()) {
			doingWork = false;
			return;
		}
		
		String filename = FRIENDS_PREFIX + loggedInUser;
		showAllOfList(null, filename, userDataLocations.get(loggedInUser));
	}
	
	// ----------------------------------- UTILITY -------------------------------- //

	// Prints out a de-duped list of all friend requests that the current user has.
	public void showAllOfList(Integer errorCode, String filename, Integer serverId) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
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

		get(serverId, filename, printCallback, tryAgainCallback);
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
			printError("The list is empty. Probably your own fault?");
		} else {
			System.out.println("\n");
			for (String person : niceRequestList) {
				System.out.println(person);
			}
		}
		doingWork = false;
	}

	
	// If userName is present in fileContents, then calls the userExistsCallback. Otherwise calls
	// the userNoExistsCallback.
	public void checkFileForName(String fileContents, String userName,
			Callback userExistsCallback, Callback userNoExistsCallback)
			throws IllegalAccessException, InvocationTargetException {
		Scanner userScanner = new Scanner(fileContents);
		while (userScanner.hasNext()) {
			String nextName = userScanner.next();
			if (userName.equals(nextName)) {
				userExistsCallback.invoke();
				return;
			}
		}

		// User doesn't already exist.
		userNoExistsCallback.invoke();
	}

	// If not logged in, prints a helpful message and returns false.
	public boolean confirmLoggedIn() {
		if (loggedInUser == null) {
			printError("Turn on. LOG IN. Drop out. Then you can do this kind of thing.");
			return false;
		}

		return true;
	}

	// Checks file by name of filename for userName. If userName is present, calls
	// userExistsCallback. Otherwise calls userNoExistsCallback.
	public void checkForNameInList(Integer errorCode, String userName, String filename, Integer serverId,
			Callback userExistsCallback, Callback userNoExistsCallback) throws SecurityException, ClassNotFoundException, NoSuchMethodException {
		// Create a failure callback that just calls this method again.
		String[] failParamTypes =
				{ "java.lang.Integer", "java.lang.String", "java.lang.String", "java.lang.Integer",
						"edu.washington.cs.cse490h.lib.Callback",
						"edu.washington.cs.cse490h.lib.Callback" };
		Method tryAgain = Callback.getMethod("checkForNameInList", this, failParamTypes);
		Object[] failParams =
				{ null, userName, filename, null, userExistsCallback, userNoExistsCallback };
		Callback tryAgainCallback = new Callback(tryAgain, this, failParams);

		// Create a success callback that checks the retrieved file for userName;
		String[] pTypes =
				{ "java.lang.String", "java.lang.String", "edu.washington.cs.cse490h.lib.Callback",
						"edu.washington.cs.cse490h.lib.Callback" };
		Method checkForName = Callback.getMethod("checkFileForName", this, pTypes);
		Object[] p = { null, userName, userExistsCallback, userNoExistsCallback };
		Callback checkForNameCallback = new Callback(checkForName, this, p);

		// We need to fetch the contents of the friends file for this user.
		get(serverId, filename, checkForNameCallback, tryAgainCallback);
	}
	
    public void printError(String output) {
    	log(output, System.err, COLOR_CYAN);
    }

    public void printOutput(String output) {
    	log(output, System.out, RPCNode.COLOR_PURPLE);
    }
 
}
