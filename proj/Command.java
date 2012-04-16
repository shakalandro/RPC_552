

public enum Command {
    CREATE, GET, PUT, APPEND, DELETE, SESSION;

    public static Command getCommand(int ordinal) {
        return Command.values()[ordinal];
    }
}