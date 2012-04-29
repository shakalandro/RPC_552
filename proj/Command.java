

public enum Command {
    CREATE, GET, PUT, APPEND, DELETE, SESSION, TXN;

    public static Command getCommand(int ordinal) {
        return Command.values()[ordinal];
    }
}