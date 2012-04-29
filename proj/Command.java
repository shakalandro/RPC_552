

public enum Command {
    CREATE, GET, PUT, APPEND, DELETE, SESSION,
    // Transactions commands
    TXN_PROP, TXN_ACK, TXN_NACK, TXN_ABORT, TXN_COMMIT, TXN_LOG_TRIM;

    public static Command getCommand(int ordinal) {
        return Command.values()[ordinal];
    }
}