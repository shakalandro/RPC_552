

public enum Status {
    SUCCESS("00 Success"),
    CRASH("01 Crash"),
    FAILURE("02 Failure"),
    NOT_EXIST("10 File does not exist"),
    ALREADY_EXISTS("11 File already exists"),
    TIMEOUT("20 Timeout"),
    TOO_LARGE("30 File too large");

    public final String CODE;

    private Status(String code) {
        this.CODE = code;
    }

    public static Status getStatus(int ordinal) {
        return Status.values()[ordinal];
    }

    public String toString() {
        return CODE;
    }
}
