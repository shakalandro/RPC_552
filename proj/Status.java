public enum Status {
    SUCCESS("00 Success", 0), CRASH("01 Crash", 01), FAILURE("02 Failure", 02), NOT_EXIST(
            "10 File does not exist", 10), ALREADY_EXISTS(
            "11 File already exists", 11), TIMEOUT("20 Timeout", 20), TOO_LARGE(
            "30 File too large", 30);

    private final String msg;
    private final Integer code;

    private Status(String msg, Integer code) {
        this.msg = msg;
        this.code = code;
    }

    public static Status getStatus(int ordinal) {
        return Status.values()[ordinal];
    }

    public String getMsg() {
        return msg;
    }

    public Integer getCode() {
        return code;
    }
}
