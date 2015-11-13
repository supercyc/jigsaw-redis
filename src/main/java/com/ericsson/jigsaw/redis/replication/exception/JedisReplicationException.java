package com.ericsson.jigsaw.redis.replication.exception;


public class JedisReplicationException extends RuntimeException {

    private static final long serialVersionUID = -4348577718578091268L;

    public JedisReplicationException(String message) {
        super(message);
    }

    public JedisReplicationException(Throwable cause) {
        super(cause);
    }

    public JedisReplicationException(String message, Throwable cause) {
        super(message, cause);
    }


    @Override
    public String getMessage() {
        String template = "description: %s";

        String errorMsg = "";
        if (super.getMessage() != null) {
            errorMsg = super.getMessage();
        }

        return String.format(template, errorMsg);
    }
}
