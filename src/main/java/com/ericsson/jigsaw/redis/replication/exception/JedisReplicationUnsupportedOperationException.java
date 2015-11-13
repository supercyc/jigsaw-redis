package com.ericsson.jigsaw.redis.replication.exception;


public class JedisReplicationUnsupportedOperationException extends RuntimeException {

    private static final long serialVersionUID = -4348577718578091268L;

    public JedisReplicationUnsupportedOperationException(String message) {
        super(message);
    }

    public JedisReplicationUnsupportedOperationException(Throwable cause) {
        super(cause);
    }

    public JedisReplicationUnsupportedOperationException(String message, Throwable cause) {
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
