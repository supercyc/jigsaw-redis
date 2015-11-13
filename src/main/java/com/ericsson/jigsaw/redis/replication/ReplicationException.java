/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.replication;

public class ReplicationException extends RuntimeException {

    private static final long serialVersionUID = 6913915240235283827L;

    private int statusCode = 500;

    public ReplicationException() {
        super();
    }

    public ReplicationException(String message, Throwable cause) {
        super(message, cause);
    }

    public ReplicationException(String message, int statusCode) {
        super(message);
        this.statusCode = statusCode;
    }

    public ReplicationException(String message) {
        super(message);
    }

    public ReplicationException(Throwable cause) {
        super(cause);
    }

    public int getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(int statusCode) {
        this.statusCode = statusCode;
    }
}
