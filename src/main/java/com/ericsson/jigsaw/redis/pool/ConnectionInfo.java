/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.pool;

import redis.clients.jedis.Protocol;

/**
 * All information for redis connection.
 */
public class ConnectionInfo {

    public static final String DEFAULT_PASSWORD = null;
    public static final String DEFAULT_POOLNAME = "NotSpecified";

    private int database = Protocol.DEFAULT_DATABASE;
    private String password = DEFAULT_PASSWORD;
    private int timeout = Protocol.DEFAULT_TIMEOUT;
    private String poolName = DEFAULT_POOLNAME;

    public ConnectionInfo() {
    }

    public ConnectionInfo(String poolName) {
        if (poolName != null) {
            this.poolName = poolName;
        }
    }

    public ConnectionInfo(int database, String password, int timeout) {
        this.timeout = timeout;
        this.password = password;
        this.database = database;
    }

    public ConnectionInfo(int database, String password, int timeout, String poolName) {
        this(database, password, timeout);
        if (poolName != null) {
            this.poolName = poolName;
        }
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public String getPoolName() {
        return poolName;
    }

    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConnectionInfo that = (ConnectionInfo) o;

        if (database != that.database) {
            return false;
        }
        if (timeout != that.timeout) {
            return false;
        }
        if (password != null ? !password.equals(that.password) : that.password != null) {
            return false;
        }
        if (poolName != null ? !poolName.equals(that.poolName) : that.poolName != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = database;
        result = 31 * result + (password != null ? password.hashCode() : 0);
        result = 31 * result + timeout;
        result = 31 * result + (poolName != null ? poolName.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ConnectionInfo [database=" + database + ", password=" + password + ", timeout=" + timeout
                + ", poolName=" + poolName + "]";
    }
}
