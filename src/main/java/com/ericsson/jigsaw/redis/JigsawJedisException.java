/*
 * ----------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 * ----------------------------------------------------------------------
 */

package com.ericsson.jigsaw.redis;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

/**
 * This class is a wrapper class of JedisException, with more information like the ip,port,poolName.
 * <p/>
 * All the operation in JedisTemplate will throw JigsawJedisException.
 */
public class JigsawJedisException extends JedisException {

    private static final long serialVersionUID = -8893087988923001155L;

    private HostAndPort jedisConnAddress;
    private String jedisConnPoolName;

    public JigsawJedisException(Throwable e) {
        super(e);
    }

    public JigsawJedisException(Throwable e, HostAndPort addr, String poolName) {
        super(e);
        initJedisConnInfo(addr, poolName);
    }

    public JigsawJedisException(String message) {
        super(message);
    }

    public JigsawJedisException(String message, HostAndPort addr, String poolName) {
        super(message);
        initJedisConnInfo(addr, poolName);
    }

    public JigsawJedisException(String message, Throwable cause) {
        super(message, cause);
    }

    public JigsawJedisException(String message, Throwable cause, HostAndPort addr, String poolName) {
        super(message, cause);
        initJedisConnInfo(addr, poolName);
    }

    private void initJedisConnInfo(HostAndPort addr, String poolName) {
        this.jedisConnAddress = addr;
        this.jedisConnPoolName = poolName;
    }

    public HostAndPort getJedisConnAddress() {
        return jedisConnAddress;
    }

    public String getJedisConnPoolName() {
        return jedisConnPoolName;
    }

    public JedisException getInternalJedisException() {
        Throwable e = getCause();
        if (e instanceof JedisException) {
            return (JedisException) e;
        }

        return null;
    }

    public boolean isJedisDataException() {
        JedisException e = getInternalJedisException();
        return (e != null) ? (e instanceof JedisDataException) : false;
    }

    public boolean isJedisConnectionException() {
        JedisException e = getInternalJedisException();
        return (e != null) ? (e instanceof JedisConnectionException) : false;
    }

    public boolean isConnectionBroken() {
        if (isJedisConnectionException()) {
            return true;
        }

        if (isJedisDataException()) {
            if ((getMessage() != null) && (getMessage().indexOf("READONLY") != -1)) {
                return true;
            }
        }

        // never goto here.
        return false;
    }
}
