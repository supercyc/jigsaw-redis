package com.ericsson.jigsaw.redis.replication.exception.handler;

import com.ericsson.jigsaw.redis.JigsawJedisException;

/**
 * Use to handle the JedisException.
 */
public interface JedisExceptionHandler {

    void handleException(JigsawJedisException jedisException);

}
