/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.replication.exception.handler.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.jigsaw.redis.JigsawJedisException;
import com.ericsson.jigsaw.redis.replication.exception.handler.JedisExceptionHandler;

public class DefaultJedisExceptionHandler implements JedisExceptionHandler {

    private static Logger logger = LoggerFactory.getLogger(DefaultJedisExceptionHandler.class);

    /**
     * Only log in error level.
     */
    @Override
    public void handleException(JigsawJedisException exception) {
        if (exception.isJedisConnectionException()) {
            logger.error("Failed to connect to the Redis server. Pool: [{}], Address: [{}]",
                    exception.getJedisConnPoolName(), exception.getJedisConnAddress(), exception);
        } else if ((exception.getMessage() != null) && (exception.getMessage().indexOf("READONLY") != -1)) {
            logger.error("The Redis server is in read-only status as a slave. Pool: [{}], Address: [{}]",
                    exception.getJedisConnPoolName(), exception.getJedisConnAddress(), exception);
        } else {
            logger.error("The Redis server has data exception. Pool: [{}], Address: [{}]",
                    exception.getJedisConnPoolName(), exception.getJedisConnAddress(), exception);
        }
    }

}
