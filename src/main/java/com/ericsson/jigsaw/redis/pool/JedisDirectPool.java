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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Pool which connect to redis instance directly.
 */
public class JedisDirectPool extends JedisPool {

    private static Logger logger = LoggerFactory.getLogger(JedisDirectPool.class);

    public JedisDirectPool(HostAndPort address, JedisPoolConfig config) {
        this(address, new ConnectionInfo(), config);
    }

    public JedisDirectPool(HostAndPort address, ConnectionInfo connectionInfo, JedisPoolConfig config) {
        initInternalPool(address, connectionInfo, config);
        logger.info("Create redis direct pool, address:" + address + ", connection info:" + connectionInfo);
    }
}
