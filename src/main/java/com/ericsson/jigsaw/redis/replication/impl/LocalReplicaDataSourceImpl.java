/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.replication.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.jigsaw.redis.JedisTemplate;
import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.replication.ReplicaConnectInfo;
import com.ericsson.jigsaw.redis.replication.exception.handler.JedisExceptionHandler;

public class LocalReplicaDataSourceImpl extends AbstractReplicaDataSource {

    private static Logger logger = LoggerFactory.getLogger(LocalReplicaDataSourceImpl.class);

    private JedisTemplate dataTemplate;

    public LocalReplicaDataSourceImpl(JedisPool dataPool, JedisExceptionHandler jedisExceptionHandler) {
        this.dataTemplate = new JedisTemplate(dataPool, jedisExceptionHandler);
    }

    @Override
    public ReplicaConnectInfo getConnectInfo() {
        JedisPool jedisPool = this.getDataTemplate().getJedisPool();

        return new ReplicaConnectInfo(jedisPool.getAddress(), jedisPool.getConnectionInfo().getDatabase());
    }

    @Override
    protected JedisTemplate getDataTemplate() {
        return this.dataTemplate;
    }

}
