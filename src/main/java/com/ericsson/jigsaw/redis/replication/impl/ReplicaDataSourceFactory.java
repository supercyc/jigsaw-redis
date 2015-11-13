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

import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.pool.JedisSentinelPool;
import com.ericsson.jigsaw.redis.replication.ReplicaDataSource;
import com.ericsson.jigsaw.redis.replication.ReplicaService;
import com.ericsson.jigsaw.redis.replication.exception.handler.JedisExceptionHandler;

public class ReplicaDataSourceFactory {

    public static ReplicaDataSource createLocalReplicaDataSource(JedisPool jedisPool,
            JedisExceptionHandler jedisExceptionHandler) {
        if (jedisPool instanceof JedisSentinelPool) {
            return new SentinelReplicaDataSourceImpl((JedisSentinelPool) jedisPool, jedisExceptionHandler);
        }
        return new LocalReplicaDataSourceImpl(jedisPool, jedisExceptionHandler);
    }

    public static ReplicaDataSource createRemoteReplicaDataSource(String appName, ReplicaService replicaService,
            int index, JedisExceptionHandler jedisExceptionHandler) {
        return new RemoteReplicaDataSourceImpl(appName, replicaService, index, jedisExceptionHandler);
    }
}
