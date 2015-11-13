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

import redis.clients.jedis.exceptions.JedisException;

import com.ericsson.jigsaw.redis.JedisTemplate;
import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.pool.JedisPoolBuilder;
import com.ericsson.jigsaw.redis.replication.ReplicaConnectInfo;
import com.ericsson.jigsaw.redis.replication.ReplicaContext;
import com.ericsson.jigsaw.redis.replication.ReplicaService;
import com.ericsson.jigsaw.redis.replication.exception.handler.JedisExceptionHandler;

public class RemoteReplicaDataSourceImpl extends AbstractReplicaDataSource {

    private int index;

    private String appName;

    private ReplicaService replicaService;

    private JedisTemplate remoteTemplate;

    private JedisExceptionHandler jedisExceptionHandler;

    private static Logger logger = LoggerFactory.getLogger(RemoteReplicaDataSourceImpl.class);

    public RemoteReplicaDataSourceImpl(String appName, ReplicaService replicaService, int index,
            JedisExceptionHandler jedisExceptionHandler) {
        this.appName = appName;
        this.replicaService = replicaService;
        this.index = index;
        this.jedisExceptionHandler = jedisExceptionHandler;
    }

    @Override
    public ReplicaConnectInfo getConnectInfo() {
        final String connectInfo = this.replicaService.getConnectInfo(appName, index);
        logger.debug("Get remote redis connect info {}", connectInfo);
        return ReplicaConnectInfo.fromString(connectInfo);
    }

    @Override
    public boolean isSyncedUp() {
        return replicaService.isSyncedUp(appName, index);
    }

    @Override
    protected JedisTemplate getDataTemplate() {
        if (this.remoteTemplate == null) {
            logger.debug("Create new JedisTemplate");
            final ReplicaConnectInfo connectInfo = this.getConnectInfo();
            final JedisPool pool = JedisPoolBuilder.newInstance().setHosts(connectInfo.getAddress().getHost())
                    .setMasterName("direct:" + connectInfo.getAddress()).setDatabase(connectInfo.getDatabase())
                    .setPoolSize(1).setPassword(ReplicaContext.getRedisPassword()).buildPool();
            this.remoteTemplate = new JedisTemplate(pool, jedisExceptionHandler);
        }
        return this.remoteTemplate;
    }

    @Override
    public void stopSyncUp() {
        this.replicaService.stopSyncUp(appName, index);
    }

    @Override
    public void startSyncUp() {
        ReplicaUtiils.checkHealthy(this.getDataTemplate());
        this.replicaService.startSyncUp(appName, index);
    }

    @Override
    protected void handleConnectionExcetpion(@SuppressWarnings("unused") JedisException e) {
        logger.warn("catch Excepton, reset template");
        this.remoteTemplate = null;
    }
}
