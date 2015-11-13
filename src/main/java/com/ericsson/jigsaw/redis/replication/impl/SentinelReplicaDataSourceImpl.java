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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

import com.ericsson.jigsaw.redis.JedisUtils;
import com.ericsson.jigsaw.redis.JigsawJedisException;
import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.pool.JedisSentinelPool;
import com.ericsson.jigsaw.redis.replication.SentinelReplicaDataSource;
import com.ericsson.jigsaw.redis.replication.exception.handler.JedisExceptionHandler;

public class SentinelReplicaDataSourceImpl extends LocalReplicaDataSourceImpl implements SentinelReplicaDataSource {

    private static Logger logger = LoggerFactory.getLogger(SentinelReplicaDataSourceImpl.class);

    public SentinelReplicaDataSourceImpl(JedisSentinelPool dataPool, JedisExceptionHandler jedisExceptionHandler) {
        super(dataPool, jedisExceptionHandler);
    }

    public JedisSentinelPool getDataPool() {
        return (JedisSentinelPool) super.getDataTemplate().getJedisPool();
    }

    public JedisExceptionHandler getJedisExceptionHandler() {
        return super.getDataTemplate().getJedisExceptionHandler();
    }

    @Override
    public boolean isSyncedUp() {
        final JedisSentinelPool jedisSentinelPool = getDataPool();
        final String masterName = jedisSentinelPool.getMasterName();
        final HostAndPort masterAddress = jedisSentinelPool.getAddress();
        final List<JedisPool> sentinelPools = jedisSentinelPool.getSentinelPools();
        for (JedisPool sentinelPool : sentinelPools) {
            Jedis sentinelJedis = null;
            boolean broken = false;

            try {
                sentinelJedis = sentinelPool.getResource();
                List<String> address = sentinelJedis.sentinelGetMasterAddrByName(masterName);
                if ((address != null) && (address.size() == 2)) {
                    HostAndPort masterAddrInSentinel = new HostAndPort(address.get(0), Integer.valueOf(address.get(1)));
                    if (!masterAddrInSentinel.equals(masterAddress)) {
                        logger.warn("master address {} in sentinel is not the address {} in jedis pool",
                                masterAddrInSentinel, masterAddress);
                        return false;
                    }
                } else {
                    logger.warn("sentinel doesn't monitor master {} yet", masterName);
                    return false;
                }
            } catch (JedisConnectionException e) {
                JigsawJedisException exception = new JigsawJedisException(e, sentinelPool.getAddress(),
                        sentinelPool.getFormattedPoolName());
                broken = true;

                getJedisExceptionHandler().handleException(exception);
                throw exception;
            } finally {
                JedisUtils.closeResource(sentinelPool, sentinelJedis, broken);
            }
        }
        return true;
    }

    @Override
    public void monitoredBySentinel() {
        final JedisSentinelPool jedisSentinelPool = getDataPool();
        final HostAndPort masterAddress = jedisSentinelPool.getAddress();
        final String masterName = jedisSentinelPool.getMasterName();
        final List<JedisPool> sentinelPools = jedisSentinelPool.getSentinelPools();
        for (JedisPool sentinelPool : sentinelPools) {
            Jedis sentinelJedis = null;
            boolean broken = false;

            try {
                sentinelJedis = sentinelPool.getResource();
                sentinelJedis.sentinelMonitor(masterName, masterAddress.getHost(), masterAddress.getPort(), 1);
                logger.info(jedisSentinelPool.getFormattedPoolName() + " is monitored by sentinel "
                        + sentinelPool.getFormattedPoolName());
            } catch (JedisConnectionException e) {
                JigsawJedisException exception = new JigsawJedisException(e, sentinelPool.getAddress(),
                        sentinelPool.getFormattedPoolName());
                broken = true;

                getJedisExceptionHandler().handleException(exception);
                throw exception;
            } finally {
                JedisUtils.closeResource(sentinelPool, sentinelJedis, broken);
            }
        }
    }

    @Override
    public void removeFromSentinel() {
        JedisSentinelPool jedisSentinelPool = getDataPool();
        final List<JedisPool> sentinelPools = jedisSentinelPool.getSentinelPools();
        final String masterName = jedisSentinelPool.getMasterName();
        for (JedisPool sentinelPool : sentinelPools) {
            Jedis sentinelJedis = null;
            boolean broken = false;
            try {
                sentinelJedis = sentinelPool.getResource();
                sentinelJedis.sentinelRemove(masterName);
                logger.info(jedisSentinelPool.getFormattedPoolName() + " was removed from sentinel "
                        + sentinelPool.getFormattedPoolName());
            } catch (JedisConnectionException e) {
                JigsawJedisException exception = new JigsawJedisException(e, sentinelPool.getAddress(),
                        sentinelPool.getFormattedPoolName());
                broken = true;

                getJedisExceptionHandler().handleException(exception);
                throw exception;
            } catch (JedisDataException e1) {
                JigsawJedisException exception = new JigsawJedisException(e1, sentinelPool.getAddress(),
                        sentinelPool.getFormattedPoolName());
                logger.error("remove master [" + masterName + "] fail", e1);

                getJedisExceptionHandler().handleException(exception);
            } finally {
                JedisUtils.closeResource(sentinelPool, sentinelJedis, broken);
            }
        }
    }

    @Override
    public void stopSyncUp() {
        super.stopSyncUp();
        monitoredBySentinel();
    }

    @Override
    public void startSyncUp() {
        removeFromSentinel();
        super.startSyncUp();
    }
}
