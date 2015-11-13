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
import redis.clients.jedis.exceptions.JedisException;

import com.ericsson.jigsaw.redis.JedisTemplate;
import com.ericsson.jigsaw.redis.JedisTemplate.JedisActionNoResult;
import com.ericsson.jigsaw.redis.JigsawJedisException;
import com.ericsson.jigsaw.redis.replication.RedisOp;
import com.ericsson.jigsaw.redis.replication.ReplicaContext;
import com.ericsson.jigsaw.redis.replication.ReplicaDataSource;

public abstract class AbstractReplicaDataSource implements ReplicaDataSource {

    private static Logger logger = LoggerFactory.getLogger(AbstractReplicaDataSource.class);

    protected static final String CONNECTINFO_DELIMITER = ",";

    public AbstractReplicaDataSource() {
        super();
    }

    @Override
    public void syncUpWith(final HostAndPort targetAddress) {
        try {

            this.getDataTemplate().execute(new JedisActionNoResult() {

                @Override
                public void action(Jedis jedis) {
                    if (ReplicaContext.getRedisPassword() != null) {
                        jedis.configSet("masterauth", ReplicaContext.getRedisPassword());
                    }
                    jedis.slaveof(targetAddress.getHost(), targetAddress.getPort());
                }
            });
        } catch (JedisException e) {
            if (needHandleConnectionException(e)) {
                handleConnectionExcetpion(e);
            }
            throw e;
        }
    }

    private boolean needHandleConnectionException(JedisException ex) {
        boolean need = false;
        JedisException exception = ex;
        if (ex instanceof JigsawJedisException) {
            exception = ((JigsawJedisException) ex).getInternalJedisException();
        }

        if (exception instanceof JedisConnectionException) {
            need = true;
        } else if (exception instanceof JedisDataException) {
            if ((ex.getMessage() != null) && (ex.getMessage().indexOf("READONLY") != -1)) {
                need = true;
            }
        }
        return need;
    }

    @Override
    public void stopSyncUp() {
        try {
            this.getDataTemplate().slaveofNoOne();
            while (true) {

                if (ReplicaUtiils.isMaster(getDataTemplate())) {
                    break;
                }
            }
            logger.info("[{}] return to master", this.getDataTemplate().getJedisPool().getAddress());
        } catch (JedisConnectionException e) {
            handleConnectionExcetpion(e);
            throw e;
        }
    }

    @Override
    public boolean isSyncedUp(HostAndPort address) {
        try {
            return ReplicaUtiils.isSyncedUp(this.getDataTemplate(), address);
        } catch (JedisConnectionException e) {
            handleConnectionExcetpion(e);
            throw e;
        }
    }

    @Override
    public String toString() {
        final JedisTemplate dataTemplate = this.getDataTemplate();
        return "ReplicaDataSource[" + (dataTemplate == null ? "Detached" : dataTemplate.getJedisPool().getAddress())
                + "]";
    }

    @Override
    public void startSyncUp() {
        // do nothing
    }

    @Override
    public boolean isSyncedUp() {
        // should be override by subclass
        return true;
    }

    @Override
    public void replay(List<RedisOp> operations) {
        if ((operations == null) || operations.isEmpty()) {
            return;
        }

        try {
            ReplicaUtiils.replay(this.getDataTemplate(), operations);
        } catch (JigsawJedisException e) {
            JedisException jedisException = e.getInternalJedisException();
            if (needHandleConnectionException(jedisException)) {
                handleConnectionExcetpion(jedisException);
            }
            throw e;
        }
    }

    protected void handleConnectionExcetpion(@SuppressWarnings("unused") JedisException e) {
        // do nothing
    }

    protected abstract JedisTemplate getDataTemplate();

    @Override
    public boolean isNotStopedSyncedUpBy(HostAndPort address) {
        return ReplicaUtiils.isMasterOf(getDataTemplate(), address);
    }

    @Override
    public void checkReadyForSyncUp() {
        try {
            if (!ReplicaUtiils.isMaster(getDataTemplate())) {
                throw new JedisDataException(this + " is READONLY");
            }
        } catch (JedisException e) {
            if (needHandleConnectionException(e)) {
                handleConnectionExcetpion(e);
            }
            throw e;
        }
    }
}