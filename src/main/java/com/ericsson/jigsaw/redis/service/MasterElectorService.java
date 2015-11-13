/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.service;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.ericsson.jigsaw.redis.JedisTemplate;
import com.ericsson.jigsaw.redis.JedisTemplate.JedisActionNoResult;
import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.service.base.ScheduledJobBase;

/**
 * The purpose of this class is define a MasterElector to select a server is a master.
 * The client can invoke the function isMaster() to know its master or not.
 *
 * @deprecated Please replace with ClusterRuntime in jigsaw-cluster.
 */
@Deprecated
public class MasterElectorService extends ScheduledJobBase {

    private static final int ELECTOR_SECONDS_DEFAULT = 5;
    private static final int EXPIRED_SECONDS_DEFAULT = 2 * ELECTOR_SECONDS_DEFAULT;

    private static Logger logger = LoggerFactory.getLogger(MasterElectorService.class);

    private JedisTemplate jedisTemplate;

    private int expiredPeriodSeconds;

    private String hostId;

    private String masterKey;

    private AtomicBoolean master = new AtomicBoolean(false);

    public MasterElectorService(JedisPool jedisPool, String masterKey, int electorPeriodSeconds,
            int expiredPeriodSeconds) {
        super(1000L * electorPeriodSeconds);
        jedisTemplate = new JedisTemplate(jedisPool);
        this.masterKey = masterKey;
        this.expiredPeriodSeconds = expiredPeriodSeconds;
    }

    public MasterElectorService(JedisPool jedisPool, String masterKey) {
        this(jedisPool, masterKey, ELECTOR_SECONDS_DEFAULT, EXPIRED_SECONDS_DEFAULT);
    }

    public boolean isMaster() {
        return master.get();
    }

    @Override
    public boolean init() {
        hostId = generateHostId();
        logger.info("masterElector init finished, hostName:{}.", hostId);
        return true;
    }

    protected String generateHostId() {
        String host = "localhost";
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.warn("can not get hostName", e);
        }
        host = host + "-" + new SecureRandom().nextInt(100000);

        return host;
    }

    @Override
    public void run() {
        jedisTemplate.execute(new JedisActionNoResult() {//NOSONAR
            @Override
            public void action(Jedis jedis) {
                String masterFromRedis = jedis.get(masterKey);

                logger.debug("master is {}", masterFromRedis);

                //if master is null, the cluster just start or the master had crashed, try to register myself as master
                if (masterFromRedis == null) {
                    //use setnx to make sure only one client can register as master.
                    if (jedis.set(masterKey, hostId, "NX", "EX", expiredPeriodSeconds) != null) {
                        master.set(true);
                        logger.info("master is changed to {}.", hostId);
                        return;
                    } else {
                        master.set(false);
                        return;
                    }
                }

                //if master is myself, update the expire time.
                if (hostId.equals(masterFromRedis)) {
                    jedis.expire(masterKey, expiredPeriodSeconds);
                    master.set(true);
                    return;
                }

                master.set(false);
            }
        });

    }

    //for test
    public void setHostId(String hostId) {
        this.hostId = hostId;
    }
}
