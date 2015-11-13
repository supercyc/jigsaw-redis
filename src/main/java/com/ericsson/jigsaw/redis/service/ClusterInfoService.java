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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.jigsaw.redis.JedisTemplate;
import com.ericsson.jigsaw.redis.pool.JedisPool;

/**
 * The purpose of this class is define a ClusterInfoService to get all
 * active service info. The client can invoke the function getMembers
 * and getCurrentMember.
 *
 * @deprecated Please replace with ClusterRuntime in jigsaw-cluster.
 */
@Deprecated
public class ClusterInfoService {

    private static Logger logger = LoggerFactory.getLogger(ClusterInfoService.class);

    private JedisTemplate jedisTemplate;

    private long timeOutMilliSeconds;

    private String clusterKey;

    private String currentMember;

    private AtomicInteger count = new AtomicInteger(0);

    public ClusterInfoService(JedisPool jedisPool, String clusterKey) {
        this(jedisPool, clusterKey, null);
    }

    public ClusterInfoService(JedisPool jedisPool, String clusterKey, String member) {
        jedisTemplate = new JedisTemplate(jedisPool);
        this.clusterKey = clusterKey;

        if ((member != null) && (member.length() > 0)) {
            currentMember = member;
        } else {
            currentMember = generateMember();
        }
        logger.debug("cluster clusterKey:{},  member:{}", clusterKey, member);
    }

    public List<String> getMembers() {
        logger.debug("clusterKey:{} ,expiredPeriodSeconds :{} , currentMember :{}", clusterKey,
                timeOutMilliSeconds, currentMember);
        // get all active member
        Set<String> memberSet = jedisTemplate.zrangeByScore(clusterKey, System.currentTimeMillis(), Double.MAX_VALUE);
        List<String> activeMembers = new ArrayList<String>(memberSet);
        // sort members
        Collections.sort(activeMembers);
        logger.debug("clusterKey:{}, all members :{}", clusterKey, activeMembers);
        return activeMembers;
    }

    public String getCurrentMember() {
        return currentMember;
    }

    private String generateMember() {
        // use local host as member
        String host = "localhost";
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            logger.warn("can not get hostName", e);
        }
        return host + "-" + new SecureRandom().nextInt(100000);
    }

    public void heartBeat() {
        jedisTemplate.zadd(clusterKey, System.currentTimeMillis() + timeOutMilliSeconds, currentMember);
        if (count.getAndIncrement() == 100) {
            jedisTemplate.zremByScore(clusterKey, Double.MIN_VALUE, System.currentTimeMillis());
            count.set(0);
        }
    }

    public void setTimeOutMilliSeconds(long timeOutMilliSeconds) {
        this.timeOutMilliSeconds = timeOutMilliSeconds;
    }

}
