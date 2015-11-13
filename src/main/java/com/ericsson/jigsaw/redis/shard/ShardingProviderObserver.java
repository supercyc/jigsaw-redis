/*
 * ----------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 * ----------------------------------------------------------------------
 */

package com.ericsson.jigsaw.redis.shard;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is to observe the hash distribution, now the implementation is print in the log.
 *
 * @param <T>
 */

public class ShardingProviderObserver<T> {
    private static Logger logger = LoggerFactory.getLogger(ShardingProviderObserver.class);
    private static int OBSERVE_INTERNAL_MILLISEC = 5000 * 60;

    private long prevObserveTimestamp = System.currentTimeMillis();
    private Map<T, AtomicLong> internalShardRecords = new HashMap<T, AtomicLong>();

    public ShardingProviderObserver(Collection<T> tList) {
        init(tList);
    }

    private void init(Collection<T> tList) {
        for (T t : tList) {
            internalShardRecords.put(t, new AtomicLong(0));
        }
    }

    public void observe(T t) {
        AtomicLong value = internalShardRecords.get(t);
        if (value == null) {
            internalShardRecords.put(t, new AtomicLong(0));
        } else {
            value.incrementAndGet();
        }

        if (System.currentTimeMillis() - prevObserveTimestamp > OBSERVE_INTERNAL_MILLISEC) {
            prevObserveTimestamp = System.currentTimeMillis();
            showShardingDistribution();
        }
    }

    public void showShardingDistribution() {
        log();
    }

    private void log() {
        logger.info("Sharding distribution:" + internalShardRecords);
    }

    //
    public static void setObserveInterval(int observerIntervalMillisec) {
        OBSERVE_INTERNAL_MILLISEC = observerIntervalMillisec;
    }
}
