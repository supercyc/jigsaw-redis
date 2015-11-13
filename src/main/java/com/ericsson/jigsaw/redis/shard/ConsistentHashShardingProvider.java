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

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import com.ericsson.jigsaw.redis.shard.hash.Hashing;

/**
 * the algorithm: hash ring, with replicated node;
 */

public class ConsistentHashShardingProvider<T> implements ShardingProvider<T> {
    private static final Hashing HASH_ALGO = Hashing.MURMUR_HASH;
    private static final int REPLICATED_NODE_NUMBER = 128;
    private TreeMap<Long, T> nodes = new TreeMap<Long, T>();

    private boolean observed;
    private ShardingProviderObserver<T> shardingProviderObserver;

    public ConsistentHashShardingProvider() {
        this.observed = false;
    }

    public ConsistentHashShardingProvider(boolean isObserved) {
        this.observed = isObserved;
    }

    @Override
    public void initNode(List<T> sourceNodes) {
        int shardNumber = 0;

        for (T node : sourceNodes) {
            for (int n = 0; n < REPLICATED_NODE_NUMBER; n++) {
                nodes.put(Math.abs(HASH_ALGO.hash("SHARD-" + shardNumber + "-NODE-" + n)), node);
            }
            shardNumber++;
        }

        if (observed) {
            shardingProviderObserver = new ShardingProviderObserver<T>(sourceNodes);
        }
    }

    @Override
    public T getNode(String key) {
        T node;
        SortedMap<Long, T> tail = nodes.tailMap(Math.abs(HASH_ALGO.hash(key)));
        if (tail.isEmpty()) {
            // the last node, back to first.
            // counter.get(nodes.get(nodes.firstKey()).getJedisPool()).incrementAndGet();
            node = nodes.get(nodes.firstKey());
        } else {
            // counter.get(tail.get(tail.firstKey()).getJedisPool()).incrementAndGet();
            node = tail.get(tail.firstKey());
        }

        if (observed) {
            shardingProviderObserver.observe(node);
        }

        return node;
    }

    @Override
    public T getSpecifiedNode(@SuppressWarnings("unused") int index) {
        throw new UnsupportedOperationException("Get specified node is not supported for hash ring.");
    }

    @Override
    public <A> ShardingProvider<A> derive(@SuppressWarnings("unused") Class<A> clazz) {
        return new ConsistentHashShardingProvider<A>(observed);
    }
}
