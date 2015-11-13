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

import com.ericsson.jigsaw.redis.shard.hash.Hashing;

/**
 * the algorithm: hash % nodeSize;
 */
public class NormHashShardingProvider<T> implements ShardingProvider<T> {
    private static final Hashing HASH_ALGO = Hashing.MURMUR_HASH;

    private List<T> nodes;
    private int nodeSize;

    private boolean observed;
    private ShardingProviderObserver<T> shardingProviderObserver;

    public NormHashShardingProvider() {
        this.observed = false;
    }

    public NormHashShardingProvider(boolean observed) {
        this.observed = observed;
    }

    @Override
    public void initNode(List<T> sourceNodes) {
        nodes = sourceNodes;
        nodeSize = sourceNodes.size();

        if (observed) {
            shardingProviderObserver = new ShardingProviderObserver<T>(sourceNodes);
        }
    }

    @Override
    public T getNode(String key) {
        T node = nodes.get((int) (Math.abs(HASH_ALGO.hash(key)) % nodeSize));

        if (observed) {
            shardingProviderObserver.observe(node);
        }

        return node;
    }

    @Override
    public T getSpecifiedNode(int index) {
        return nodes.get(index % nodeSize);
    }

    @Override
    public <A> ShardingProvider<A> derive(@SuppressWarnings("unused") Class<A> clazz) {
        return new NormHashShardingProvider<A>(observed);
    }
}
