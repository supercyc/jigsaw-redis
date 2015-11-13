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

public interface ShardingProvider<T> {

    public void initNode(List<T> sourceNodes);

    public T getNode(String key);

    public T getSpecifiedNode(int index);

    public <A> ShardingProvider<A> derive(Class<A> clazz);

    //Did not support add and remove yet.
    //public void addNode(List<T> newNodes);
    //public void removeNode(List<T> delNodes);
}
