/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.replication.proxy;

import com.ericsson.jigsaw.redis.replication.backlog.RedisBacklogManager;

public abstract class BacklogProxy<T> extends ThreadLocalTargetProxy<T> {

    protected RedisBacklogManager backlogManager;

    public BacklogProxy(RedisBacklogManager backlogManager) {
        this.backlogManager = backlogManager;
    }
}
