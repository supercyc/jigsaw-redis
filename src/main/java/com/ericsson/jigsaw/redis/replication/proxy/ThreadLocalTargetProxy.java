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

public abstract class ThreadLocalTargetProxy<T> extends TargetProxy<T> {
    
    private ThreadLocal<T> threadLocaTarget = new ThreadLocal<T>();

    public ThreadLocalTargetProxy() {
        super(null);
    }

    public T onBehalfOf(T target) {
        this.threadLocaTarget.set(target);
        return this.getProxy();
    }

    @Override
    protected T getTarget() {
        return this.threadLocaTarget.get();
    }

    protected void detachTarget() {
        this.threadLocaTarget.remove();
    }
}
