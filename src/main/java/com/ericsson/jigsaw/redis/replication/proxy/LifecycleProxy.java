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

import java.lang.reflect.Method;

public interface LifecycleProxy<T> {

    void preInvoke(Method method, Object[] args) throws Throwable;

    void postInvoke(Method method, Object[] args) throws Throwable;

    Object invoke(Method method, Object[] args) throws Throwable;
}
