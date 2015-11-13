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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TargetProxy<T> extends AbstractProxyFactory<T> implements LifecycleProxy<T>, MethodInterceptor {

    private static Logger logger = LoggerFactory.getLogger(TargetProxy.class);
    
    private T target;

    public TargetProxy(T target) {
        this.target = target;
    }

    protected T getTarget() {
        return this.target;
    }

    @Override
    public Object intercept(@SuppressWarnings("unused") Object obj, Method method, Object[] args,
            @SuppressWarnings("unused") MethodProxy proxy) throws Throwable {
        this.preInvoke(method, args);
        final Object result = this.invoke(method, args);
        this.postInvoke(method, args);
        return result;
    }

    @Override
    public Object invoke(Method method, Object[] args) throws Throwable {
        Object result = null;
        try {
            if (getTarget() != null) {
                result = method.invoke(getTarget(), args);
            } else {
                logger.error("Invoke method {} error, target is detached, args={}", method, args);
            }
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
        return result;
    }

    @Override
    @SuppressWarnings("unused")
    public void postInvoke(Method method, Object[] args) throws Throwable {
        // do nothing
    }

    @Override
    @SuppressWarnings("unused")
    public void preInvoke(Method method, Object[] args) throws Throwable {
        // do nothing
    }
}
