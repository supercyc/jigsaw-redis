/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.service.base;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The purpose of this class is to create a base class for the scheduled at fixed rate job, pls extends this class if
 * any scheduledJob is needed.
 */
public abstract class ScheduledJobBase implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(ScheduledJobBase.class);

    private static final int WAIT_THREADPOOL_TERMINATE_SECONDS = 5;

    private ScheduledExecutorService threadPool;
    private boolean hasInternalPool = false;
    private ScheduledFuture<?> scheduledFuture;

    private long scheduleMilliseconds;

    /**
     * Creates a new instance of <code>ScheduledJobBase</code>. Also create a new threadPool.
     */
    public ScheduledJobBase(long scheduleMilliseconds) {
        this(scheduleMilliseconds, null);
    }

    /**
     * Creates a new instance of <code>ScheduledJobBase</code>. The schedule task will use the specific threadPool.
     */
    public ScheduledJobBase(long scheduleMilliseconds, ScheduledExecutorService threadPool) {
        this.scheduleMilliseconds = scheduleMilliseconds;

        if (threadPool == null) {
            this.threadPool = Executors.newScheduledThreadPool(1);
            hasInternalPool = true;
        } else {
            this.threadPool = threadPool;
        }
    }

    /**
     * start the scheduled task.
     */
    public void start() {
        if (init()) {
            scheduledFuture = threadPool.scheduleAtFixedRate(new NoExceptionRunnable(this), 1000, scheduleMilliseconds,
                    TimeUnit.MILLISECONDS);
        } else {
            logger.error("job init failed.");
        }
    }

    /**
     * Stop the scheduled task.
     */
    public void stop() {
        try {
            if (threadPool != null) {
                if (hasInternalPool) {
                    threadPool.shutdownNow();
                    if (!threadPool.awaitTermination(WAIT_THREADPOOL_TERMINATE_SECONDS, TimeUnit.SECONDS)) {
                        logger.error("ThreadPool terminate failed!");
                    }
                } else {
                    scheduledFuture.cancel(true);
                }
            }
        } catch (Throwable e) {
            logger.warn("Exception happen when stopping job", e);
        }

        destroy();
    }

    /**
     * Initialize method which will be called in {@link #start()} before start thread pool, can be override in
     * sub-class.
     */
    public boolean init() {
        return true;
    }

    /**
     * Initialize method called in {@link #stop()} after stop thread pool, can be override in sub-class.
     */
    public void destroy() {

    }

    /**
     * Runnable Wrapper to catch any exception to prevent it break the thread.
     */
    protected static class NoExceptionRunnable implements Runnable {

        private Runnable runnable;

        public NoExceptionRunnable(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            try {
                runnable.run();
            } catch (Throwable e) {
                //catch any exception, because the scheduled task will fail
                logger.error("Unexpected exception when running the job.", e);
            }
        }
    }
}
