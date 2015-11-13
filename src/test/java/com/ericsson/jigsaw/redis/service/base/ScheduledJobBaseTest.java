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

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ScheduledJobBaseTest {

    boolean isInitSuccess;
    private int index;
    boolean isDestroy;
    private ScheduledJobBase job;

    @Before
    public void setUp() throws Exception {
        index = 0;
        isDestroy = false;
        isInitSuccess = true;
    }

    @After
    public void tearDown() throws Exception {
        job = null;
    }

    @Test
    public void testJobInitFailure() throws InterruptedException {
        //init
        job = new TestJob(200);
        isInitSuccess = false;

        job.start();
        Thread.sleep(2000);
        job.stop();

        assertTrue((index == 0));
        assertTrue(isDestroy);
    }

    @Test
    public void testJob() throws InterruptedException {
        //init
        job = new TestJob(200);

        job.start();
        Thread.sleep(2000);
        job.stop();

        assertTrue((index > 2));
        assertTrue(isDestroy);
    }

    @Test
    public void testJobWithExcpetion() throws InterruptedException {
        //init
        job = new TestJobWithExcepton(200);

        job.start();
        Thread.sleep(2000);
        job.stop();

        assertTrue((index > 2));
        assertTrue(isDestroy);
    }

    private class TestJob extends ScheduledJobBase {

        public TestJob(long ms) {
            super(ms);
        }

        @Override
        public boolean init() {
            return isInitSuccess;
        }

        @Override
        public void run() {
            index++;
        }

        @Override
        public void destroy() {
            isDestroy = true;
        }
    }

    private class TestJobWithExcepton extends ScheduledJobBase {

        public TestJobWithExcepton(long ms) {
            super(ms);
        }

        @Override
        public boolean init() {
            return isInitSuccess;
        }

        @Override
        public void run() {
            index++;
            throw new RuntimeException("simulated exception.");
        }

        @Override
        public void destroy() {
            isDestroy = true;
        }
    }
}
