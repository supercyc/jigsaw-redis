/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.pool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.jigsaw.redis.JedisTemplate;
import com.ericsson.jigsaw.redis.JedisTemplate.JedisAction;
import com.ericsson.jigsaw.redis.JedisUtils;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * Pool which to get redis master address get from sentinel instances.
 */
public class JedisSentinelPool extends JedisPool {

    private static final String NO_ADDRESS_YET = "I dont know because no sentinel up";

    private static Logger logger = LoggerFactory.getLogger(JedisSentinelPool.class);

    private List<JedisPool> sentinelPools = new ArrayList<JedisPool>();
    private MasterSwitchListener masterSwitchListener;

    private String masterName;
    private JedisPoolConfig masterPoolConfig;
    private ConnectionInfo masterConnectionInfo;
    private CountDownLatch poolInit = new CountDownLatch(1);

    /**
     * Creates a new instance of <code>JedisSentinelPool</code>.
     * <p/>
     * All parameters can be null or empty.
     *
     * @param sentinelAddresses Array of HostAndPort to sentinel
     *            instances.
     * @param masterName One sentinel can monitor several redis
     *            master-slave pair, use master name to identify them.
     * @param masterConnectionInfo The the other information like
     *            password,timeout.
     * @param masterPoolConfig Configuration of redis pool.
     */
    @SuppressWarnings("null")
    public JedisSentinelPool(HostAndPort[] sentinelAddresses, String masterName, ConnectionInfo masterConnectionInfo,
            JedisPoolConfig masterPoolConfig) {
        // sentinelAddresses
        assertArgument(((sentinelAddresses == null) || (sentinelAddresses.length == 0)), "sentinelInfo is not set");

        for (HostAndPort sentinelInfo : sentinelAddresses) {
            ConnectionInfo internalConnectionInfo = new ConnectionInfo(masterConnectionInfo.getPoolName());
            sentinelPools.add(new JedisDirectPool(sentinelInfo, internalConnectionInfo, new JedisPoolConfig()));
        }

        // masterConnectionInfo
        assertArgument(masterConnectionInfo == null, "masterConnectionInfo is not set");
        this.masterConnectionInfo = masterConnectionInfo;

        // masterName
        assertArgument(((masterName == null) || masterName.isEmpty()), "masterName is not set");
        this.masterName = masterName;

        // poolConfig
        assertArgument(masterPoolConfig == null, "masterPoolConfig is not set");
        this.masterPoolConfig = masterPoolConfig;

        // Start MasterSwitchListener thread ,internal poll will be start in the thread
        masterSwitchListener = new MasterSwitchListener(getFormattedPoolName());
        masterSwitchListener.start();

        try {
            if (!poolInit.await(2, TimeUnit.SECONDS)) {
                logger.warn(getFormattedPoolName() + "the sentinel pool can't not init in 2 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        logger.info("Create redis sentinel pool, address:" + Arrays.deepToString(sentinelAddresses)
                + ", connection info:" + masterConnectionInfo);
    }

    public JedisSentinelPool(HostAndPort[] sentinelAddresses, String masterName, JedisPoolConfig masterPoolConfig) {
        this(sentinelAddresses, masterName, new ConnectionInfo(), masterPoolConfig);
    }

    @Override
    public void destroy() {
        // shutdown the listener thread
        masterSwitchListener.shutdown();

        // destroy sentinel pools
        for (JedisPool sentinel : sentinelPools) {
            sentinel.destroy();
        }

        // destroy redis pool
        destroyInternelPool();

        // wait for the masterSwitchListener thread finish
        try {
            logger.info(getFormattedPoolName() + "Waiting for MasterSwitchListener thread finish");
            masterSwitchListener.join();
            logger.info(getFormattedPoolName() + "MasterSwitchListener thread finished");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    protected void destroyInternelPool() {
        if (this.internalPool != null) {
            closeInternalPool();
        }

        address = null;
        connectionInfo = null;
        internalPool = null;

        logger.info(getFormattedPoolName() + "destroy sentinel internal pool.");
    }

    /**
     * Assert the argurment, throw IllegalArgumentException if the
     * expression is true.
     */
    private void assertArgument(boolean expression, String message) {
        if (expression) {
            throw new IllegalArgumentException(getFormattedPoolName() + message);
        }
    }

    @Override
    public String getFormattedPoolName() {
        return "[RedisSentinelPool: " + this.masterConnectionInfo.getPoolName() + "]:";
    }

    // for test
    public MasterSwitchListener getMasterSwitchListener() {
        return masterSwitchListener;
    }

    public List<JedisPool> getSentinelPools() {
        return sentinelPools;
    }

    public String getMasterName() {
        return masterName;
    }

    /**
     * Listener thread to listen master switch message from sentinel.
     */
    public class MasterSwitchListener extends Thread {
        public static final String THREAD_NAME_PREFIX = "MasterSwitchListener-";

        private JedisPubSub subscriber;
        private JedisPool sentinelPool;
        private Jedis sentinelJedis;
        private AtomicBoolean running = new AtomicBoolean(true);
        private HostAndPort previousMasterAddress;

        public MasterSwitchListener(@SuppressWarnings("unused") String poolPrefix) {
            super(getFormattedPoolName() + THREAD_NAME_PREFIX + masterName);
        }

        // stop the blocking subscription and interrupt the thread
        public void shutdown() {
            // interrupt the thread
            running.getAndSet(false);
            this.interrupt();

            // stop the blocking subscription
            try {
                if (subscriber != null) {
                    subscriber.unsubscribe();
                }
            } finally {
                JedisUtils.destroyJedis(sentinelJedis);
            }
        }

        @Override
        public void run() {
            while (running.get()) {
                try {
                    sentinelPool = pickupSentinel();

                    if (sentinelPool != null) {

                        HostAndPort masterAddress = queryMasterAddress();

                        if (masterAddress == null) {
                            logger.error(getFormattedPoolName() + " Master name " + masterName
                                    + " is not in sentinel.conf, make sure if redis is executing slaveof operation");
                            sleep(2000);
                            continue;
                        }

                        if ((internalPool != null) && isAddressChange(masterAddress)) {
                            logger.info(getFormattedPoolName() + " The internalPool {} had changed, destroy it now.",
                                    previousMasterAddress);
                            destroyInternelPool();
                        }

                        if (internalPool == null) {
                            logger.info(getFormattedPoolName()
                                    + " The internalPool {} is not init or the address had changed, init it now.",
                                    masterAddress);
                            initInternalPool(masterAddress, masterConnectionInfo, masterPoolConfig);
                            poolInit.countDown();
                        }

                        previousMasterAddress = masterAddress;

                        sentinelJedis = sentinelPool.getResource();
                        subscriber = new MasterSwitchSubscriber();
                        sentinelJedis.subscribe(subscriber, "+switch-master", "+redirect-to-master");
                    } else {
                        logger.info(getFormattedPoolName()
                                + "All sentinels down, sleep 2 seconds and try to connect again.");
                        // When the system startup but the sentinels not yet, init a urgly address to prevent null point
                        // exception. change the logic later.
                        if (internalPool == null) {
                            HostAndPort masterAddress = new HostAndPort(NO_ADDRESS_YET, 6379);
                            initInternalPool(masterAddress, masterConnectionInfo, masterPoolConfig);
                            previousMasterAddress = masterAddress;
                        }
                        sleep(2000);
                    }
                } catch (JedisConnectionException e) {

                    if (sentinelJedis != null) {
                        sentinelPool.returnBrokenResource(sentinelJedis);
                    }

                    if (running.get()) {
                        logger.error(getFormattedPoolName() + "Lost connection with Sentinel "
                                + sentinelPool.getAddress() + ", sleep 1 seconds and try to connect other one. ");
                        sleep(1000);
                    }
                } catch (Exception e) {
                    logger.error(getFormattedPoolName() + e.getMessage(), e);
                    sleep(1000);
                }
            }
        }

        public HostAndPort getCurrentMasterAddress() {
            return previousMasterAddress;
        }

        /**
         * Pickup the first available sentinel, if all sentinel down,
         * return null.
         */
        private JedisPool pickupSentinel() {
            for (JedisPool pool : sentinelPools) {
                if (JedisUtils.ping(pool)) {
                    return pool;
                }
            }

            return null;
        }

        private boolean isAddressChange(HostAndPort currentMasterAddress) {
            if (previousMasterAddress == null) {
                return true;
            }

            return !previousMasterAddress.equals(currentMasterAddress);
        }

        /**
         * Query master address from sentinel.
         */
        private HostAndPort queryMasterAddress() {
            JedisTemplate sentinelTemplate = new JedisTemplate(sentinelPool);
            @SuppressWarnings("hiding")
            List<String> address = sentinelTemplate.execute(new JedisAction<List<String>>() {
                @Override
                public List<String> action(Jedis jedis) {
                    return jedis.sentinelGetMasterAddrByName(masterName);
                }
            });

            if ((address == null) || address.isEmpty()) {
                return null;
            }

            return new HostAndPort(address.get(0), Integer.valueOf(address.get(1)));
        }

        private void sleep(int millseconds) {
            try {
                Thread.sleep(millseconds);
            } catch (InterruptedException e1) {
                Thread.currentThread().interrupt();
            }
        }

        /**
         * Subscriber for the master switch message from sentinel and
         * init the new pool.
         */
        private class MasterSwitchSubscriber extends JedisPubSub {
            @Override
            public void onMessage(@SuppressWarnings("unused") String channel, String message) {
                // message example: +switch-master: mymaster 127.0.0.1 6379 127.0.0.1 6380
                // +redirect-to-master default 127.0.0.1 6380 127.0.0.1 6381 (if slave-master fail-over quick enough)
                logger.info(getFormattedPoolName() + "Sentinel " + sentinelPool.getAddress() + " published: " + message);
                String[] switchMasterMsg = message.split(" ");
                // if the master name equals my master name, destroy the old pool and init a new pool
                if (masterName.equals(switchMasterMsg[0])) {

                    HostAndPort masterAddress = new HostAndPort(switchMasterMsg[3],
                            Integer.parseInt(switchMasterMsg[4]));
                    logger.info(getFormattedPoolName() + "Switch master to " + masterAddress);
                    destroyInternelPool();
                    initInternalPool(masterAddress, masterConnectionInfo, masterPoolConfig);
                    previousMasterAddress = masterAddress;
                }
            }

            @Override
            @SuppressWarnings("unused")
            public void onPMessage(String pattern, String channel, String message) {
                //do nothing
            }

            @Override
            @SuppressWarnings("unused")
            public void onSubscribe(String channel, int subscribedChannels) {
                //do nothing
            }

            @Override
            @SuppressWarnings("unused")
            public void onUnsubscribe(String channel, int subscribedChannels) {
                //do nothing
            }

            @Override
            @SuppressWarnings("unused")
            public void onPUnsubscribe(String pattern, int subscribedChannels) {
                //do nothing
            }

            @Override
            @SuppressWarnings("unused")
            public void onPSubscribe(String pattern, int subscribedChannels) {
                //do nothing
            }
        }
    }

}
