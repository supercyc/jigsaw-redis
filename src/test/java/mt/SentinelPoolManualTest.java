/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package mt;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import com.ericsson.jigsaw.redis.JedisTemplate;
import com.ericsson.jigsaw.redis.JedisTemplate.JedisAction;
import com.ericsson.jigsaw.redis.JedisTemplate.JedisActionNoResult;
import com.ericsson.jigsaw.redis.pool.ConnectionInfo;
import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.pool.JedisSentinelPool;

/**
 * Since most testing is about the thread and the real interaction with Jedis/Sentinel, use manual test instead of Unit
 * test.
 * 
 * 
 * Setup: start 1 master, 1 slave, 1/2 sentinels
 * 
 * Case1: Stop master, the client print error message, after 30 seconds, the client print a master change message and
 * stop print error message.
 * 
 * Case2: Restart sentinel.
 * 
 * Case3: No sentinel ready, the client throw an exception.
 * 
 * Case4: Master name not in sentinel.conf, the client throw an exception.
 * 
 * 
 * TearDown: Hit enter to stop client.
 */
public class SentinelPoolManualTest implements Runnable {

    private static final String COUNTER_KEY = "test:counter";
    private AtomicInteger counter = new AtomicInteger(0);

    private JedisSentinelPool pool;
    private JedisTemplate template;

    public static void main(String[] args) throws Exception {
        String hosts = System.getProperty("hosts", "127.0.0.1:26379");
        String masterName = System.getProperty("masterName", "default");

        SentinelPoolManualTest test = new SentinelPoolManualTest();
        test.initPool(hosts, masterName);

        ExecutorService threadPool = Executors.newFixedThreadPool(1);
        threadPool.submit(test);
        System.out.println("Press enter in console to stop the system");
        while (true) {
            char c = (char) System.in.read();
            if (c == '\n') {
                System.out.println("Shuting down");
                test.shutdownPool();
                threadPool.shutdownNow();
                boolean poolExit = threadPool.awaitTermination(10, TimeUnit.SECONDS);
                if (!poolExit) {
                    System.out.println("Thread pool stop fail, forcing shutdown");
                    System.exit(-1);
                }
                break;
            }
        }

        System.out.println("System shutdown");
    }

    public void initPool(String hosts, String masterName) {
        String[] hostAndPortArray = hosts.split(",");
        List<HostAndPort> sentinelInfos = new ArrayList<HostAndPort>();
        for (String hostAndPort : hostAndPortArray) {
            HostAndPort sentinelInfo = new HostAndPort(hostAndPort.split(":")[0], new Integer(
                    hostAndPort.split(":")[1]));
            sentinelInfos.add(sentinelInfo);
        }

        ConnectionInfo redisAddtionalInfo = new ConnectionInfo(Protocol.DEFAULT_DATABASE,
                ConnectionInfo.DEFAULT_PASSWORD, Protocol.DEFAULT_TIMEOUT);
        JedisPoolConfig poolConfig = JedisPool.createPoolConfig(10);

        pool = new JedisSentinelPool(sentinelInfos.toArray(new HostAndPort[sentinelInfos.size()]),
                masterName, redisAddtionalInfo, poolConfig);
        template = new JedisTemplate(pool);

        template.execute(new JedisActionNoResult() {
            @Override
            public void action(Jedis jedis) {
                jedis.set(COUNTER_KEY, String.valueOf(0));
            }
        });

    }

    public void shutdownPool() {
        System.out.println("JVM Counter is " + counter.get());

        String redisCounter = template.execute(new JedisAction<String>() {
            @Override
            public String action(Jedis jedis) {
                return jedis.get(COUNTER_KEY);
            }
        });

        System.out.println("Redis Counter is " + redisCounter);

        pool.destroy();
    }

    @Override
    public void run() {
        while (true) {
            try {
                template.execute(new JedisActionNoResult() {
                    @Override
                    public void action(Jedis jedis) {
                        jedis.incr(COUNTER_KEY);
                        counter.incrementAndGet();
                    }
                });
                Thread.sleep(100);
            } catch (Exception e) {

                if (e instanceof InterruptedException) {
                    System.out.println("thread intterupted");
                    break;
                }

                System.out.println(e.getMessage());
                try {
                    System.out.println("Sleep 1 seconds");
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    System.out.println("thread intterupted");
                    break;
                }
            }
        }
    }
}
