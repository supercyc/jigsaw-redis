/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2014
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.pool;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

/**
 * Build JedisPool smartly, depends on masterName whether prefix with
 * "direct:", it will build JedisSentinelPool or JedisDirectPool.
 *
 * Also you can use validateUrl() to validate the format.
 *
 * @see sample.RedisSample
 */
public class JedisPoolBuilder {

    public static final String DIRECT_POOL_PREFIX = "direct:";
    private static Logger logger = LoggerFactory.getLogger(JedisPoolBuilder.class);
    private static final String[] EMPTY_MASTERS = {};

    private String[] sentinelHosts;
    private String[] sentinelPorts;
    private int sentinelPort = Protocol.DEFAULT_SENTINEL_PORT;

    private String masterName;
    private String[] shardedMasterNames = EMPTY_MASTERS;

    private int poolSize = -1;

    private int database = Protocol.DEFAULT_DATABASE;
    private String password = ConnectionInfo.DEFAULT_PASSWORD;
    private int timeout = Protocol.DEFAULT_TIMEOUT;
    private String poolName;

    public static JedisPoolBuilder newInstance() {
        return new JedisPoolBuilder();
    }

    public JedisPoolBuilder setHosts(String[] hosts) {
        this.sentinelHosts = hosts;
        return this;
    }

    public JedisPoolBuilder setHosts(String hosts) {
        if (hosts != null) {
            this.sentinelHosts = hosts.split(",");
        }
        return this;
    }

    public JedisPoolBuilder setPort(int port) {
        this.sentinelPort = port;
        return this;
    }

    public JedisPoolBuilder setPorts(String ports) {
        if (ports != null) {
            this.sentinelPorts = ports.split(",");
        }
        return this;
    }

    public JedisPoolBuilder setMasterName(String masterName) {
        this.masterName = masterName;
        return this;
    }

    public JedisPoolBuilder setShardedMasterNames(String[] shardedMasterNames) {
        this.shardedMasterNames = shardedMasterNames;
        return this;
    }

    public JedisPoolBuilder setShardedMasterNames(String shardedMasterNames) {
        if (shardedMasterNames != null) {
            this.shardedMasterNames = shardedMasterNames.split(",");
        }
        return this;
    }

    public JedisPoolBuilder setPoolSize(int poolSize) {
        this.poolSize = poolSize;
        return this;
    }

    public JedisPoolBuilder setDatabase(int database) {
        this.database = database;
        return this;
    }

    public JedisPoolBuilder setPassword(String password) {
        this.password = password;
        return this;
    }

    public JedisPoolBuilder setTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public JedisPoolBuilder setPoolName(String poolName) {
        this.poolName = poolName;
        return this;
    }

    /**
     * URL example direct/sentinel:[sentinel or redis address and
     * port]
     * ?masterNames=xx,xx&poolSize=x&database=x&password=x&timeout=x
     * <p/>
     * single direct redis: direct://localhost:6379?poolSize=5 *
     * <p/>
     * sentinel pool:
     * sentinel://sc-1:26379,sc-2:26379?masterNames=default
     * &pollSize=100
     * <p/>
     * sharding sentinel:
     * sentinel://sc-1:26379,sc-2:26379?masterNames=
     * shard1,shard2&pollSize=100
     */
    public JedisPoolBuilder setUrl(String url) {
        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException ex) {
            logger.error("Incorrect URI for initializing Jedis pool", ex);
            return this;
        }
        String authority = uri.getAuthority();
        if (authority != null) {
            String hosts = "";
            String ports = "";
            for (String auth : authority.split(",")) {
                String[] hostPort = auth.split(":");
                if (hostPort.length == 2) {
                    hosts += hostPort[0] + ",";
                    ports += hostPort[1] + ",";
                } else {
                    logger.error("Incorrect authority format in URI, should be formatted as host:port");
                    return this;
                }
            }
            setHosts(hosts);
            setPorts(ports);
        }

        final Properties prop = new Properties();
        String query = uri.getQuery();
        if (query != null) {
            try {
                prop.load(new StringReader(query.replace("&", "\n")));
            } catch (IOException ex) {
                logger.error("Failed to load the URI query string as stream", ex);
                return this;
            }
        } else {
            logger.error("No redis pool information set in query part of URI");
            return this;
        }
        if ("direct".equals(uri.getScheme())) {
            String masterName = DIRECT_POOL_PREFIX + authority;
            setMasterName(masterName);
            setShardedMasterNames(masterName);
        } else if ("sentinel".equals(uri.getScheme())) {
            String masterNames = prop.getProperty("masterNames");
            if (masterNames != null) {
                // Set the first as the masterName
                setMasterName(masterNames.split(",")[0]);
                setShardedMasterNames(masterNames);
            }
        }

        if (prop.getProperty("poolSize") != null) {
            setPoolSize(Integer.parseInt(prop.getProperty("poolSize")));
        }
        if (prop.getProperty("database") != null) {
            setDatabase(Integer.parseInt(prop.getProperty("database")));
        }
        if (prop.getProperty("password") != null) {
            setPassword(prop.getProperty("password"));
        }
        if (prop.getProperty("timeout") != null) {
            setTimeout(Integer.parseInt(prop.getProperty("timeout")));
        }
        if (prop.getProperty("poolName") != null) {
            setPoolName((prop.getProperty("poolName")));
        }
        return this;
    }

    public JedisPool buildPool() {

        if (sentinelHosts == null) {
            throw new IllegalArgumentException("Hostname is null or not set correctly");
        }

        if (masterName == null || "".equals(masterName)) {
            throw new IllegalArgumentException("masterName is null or empty");
        }

        if (poolSize < 1) {
            throw new IllegalArgumentException("poolSize is less then one");
        }

        JedisPoolConfig config = JedisPool.createPoolConfig(poolSize);
        ConnectionInfo connectionInfo = buildConnectionInfo();

        if (isDirect(masterName)) {
            return buildDirectPool(masterName, connectionInfo, config);
        }
        if (sentinelHosts == null || sentinelHosts.length == 0) {
            throw new IllegalArgumentException("sentinelHosts is null or empty");
        }
        return buildSentinelPool(masterName, connectionInfo, config);
    }

    public List<JedisPool> buildShardedPools() {

        if (shardedMasterNames == null || shardedMasterNames.length == 0 || "".equals(shardedMasterNames[0])) {
            throw new IllegalArgumentException("shardedMasterNames is null or empty");
        }

        if (poolSize < 1) {
            throw new IllegalArgumentException("poolSize is less then one");
        }

        JedisPoolConfig config = JedisPool.createPoolConfig(poolSize);

        List<JedisPool> jedisPools = new ArrayList<JedisPool>();

        if (isDirect(shardedMasterNames[0])) {
            shardedMasterNames[0] = shardedMasterNames[0].substring(shardedMasterNames[0].indexOf(":") + 1,
                    shardedMasterNames[0].length());
            for (String theMasterName : shardedMasterNames) {
                ConnectionInfo connectionInfo = buildConnectionInfo();
                connectionInfo.setPoolName(connectionInfo.getPoolName() + "-" + theMasterName);
                jedisPools.add(buildDirectPool("a:" + theMasterName, connectionInfo, config));
            }
        } else {

            if (sentinelHosts == null || sentinelHosts.length == 0) {
                throw new IllegalArgumentException("sentinelHosts is null or empty");
            }

            for (String theMasterName : shardedMasterNames) {
                ConnectionInfo connectionInfo = buildConnectionInfo();
                connectionInfo.setPoolName(connectionInfo.getPoolName() + "-" + theMasterName);
                jedisPools.add(buildSentinelPool(theMasterName, connectionInfo, config));
            }
        }
        return jedisPools;
    }

    public String[] getShardedMasterNames() {
        return shardedMasterNames == null ? EMPTY_MASTERS: shardedMasterNames;
    }

    /**
     * This static function is used to validate the url format is
     * correct or not. The format should be a URI formatted, like
     * this: direct/sentinel:[sentinel or redis address and
     * port]?masterNames
     * =xx,xx&poolSize=x&database=x&password=x&timeout=x
     * 
     * @return true if validation pass, otherwise return false.
     */
    public static boolean validateUrl(String url) {
        URI uri;
        try {
            uri = new URI(url);
        } catch (Exception ex) {
            return false;
        }

        if ((!"direct".equals(uri.getScheme())) && (!"sentinel".equals(uri.getScheme()))) {
            return false;
        }

        if (uri.getAuthority() == null || (!uri.getAuthority().contains(":"))) {
            return false;
        }

        return true;
    }

    private JedisPool buildDirectPool(String directMasterName, ConnectionInfo connectionInfo, JedisPoolConfig config) {
        String hostPortStr = directMasterName.substring(directMasterName.indexOf(":") + 1, directMasterName.length());
        String[] hostPort = hostPortStr.split(":");

        logger.info("Building JedisDirectPool, on redis server " + hostPort[0] + " ,port is " + hostPort[1]);

        HostAndPort masterAddress = new HostAndPort(hostPort[0], Integer.parseInt(hostPort[1]));
        return new JedisDirectPool(masterAddress, connectionInfo, config);
    }

    private JedisPool buildSentinelPool(String sentinelMasterName, ConnectionInfo connectionInfo, JedisPoolConfig config) {
        logger.info("Building JedisSentinelPool, on sentinel sentinelHosts:" + Arrays.toString(sentinelHosts)
                + " ,sentinelPort is " + Arrays.toString(sentinelPorts) + " ,masterName is " + sentinelMasterName);

        HostAndPort[] sentinelAddress = new HostAndPort[sentinelHosts.length];
        for (int i = 0; i < sentinelHosts.length; i++) {
            int port = sentinelPorts == null ? sentinelPort : Integer.parseInt(sentinelPorts[i]);
            sentinelAddress[i] = new HostAndPort(sentinelHosts[i], port);
        }

        return new JedisSentinelPool(sentinelAddress, sentinelMasterName, connectionInfo, config);
    }

    private static boolean isDirect(String masterName) {
        return masterName.startsWith(DIRECT_POOL_PREFIX);
    }

    private ConnectionInfo buildConnectionInfo() {
        if (poolName == null) {
            logger.warn("Oh, you did not specify the pool name for redis, it is better have a specific name, please set it, man!");
        }

        return new ConnectionInfo(database, password, timeout, poolName);
    }
}
