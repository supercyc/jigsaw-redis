package com.ericsson.jigsaw.redis.pool;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.Test;

public class PoolBuilderTest {

    @Test
    public void testValidate() {
        assertThat(JedisPoolBuilder.validateUrl(null)).isFalse();

        assertThat(JedisPoolBuilder.validateUrl("")).isFalse();

        assertThat(JedisPoolBuilder.validateUrl("direct:192.168.0.15:6379")).isFalse();

        assertThat(JedisPoolBuilder.validateUrl("direc://192.168.0.15:6379")).isFalse();

        assertThat(JedisPoolBuilder.validateUrl("direct://192.168.0.15:6379")).isTrue();

        assertThat(JedisPoolBuilder.validateUrl("direct://192.168.0.15")).isFalse();
    }

    @Test
    public void setDirectShardingUrl() {

        JedisPoolBuilder builder = new JedisPoolBuilder();

        builder.setUrl(
                "direct://192.168.0.15:6379,192.168.0.16:6379,192.168.0.17:6379?poolSize=200&poolName=rxEngineSessionPool");

        List<JedisPool> pools = builder.buildShardedPools();

        assertThat(pools).hasSize(3);

        JedisPoolBuilder anotherBuilder = new JedisPoolBuilder();

        anotherBuilder.setUrl("direct://192.168.0.16:6379,192.168.0.15:6379?poolSize=100&poolName=rxEngineSessionPool");

        List<JedisPool> anotherPools = anotherBuilder.buildShardedPools();

        assertThat(anotherPools).hasSize(2);

        assertThat(pools).contains(anotherPools.toArray(new JedisPool[2]));
    }

    @Test
    public void setDirectSingleUrl() {

        JedisPoolBuilder builder = new JedisPoolBuilder();

        builder.setUrl("direct://192.168.0.15:6379?poolSize=200&poolName=rxEngineSessionPool");

        JedisPool pool = builder.buildPool();

        assertThat(pool).isNotNull();

        assertThat(builder.getShardedMasterNames().length).isEqualTo(1);
    }

    @Test
    public void setEmptyUrl() {
        JedisPoolBuilder builder = new JedisPoolBuilder();
        builder.setUrl("direc");
        assertThat(builder.getShardedMasterNames().length).isEqualTo(0);
    }

    @Test
    public void setShardDirectSingleUrl() {

        JedisPoolBuilder builder = new JedisPoolBuilder();

        builder.setUrl("direct://192.168.0.15:6379?poolSize=200&poolName=rxEngineSessionPool");

        JedisPool pool = builder.buildPool();

        assertThat(pool).isNotNull();

        assertThat(builder.getShardedMasterNames().length).isEqualTo(1);
    }

    @Test
    public void setSentinelUrl() {
        JedisPoolBuilder builder = new JedisPoolBuilder();
        builder.setUrl(
                "sentinel://rdsmon-1:26379,rdsmon-2:26379?masterNames=default1,default2&poolSize=200&poolName=rxEngineSessionPool");

        List<JedisPool> pools = builder.buildShardedPools();

        JedisSentinelPool pool1 = (JedisSentinelPool) pools.get(0);
        JedisSentinelPool pool2 = (JedisSentinelPool) pools.get(1);

        //[HostAndPort:rdsmon-1:26379, ConnectionInfo:ConnectionInfo [database=0, password=null, timeout=2000, poolName=rxEngineSessionPool-default1]]
        //[HostAndPort:rdsmon-1:26379, ConnectionInfo:ConnectionInfo [database=0, password=null, timeout=2000, poolName=rxEngineSessionPool-default2]]
        assertThat(pool1.getSentinelPools().get(0)).isNotEqualTo(pool2.getSentinelPools().get(0));
    }
}
