package com.ericsson.jigsaw.redis;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import redis.clients.jedis.Jedis;

import com.ericsson.jigsaw.redis.JedisTemplate.JedisAction;
import com.ericsson.jigsaw.redis.JedisTemplate.JedisActionNoResult;
import com.ericsson.jigsaw.redis.embedded.EmbeddedRedisBuilder;
import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.shard.ConsistentHashShardingProvider;
import com.ericsson.jigsaw.redis.shard.NormHashShardingProvider;
import com.ericsson.jigsaw.redis.shard.ShardingProviderObserver;

public class JedisShardedTemplateTest {

    private JedisShardedTemplate jedisTemplate;

    private JedisShardedTemplate jedisTemplateWithNormShard;

    @Before
    public void setup() {
        Jedis embeddedRedis = new EmbeddedRedisBuilder().createEmbeddedJedis();
        JedisPool jedisPool1 = Mockito.mock(JedisPool.class);
        Mockito.when(jedisPool1.getResource()).thenReturn(embeddedRedis);

        Mockito.when(jedisPool1.getFormattedPoolName()).thenReturn("1");

        JedisPool jedisPool2 = Mockito.mock(JedisPool.class);
        Mockito.when(jedisPool2.getResource()).thenReturn(embeddedRedis);
        Mockito.when(jedisPool2.getFormattedPoolName()).thenReturn("2");

        JedisPool jedisPool3 = Mockito.mock(JedisPool.class);
        Mockito.when(jedisPool3.getResource()).thenReturn(embeddedRedis);
        Mockito.when(jedisPool3.getFormattedPoolName()).thenReturn("3");

        jedisTemplate = new JedisShardedTemplate(new ConsistentHashShardingProvider(true), null, new JedisPool[] {
                jedisPool1, jedisPool2, jedisPool3 });

        jedisTemplateWithNormShard = new JedisShardedTemplate(new NormHashShardingProvider(true), null,
                new JedisPool[] { jedisPool1, jedisPool2, jedisPool3 });

        ShardingProviderObserver.setObserveInterval(10);
    }

    @Test
    public void shardingTestWithNorm() {
        String key = "qos:session:z1-pl-2.cpt.ericsson.com;1304032909;1241590;";
        String value = "123";
        for (int i = 1000; i < 9999; i++) {
            jedisTemplateWithNormShard.set(key + i, value);
        }
    }

    public void shardingTest() {

        String key = "qos:session:z1-pl-2.cpt.ericsson.com;1304032909;1241590;";
        String value = "123";
        for (int i = 1000; i < 9999; i++) {
            jedisTemplate.set(key + i, value);
        }

        /*
         * for (Entry<JedisPool, AtomicLong> entry : jedisTemplate.counter.entrySet()) {
         * System.out.println("Result:" + entry.getKey().getFormattedPoolName() + ":::::"
         * + entry.getValue().longValue());
         * }
         */
    }

    @Test
    public void stringActions() {
        String key = "test.string.key";
        String notExistKey = key + "not.exist";
        String value = "123";

        jedisTemplate.set(key, value);
        assertThat(jedisTemplate.get(key)).isEqualTo(value);
        assertThat(jedisTemplate.get(notExistKey)).isNull();

        // assertThat(jedisTemplate.del(key)).isTrue();
        // assertThat(jedisTemplate.del(notExistKey)).isFalse();

        for (int i = 0; i < 1000; i++) {
            jedisTemplate.set(key + i, value);
        }
    }

    @Test
    public void hashActions() {
        String key = "test.string.key";
        String field1 = "aa";
        String field2 = "bb";
        String notExistField = field1 + "not.exist";
        String value1 = "123";
        String value2 = "456";

        // hget/hset
        jedisTemplate.hset(key, field1, value1);
        assertThat(jedisTemplate.hget(key, field1)).isEqualTo(value1);
        assertThat(jedisTemplate.hget(key, notExistField)).isNull();

        // hmget/hmset
        Map<String, String> map = new HashMap<String, String>();
        map.put(field1, value1);
        map.put(field2, value2);
        jedisTemplate.hmset(key, map);

        assertThat(jedisTemplate.hmget(key, new String[] { field1, field2 })).containsExactly(value1, value2);

        // hkeys
        assertThat(jedisTemplate.hkeys(key)).contains(field1, field2);

        // hdel
        assertThat(jedisTemplate.hdel(key, field1));
        assertThat(jedisTemplate.hget(key, field1)).isNull();
    }

    @Test
    public void listActions() {
        String key = "test.list.key";
        String value = "123";
        String value2 = "456";

        // push/pop single element
        jedisTemplate.lpush(key, value);
        assertThat(jedisTemplate.llen(key)).isEqualTo(1);
        assertThat(jedisTemplate.rpop(key)).isEqualTo(value);
        assertThat(jedisTemplate.rpop(key)).isNull();

        // push/pop two elements
        jedisTemplate.lpush(key, value);
        jedisTemplate.lpush(key, value2);
        assertThat(jedisTemplate.llen(key)).isEqualTo(2);
        assertThat(jedisTemplate.rpop(key)).isEqualTo(value);
        assertThat(jedisTemplate.rpop(key)).isEqualTo(value2);

        // remove elements
        jedisTemplate.lpush(key, value);
        jedisTemplate.lpush(key, value);
        jedisTemplate.lpush(key, value);
        assertThat(jedisTemplate.llen(key)).isEqualTo(3);
        assertThat(jedisTemplate.lremFirst(key, value)).isTrue();
        assertThat(jedisTemplate.llen(key)).isEqualTo(2);
        assertThat(jedisTemplate.lremAll(key, value)).isTrue();
        assertThat(jedisTemplate.llen(key)).isEqualTo(0);
        assertThat(jedisTemplate.lremAll(key, value)).isFalse();
    }

    @Test
    public void orderedSetActions() {
        String key = "test.orderedSet.key";
        String member = "abc";
        String member2 = "def";
        double score1 = 1;
        double score11 = 11;
        double score2 = 2;

        // zadd
        assertThat(jedisTemplate.zadd(key, score1, member)).isTrue();
        assertThat(jedisTemplate.zadd(key, score2, member2)).isTrue();

        // zcard
        assertThat(jedisTemplate.zcard(key)).isEqualTo(2);
        assertThat(jedisTemplate.zcard(key + "not.exist")).isEqualTo(0);

        // zrem
        assertThat(jedisTemplate.zrem(key, member2)).isTrue();
        assertThat(jedisTemplate.zcard(key)).isEqualTo(1);
        assertThat(jedisTemplate.zrem(key, member2 + "not.exist")).isFalse();

        // unique & zscore
        assertThat(jedisTemplate.zadd(key, score11, member)).isFalse();
        assertThat(jedisTemplate.zcard(key)).isEqualTo(1);
        assertThat(jedisTemplate.zscore(key, member)).isEqualTo(score11);
        assertThat(jedisTemplate.zscore(key, member + "not.exist")).isNull();
    }

    @Test
    public void execute() {

        final String key = "test.string.key";

        final String value = "123";

        jedisTemplate.execute(key, new JedisActionNoResult() {

            @Override
            public void action(Jedis jedis) {
                jedis.set(key, value);
            }
        });

        assertThat(jedisTemplate.get(key)).isEqualTo(value);

        String result = jedisTemplate.execute(key, new JedisAction<String>() {

            @Override
            public String action(Jedis jedis) {
                return jedis.get(key);
            }

        });

        assertThat(result).isEqualTo(value);
    }
}
