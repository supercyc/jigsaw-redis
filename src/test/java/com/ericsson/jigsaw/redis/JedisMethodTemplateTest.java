package com.ericsson.jigsaw.redis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.ericsson.jigsaw.redis.embedded.EmbeddedRedisBuilder;
import com.ericsson.jigsaw.redis.pool.JedisPool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import static org.assertj.core.api.Assertions.assertThat;

public class JedisMethodTemplateTest {

    private JedisTemplate jedisTemplate;

    @Before
    public void setup() {
        Jedis embeddedRedis = new EmbeddedRedisBuilder().createEmbeddedJedis();
        JedisPool jedisPool = Mockito.mock(JedisPool.class);
        Mockito.when(jedisPool.getResource()).thenReturn(embeddedRedis);

        jedisTemplate = new JedisTemplate(jedisPool);
    }

    @Test
    public void stringActions() throws InterruptedException {
        String key = "test.string.key";
        String notExistKey = key + "not.exist";
        String value = "123";

        //get/set
        jedisTemplate.set(key, value);
        assertThat(jedisTemplate.get(key)).isEqualTo(value);
        assertThat(jedisTemplate.get(notExistKey)).isNull();

        //getAsInt/getAsLong
        jedisTemplate.set(key, value);
        assertThat(jedisTemplate.getAsInt(key)).isEqualTo(123);
        assertThat(jedisTemplate.getAsInt(notExistKey)).isNull();

        jedisTemplate.set(key, value);
        assertThat(jedisTemplate.getAsLong(key)).isEqualTo(123L);
        assertThat(jedisTemplate.getAsLong(notExistKey)).isNull();

        //setnx
        jedisTemplate.set(key, value);
        assertThat(jedisTemplate.setnx(key, value)).isFalse();
        assertThat(jedisTemplate.setnx(key + "nx", value)).isTrue();

        //setex
        jedisTemplate.setex(key + "ex", value + "ex", 1);
        assertThat(jedisTemplate.get(key + "ex")).isEqualTo(value + "ex");
        Thread.currentThread().sleep(2000);
        assertThat(jedisTemplate.get(key + "ex")).isNull();

        //setexnx
        assertThat(jedisTemplate.setnxex(key + "nxex", value, 1)).isTrue();
        assertThat(jedisTemplate.setnxex(key + "nxex", value, 1)).isFalse();
        Thread.currentThread().sleep(2000);
        assertThat(jedisTemplate.get(key + "nxex")).isNull();

        //mget
        jedisTemplate.set(key + "1", value + "1");
        jedisTemplate.set(key + "2", value + "2");
        List<String> result = jedisTemplate.mget(key + "1", key + "2");
        assertThat(result).contains(value + "1", value + "2");

        //getset
        jedisTemplate.set(key, value);
        String oldValue = jedisTemplate.getSet(key, value + "2");
        assertThat(oldValue).isEqualTo(value);
        assertThat(jedisTemplate.get(key)).isEqualTo(value + "2");

        //incr/decr
        jedisTemplate.set(key, value);
        jedisTemplate.incr(key);
        assertThat(jedisTemplate.get(key)).isEqualTo("124");
        jedisTemplate.incrBy(key, 2);
        assertThat(jedisTemplate.get(key)).isEqualTo("126");
        jedisTemplate.decr(key);
        assertThat(jedisTemplate.get(key)).isEqualTo("125");
        jedisTemplate.decrBy(key, 3);
        assertThat(jedisTemplate.get(key)).isEqualTo("122");

        //del
        assertThat(jedisTemplate.del(key)).isTrue();
        assertThat(jedisTemplate.del(notExistKey)).isFalse();

    }

    @Test
    public void hashActions() {
        String key = "test.string.key";
        String field1 = "aa";
        String field2 = "bb";
        String notExistField = field1 + "not.exist";
        String value1 = "123";
        String value2 = "456";

        //hget/hset
        jedisTemplate.hset(key, field1, value1);
        assertThat(jedisTemplate.hget(key, field1)).isEqualTo(value1);
        assertThat(jedisTemplate.hget(key, notExistField)).isNull();

        //hmget/hmset
        Map<String, String> map = new HashMap<String, String>();
        map.put(field1, value1);
        map.put(field2, value2);
        jedisTemplate.hmset(key, map);

        assertThat(jedisTemplate.hmget(key, new String[] { field1, field2 })).containsExactly(value1, value2);

        //hgetall
        Map resultMap = jedisTemplate.hgetAll(key);
        assertThat(resultMap).containsKeys(map.keySet().toArray());

        //hsetnx
        assertThat(jedisTemplate.hsetnx(key, field1, value1)).isFalse();

        //hincr
        jedisTemplate.hset(key, field1, value1);
        jedisTemplate.hincrBy(key, field1, 2);
        assertThat(jedisTemplate.hget(key, field1)).isEqualTo("125");

        //hkeys
        assertThat(jedisTemplate.hkeys(key)).contains(field1, field2);

        //hlen 
        assertThat(jedisTemplate.hlen(key)).isEqualTo(2);

        //hexists
        assertThat(jedisTemplate.hexists(key, field1)).isTrue();
        assertThat(jedisTemplate.hexists(key, field1 + "NotExist")).isFalse();

        //hdel
        assertThat(jedisTemplate.hdel(key, field1));
        assertThat(jedisTemplate.hget(key, field1)).isNull();
    }

    @Test
    public void listActions() {
        String key = "test.list.key";
        String key2 = "test.list.key2";
        String value = "123";
        String value2 = "456";
        String value3 = "789";

        // push/pop single element
        jedisTemplate.del(key);
        jedisTemplate.lpush(key, value);
        assertThat(jedisTemplate.llen(key)).isEqualTo(1);
        assertThat(jedisTemplate.rpop(key)).isEqualTo(value);
        assertThat(jedisTemplate.rpop(key)).isNull();

        // push/pop/blocking pop 3 elements
        jedisTemplate.del(key);
        jedisTemplate.lpush(key, value, value2, value3);
        assertThat(jedisTemplate.llen(key)).isEqualTo(3);
        assertThat(jedisTemplate.rpop(key)).isEqualTo(value);
        assertThat(jedisTemplate.brpop(key)).isEqualTo(value2);
        assertThat(jedisTemplate.brpop(1, key)).isEqualTo(value3);

        assertThat(jedisTemplate.rpop(key)).isNull();
        assertThat(jedisTemplate.rpop(key + "notExist")).isNull();

        //rpoplpush
        jedisTemplate.del(key);
        jedisTemplate.lpush(key, value, value2);
        jedisTemplate.rpoplpush(key, key2);
        assertThat(jedisTemplate.lindex(key2, 0)).isEqualTo(value);
        jedisTemplate.brpoplpush(key, key2, 1);
        assertThat(jedisTemplate.lindex(key2, 0)).isEqualTo(value2);

        //lrange
        jedisTemplate.del(key);
        jedisTemplate.lpush(key, value, value2, value3);
        List<String> result = jedisTemplate.lrange(key, 0, 1);
        assertThat(result).containsExactly(value3, value2);

        // ltrim
        jedisTemplate.del(key);
        jedisTemplate.lpush(key, "1", "2", "3", "4", "5");
        jedisTemplate.ltrim(key, 0, 3);
        result = jedisTemplate.lrange(key, 0, -1);
        assertThat(result).containsExactly("5", "4", "3", "2");

        jedisTemplate.ltrimFromLeft(key, 3);
        result = jedisTemplate.lrange(key, 0, -1);
        assertThat(result).containsExactly("5", "4", "3");

        // remove elements
        jedisTemplate.del(key);
        jedisTemplate.lpush(key, value, value, value);
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
        String member1 = "abc";
        String member2 = "def";
        String member3 = "ghi";
        double score1 = 1;
        double score11 = 11;
        double score2 = 2;
        double score3 = 3;

        // zadd
        assertThat(jedisTemplate.zadd(key, score1, member1)).isTrue();
        assertThat(jedisTemplate.zadd(key, score2, member2)).isTrue();

        // zscore
        assertThat(jedisTemplate.zscore(key, member1)).isEqualTo(score1);

        // zrang
        assertThat(jedisTemplate.zrank(key, member1)).isEqualTo(0);
        assertThat(jedisTemplate.zrevrank(key, member1)).isEqualTo(1);

        // zcard
        assertThat(jedisTemplate.zcard(key)).isEqualTo(2);
        assertThat(jedisTemplate.zcard(key + "not.exist")).isEqualTo(0);

        // zcount
        initSortedSet(key, member1, member2, member3, score1, score2, score3);

        assertThat(jedisTemplate.zcount(key, 1, 2)).isEqualTo(2);
        assertThat(jedisTemplate.zcount(key, 3, 100)).isEqualTo(1);

        // zrem
        initSortedSet(key, member1, member2, member3, score1, score2, score3);
        assertThat(jedisTemplate.zrem(key, member2)).isTrue();
        assertThat(jedisTemplate.zcard(key)).isEqualTo(2);
        assertThat(jedisTemplate.zrem(key, member2 + "not.exist")).isFalse();
        
        initSortedSet(key, member1, member2, member3, score1, score2, score3);
        assertThat(jedisTemplate.zremByRank(key, 0, 1)).isEqualTo(2);
        assertThat(jedisTemplate.zcard(key)).isEqualTo(1);
        
        initSortedSet(key, member1, member2, member3, score1, score2, score3);
        assertThat(jedisTemplate.zremByRank(key, 1, 2)).isEqualTo(2);
        assertThat(jedisTemplate.zcard(key)).isEqualTo(1);

        // zrange
        initSortedSet(key, member1, member2, member3, score1, score2, score3);
        assertThat(jedisTemplate.zrange(key, 0, 1)).containsExactly(member1, member2);
        assertThat(jedisTemplate.zrange(key, 2, 100)).containsExactly(member3);
        assertThat(jedisTemplate.zrangeByScore(key, 1, 2)).containsExactly(member1, member2);
        assertThat(jedisTemplate.zrangeByScore(key, 3, 100)).containsExactly(member3);
        Set<Tuple> result = jedisTemplate.zrangeWithScores(key, 0, 1);
        Tuple tuple = result.iterator().next();
        assertThat(tuple.getElement()).isEqualTo(member1);
        assertThat(tuple.getScore()).isEqualTo(score1);

        // zrevzrang
        initSortedSet(key, member1, member2, member3, score1, score2, score3);
        assertThat(jedisTemplate.zrevrange(key, 0, 1)).containsExactly(member3, member2);
        assertThat(jedisTemplate.zrevrange(key, 2, 100)).containsExactly(member1);
        assertThat(jedisTemplate.zrevrangeByScore(key, 3, 2)).containsExactly(member3, member2);
        assertThat(jedisTemplate.zrevrangeByScore(key, 1, -100)).containsExactly(member1);
        result = jedisTemplate.zrevrangeWithScores(key, 0, 1);
        tuple = result.iterator().next();
        assertThat(tuple.getElement()).isEqualTo(member3);
        assertThat(tuple.getScore()).isEqualTo(score3);

        // unique & zscore
        initSortedSet(key, member1, member2, member3, score1, score2, score3);
        assertThat(jedisTemplate.zadd(key, score11, member1)).isFalse();
        assertThat(jedisTemplate.zcard(key)).isEqualTo(3);
        assertThat(jedisTemplate.zscore(key, member1)).isEqualTo(score11);
        assertThat(jedisTemplate.zscore(key, member1 + "not.exist")).isNull();
    }

    private void initSortedSet(String key, String member, String member2, String member3, double score1, double score2,
            double score3) {
        jedisTemplate.del(key);
        jedisTemplate.zadd(key, score1, member);
        jedisTemplate.zadd(key, score2, member2);
        jedisTemplate.zadd(key, score3, member3);
    }

    @Test
    public void serverActions() {
        String key = "test.string.key";
        String value = "123";

        //flushDB
        jedisTemplate.set(key, value);
        assertThat(jedisTemplate.get(key)).isEqualTo(value);

        jedisTemplate.flushDB();

        assertThat(jedisTemplate.dbSize()).isEqualTo(0);
        assertThat(jedisTemplate.get(key)).isNull();

        //ping
        String result = jedisTemplate.ping();
        assertThat(result).isNotNull();
        assertThat(result).isEqualTo("OK");
    }
}
