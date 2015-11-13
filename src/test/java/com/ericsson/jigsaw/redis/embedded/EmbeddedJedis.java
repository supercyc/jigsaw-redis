package com.ericsson.jigsaw.redis.embedded;

import static ch.lambdaj.collection.LambdaCollections.with;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import redis.clients.jedis.AdvancedBinaryJedisCommands;
import redis.clients.jedis.AdvancedJedisCommands;
import redis.clients.jedis.BasicCommands;
import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.DebugParams;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.MultiKeyBinaryCommands;
import redis.clients.jedis.MultiKeyCommands;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.ScriptingCommands;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;
import redis.clients.util.SafeEncoder;
import redis.clients.util.Slowlog;
import ch.lambdaj.function.convert.Converter;

import com.ericsson.jigsaw.redis.embedded.ListDatatypeOperations.ListPositionEnum;
import com.ericsson.jigsaw.redis.embedded.SortsetDatatypeOperations.ScoredByteBuffer;

public class EmbeddedJedis implements JedisCommands, BinaryJedisCommands, MultiKeyCommands, MultiKeyBinaryCommands,
        ScriptingCommands, BasicCommands, AdvancedJedisCommands, AdvancedBinaryJedisCommands {

    private static final ByteArrayToStringConverter BYTE_ARRAY_TO_STRING_CONVERTER = ByteArrayToStringConverter
            .createByteArrayToStringConverter();
    private static final StringToByteArrayConverter STRING_TO_BYTE_ARRAY_CONVERTER = StringToByteArrayConverter
            .createStringToByteArrayConverter();

    protected HashDatatypeOperations hashDatatypeOperations;
    protected ListDatatypeOperations listDatatypeOperations;
    protected SetDatatypeOperations setDatatypeOperations;
    protected SortsetDatatypeOperations sortsetDatatypeOperations;
    protected StringDatatypeOperations stringDatatypeOperations;
    protected KeysServerOperations keysServerOperations;
    protected PubSubServerOperations pubSubServerOperations;
    protected ConnectionServerOperations connectionServerOperations;
    protected ScriptingServerOperations scriptingServerOperations;
    protected TransactionServerOperations transactionServerOperations;

    public EmbeddedJedis() {
        hashDatatypeOperations = new HashDatatypeOperations();
        listDatatypeOperations = new ListDatatypeOperations();
        setDatatypeOperations = new SetDatatypeOperations();
        sortsetDatatypeOperations = new SortsetDatatypeOperations();
        stringDatatypeOperations = new StringDatatypeOperations();
        pubSubServerOperations = new PubSubServerOperations();
        connectionServerOperations = new ConnectionServerOperations();
        scriptingServerOperations = new ScriptingServerOperations();
        transactionServerOperations = new TransactionServerOperations(this);
        keysServerOperations = KeysServerOperations.createKeysServerOperations(hashDatatypeOperations,
                listDatatypeOperations, setDatatypeOperations, sortsetDatatypeOperations, stringDatatypeOperations);
    }

    @Override
    public String set(byte[] key, byte[] value) {
        keysServerOperations.del(key);
        stringDatatypeOperations.removeExpiration(key);
        return stringDatatypeOperations.set(key, value);
    }

    @Override
    public byte[] get(byte[] key) {
        updateTtl(key);
        checkValidTypeOrNone(key, StringDatatypeOperations.STRING);

        return stringDatatypeOperations.get(key);
    }

    @Override
    public Boolean exists(byte[] key) {
        updateTtl(key);
        return this.stringDatatypeOperations.exists(key);
    }

    @Override
    public String type(byte[] key) {
        updateTtl(key);
        return this.keysServerOperations.type(key);
    }

    @Override
    public Long expire(byte[] key, int seconds) {
        return this.keysServerOperations.expire(key, seconds);
    }

    @Override
    public Long expireAt(byte[] key, long unixTime) {
        return this.keysServerOperations.expireAt(key, unixTime);
    }

    @Override
    public Long ttl(byte[] key) {
        return this.keysServerOperations.ttl(key);
    }

    @Override
    public byte[] getSet(byte[] key, byte[] value) {
        updateTtl(key);
        checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
        stringDatatypeOperations.removeExpiration(key);

        return this.stringDatatypeOperations.getSet(key, value);
    }

    @Override
    public Long setnx(byte[] key, byte[] value) {
        updateTtl(key);
        checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
        return this.stringDatatypeOperations.setnx(key, value);
    }

    @Override
    public String setex(byte[] key, int seconds, byte[] value) {
        updateTtl(key);
        checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
        return this.stringDatatypeOperations.setex(key, seconds, value);
    }

    @Override
    public Long decrBy(byte[] key, long integer) {
        updateTtl(key);
        checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
        return this.stringDatatypeOperations.decrBy(key, integer);
    }

    @Override
    public Long decr(byte[] key) {
        updateTtl(key);
        checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
        return this.stringDatatypeOperations.decr(key);
    }

    @Override
    public Long incrBy(byte[] key, long integer) {
        updateTtl(key);
        checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
        return this.stringDatatypeOperations.incrBy(key, integer);
    }

    @Override
    public Long incr(byte[] key) {
        updateTtl(key);
        checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
        return this.stringDatatypeOperations.incr(key);
    }

    @Override
    public Long append(byte[] key, byte[] value) {
        updateTtl(key);
        checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
        return this.stringDatatypeOperations.append(key, value);
    }

    @Override
    public byte[] substr(byte[] key, int start, int end) {
        updateTtl(key);
        checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
        return this.stringDatatypeOperations.substr(key, start, end);
    }

    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        updateTtl(key);
        checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
        return this.hashDatatypeOperations.hset(key, field, value);
    }

    @Override
    public byte[] hget(byte[] key, byte[] field) {
        updateTtl(key);
        checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
        return this.hashDatatypeOperations.hget(key, field);
    }

    @Override
    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        updateTtl(key);
        checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
        return this.hashDatatypeOperations.hsetnx(key, field, value);
    }

    @Override
    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        updateTtl(key);
        checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
        return this.hashDatatypeOperations.hmset(key, hash);
    }

    @Override
    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        updateTtl(key);
        checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
        return this.hashDatatypeOperations.hmget(key, fields);
    }

    @Override
    public Long hincrBy(byte[] key, byte[] field, long value) {
        updateTtl(key);
        checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
        return this.hashDatatypeOperations.hincrBy(key, field, value);
    }

    @Override
    public Boolean hexists(byte[] key, byte[] field) {
        updateTtl(key);
        checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
        return this.hashDatatypeOperations.hexists(key, field);
    }

    @Override
    public Long hdel(byte[] key, byte[]... fields) {
        updateTtl(key);
        checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
        return this.hashDatatypeOperations.hdel(key, fields);
    }

    @Override
    public Long hlen(byte[] key) {
        updateTtl(key);
        checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
        return this.hashDatatypeOperations.hlen(key);
    }

    @Override
    public Set<byte[]> hkeys(byte[] key) {
        updateTtl(key);
        checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
        return this.hashDatatypeOperations.hkeys(key);
    }

    @Override
    public Collection<byte[]> hvals(byte[] key) {
        updateTtl(key);
        checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
        return this.hashDatatypeOperations.hvals(key);
    }

    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        updateTtl(key);
        checkValidTypeOrNone(key, HashDatatypeOperations.HASH);
        return this.hashDatatypeOperations.hgetAll(key);
    }

    @Override
    public Long rpush(byte[] key, byte[]... values) {
        updateTtl(key);
        checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
        return this.listDatatypeOperations.rpush(key, values);
    }

    @Override
    public Long lpush(byte[] key, byte[]... values) {
        updateTtl(key);
        checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
        return this.listDatatypeOperations.lpush(key, values);
    }

    @Override
    public byte[] rpoplpush(byte[] srckey, byte[] dstkey) {
        byte[] result = this.rpop(srckey);
        if (result != null) {
            this.lpush(dstkey, result);
        }
        return result;
    }

    @Override
    public byte[] brpoplpush(byte[] source, byte[] destination, int timeout) {
        List<byte[]> result = this.brpop(timeout, source);
        if (result != null && result.size() > 0) {
            byte[] value = result.get(1);
            this.lpush(destination, value);
            return value;
        }

        return null;
    }

    @Override
    public Long llen(byte[] key) {
        updateTtl(key);
        checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
        return this.listDatatypeOperations.llen(key);
    }

    @Override
    public List<byte[]> lrange(byte[] key, long start, long end) {
        updateTtl(key);
        checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
        return this.listDatatypeOperations.lrange(key, start, end);
    }

    @Override
    public String ltrim(byte[] key, long start, long end) {
        updateTtl(key);
        checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
        return this.listDatatypeOperations.ltrim(key, start, end);
    }

    @Override
    public byte[] lindex(byte[] key, long index) {
        updateTtl(key);
        checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
        return this.listDatatypeOperations.lindex(key, (int) index);
    }

    @Override
    public String lset(byte[] key, long index, byte[] value) {
        updateTtl(key);
        checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
        return this.listDatatypeOperations.lset(key, (int) index, value);
    }

    @Override
    public Long lrem(byte[] key, long count, byte[] value) {
        updateTtl(key);
        checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
        return this.listDatatypeOperations.lrem(key, count, value);
    }

    @Override
    public byte[] lpop(byte[] key) {
        updateTtl(key);
        checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
        return this.listDatatypeOperations.lpop(key);
    }

    @Override
    public byte[] rpop(byte[] key) {
        updateTtl(key);
        checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
        return this.listDatatypeOperations.rpop(key);
    }

    @Override
    public Long sadd(byte[] key, byte[]... member) {
        updateTtl(key);
        checkValidTypeOrNone(key, SetDatatypeOperations.SET);
        return this.setDatatypeOperations.sadd(key, member);
    }

    @Override
    public Set<byte[]> smembers(byte[] key) {
        updateTtl(key);
        checkValidTypeOrNone(key, SetDatatypeOperations.SET);
        return this.setDatatypeOperations.smembers(key);
    }

    @Override
    public Long srem(byte[] key, byte[]... member) {
        updateTtl(key);
        checkValidTypeOrNone(key, SetDatatypeOperations.SET);
        return this.setDatatypeOperations.srem(key, member);
    }

    @Override
    public byte[] spop(byte[] key) {
        updateTtl(key);
        checkValidTypeOrNone(key, SetDatatypeOperations.SET);
        return this.setDatatypeOperations.spop(key);
    }

    @Override
    public Long scard(byte[] key) {
        updateTtl(key);
        checkValidTypeOrNone(key, SetDatatypeOperations.SET);
        return this.setDatatypeOperations.scard(key);
    }

    @Override
    public Boolean sismember(byte[] key, byte[] member) {
        updateTtl(key);
        checkValidTypeOrNone(key, SetDatatypeOperations.SET);
        return this.setDatatypeOperations.sismember(key, member);
    }

    @Override
    public byte[] srandmember(byte[] key) {
        updateTtl(key);
        checkValidTypeOrNone(key, SetDatatypeOperations.SET);
        return this.setDatatypeOperations.srandmember(key);
    }

    @Override
    public Long zadd(byte[] key, double score, byte[] member) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zadd(key, score, member);
    }

    @Override
    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zadd(key, scoreMembers);
    }

    @Override
    public Set<byte[]> zrange(byte[] key, long start, long end) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zrange(key, start, end);
    }

    @Override
    public Long zrem(byte[] key, byte[]... members) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zrem(key, members);
    }

    @Override
    public Double zincrby(byte[] key, double score, byte[] member) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zincrby(key, score, member);
    }

    @Override
    public Long zrank(byte[] key, byte[] member) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zrank(key, member);
    }

    @Override
    public Long zrevrank(byte[] key, byte[] member) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zrevrank(key, member);
    }

    @Override
    public Set<byte[]> zrevrange(byte[] key, long start, long end) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zrevrange(key, start, end);
    }

    @Override
    public Set<Tuple> zrangeWithScores(byte[] key, long start, long end) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        Set<ScoredByteBuffer> rangeWithScore = this.sortsetDatatypeOperations.zrangeWithScores(key, start, end);
        return new LinkedHashSet<Tuple>(with(rangeWithScore).convert(toTuple()));
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        Set<ScoredByteBuffer> rangeWithScore = this.sortsetDatatypeOperations.zrevrangeWithScores(key, start, end);
        return new LinkedHashSet<Tuple>(with(rangeWithScore).convert(toTuple()));
    }

    @Override
    public Long zcard(byte[] key) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zcard(key);
    }

    @Override
    public Double zscore(byte[] key, byte[] member) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zscore(key, member);
    }

    @Override
    public List<byte[]> sort(byte[] key) {
        updateTtl(key);
        return this.keysServerOperations.sort(key);
    }

    @Override
    public Long zcount(byte[] key, double min, double max) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zcount(key, min, max);
    }

    @Override
    public Long zcount(byte[] key, byte[] min, byte[] max) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zcount(key, min, max);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zrangeByScore(key, min, max);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zrangeByScore(key, min, max, offset, count);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        Set<ScoredByteBuffer> zrangeByScoreWithScores = this.sortsetDatatypeOperations.zrangeByScoreWithScores(key,
                min, max);
        return new LinkedHashSet<Tuple>(with(zrangeByScoreWithScores).convert(toTuple()));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        Set<ScoredByteBuffer> zrangeByScoreWithScores = this.sortsetDatatypeOperations.zrangeByScoreWithScores(key,
                min, max, offset, count);
        return new LinkedHashSet<Tuple>(with(zrangeByScoreWithScores).convert(toTuple()));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        Set<ScoredByteBuffer> zrangeByScoreWithScores = this.sortsetDatatypeOperations.zrangeByScoreWithScores(key,
                min, max);
        return new LinkedHashSet<Tuple>(with(zrangeByScoreWithScores).convert(toTuple()));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset, int count) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        Set<ScoredByteBuffer> zrangeByScoreWithScores = this.sortsetDatatypeOperations.zrangeByScoreWithScores(key,
                min, max, offset, count);
        return new LinkedHashSet<Tuple>(with(zrangeByScoreWithScores).convert(toTuple()));
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zrevrangeByScore(key, max, min);
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zrevrangeByScore(key, max, min, offset, count);
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zrevrangeByScore(key, max, min);
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zrevrangeByScore(key, max, min);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        Set<ScoredByteBuffer> zrangeByScoreWithScores = this.sortsetDatatypeOperations.zrevrangeByScoreWithScores(key,
                max, min);
        return new LinkedHashSet<Tuple>(with(zrangeByScoreWithScores).convert(toTuple()));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        Set<ScoredByteBuffer> zrangeByScoreWithScores = this.sortsetDatatypeOperations.zrevrangeByScoreWithScores(key,
                max, min, offset, count);
        return new LinkedHashSet<Tuple>(with(zrangeByScoreWithScores).convert(toTuple()));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        Set<ScoredByteBuffer> zrangeByScoreWithScores = this.sortsetDatatypeOperations.zrevrangeByScoreWithScores(key,
                max, min);
        return new LinkedHashSet<Tuple>(with(zrangeByScoreWithScores).convert(toTuple()));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset, int count) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        Set<ScoredByteBuffer> zrangeByScoreWithScores = this.sortsetDatatypeOperations.zrevrangeByScoreWithScores(key,
                max, min, offset, count);
        return new LinkedHashSet<Tuple>(with(zrangeByScoreWithScores).convert(toTuple()));
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zrangeByScore(key, min, max);
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zrangeByScore(key, min, max, offset, count);
    }

    @Override
    public Long zremrangeByRank(byte[] key, long start, long end) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zremrangeByRank(key, start, end);
    }

    @Override
    public Long zremrangeByScore(byte[] key, double start, double end) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zremrangeByScore(key, start, end);
    }

    @Override
    public Long zremrangeByScore(byte[] key, byte[] start, byte[] end) {
        updateTtl(key);
        checkValidTypeOrNone(key, SortsetDatatypeOperations.ZSET);
        return this.sortsetDatatypeOperations.zremrangeByScore(key, start, end);
    }

    @Override
    public Long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
        updateTtl(key);
        checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
        return this.listDatatypeOperations.linsert(key, toListPosition().convert(where), pivot, value);
    }

    @Override
    public Long lpushx(byte[] key, byte[]... value) {
        updateTtl(key);
        checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
        return this.listDatatypeOperations.lpushx(key, value);
    }

    @Override
    public Long rpushx(byte[] key, byte[]... value) {
        updateTtl(key);
        checkValidTypeOrNone(key, ListDatatypeOperations.LIST);
        return this.listDatatypeOperations.rpushx(key, value);
    }

    @Override
    public Boolean setbit(byte[] key, long offset, byte[] value) {
        updateTtl(key);
        checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
        return this.stringDatatypeOperations.setbit(key, offset, value);
    }

    @Override
    public Boolean getbit(byte[] key, long offset) {
        updateTtl(key);
        checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
        return this.stringDatatypeOperations.getbit(key, offset);
    }

    @Override
    public Long setrange(byte[] key, long offset, byte[] value) {
        updateTtl(key);
        checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
        return this.stringDatatypeOperations.setrange(key, offset, value);
    }

    @Override
    public byte[] getrange(byte[] key, long startOffset, long endOffset) {
        updateTtl(key);
        checkValidTypeOrNone(key, StringDatatypeOperations.STRING);
        return this.stringDatatypeOperations.getrange(key, startOffset, endOffset);
    }

    @Override
    public Long dbSize() {
        updateAllTtlTimes();
        return this.keysServerOperations.dbSize();
    }

    @Override
    public String flushDB() {
        return this.keysServerOperations.flushDB();
    }

    @Override
    public String flushAll() {
        return this.flushDB();
    }

    @Override
    public Long del(final byte[]... keys) {
        return this.keysServerOperations.del(keys);
    }

    @Override
    public Long del(final String... keys) {
        byte[][] arrayOfKeys = with(keys).convert(toByteArray()).toArray(new byte[keys.length][]);
        return this.del(arrayOfKeys);
    }

    @Override
    public String rename(final String oldkey, final String newkey) {
        return this.rename(toByteArray().convert(oldkey), toByteArray().convert(newkey));
    }

    @Override
    public String rename(final byte[] oldkey, final byte[] newkey) {
        updateTtl(oldkey);
        updateTtl(newkey);
        return this.keysServerOperations.rename(oldkey, newkey);
    }

    /**
     * Rename oldkey into newkey but fails if the destination key newkey already
     * exists.
     * <p>
     * Time complexity: O(1)
     * 
     * @param oldkey
     * @param newkey
     * @return Integer reply, specifically: 1 if the key was renamed 0 if the
     *         target key already exist
     */
    @Override
    public Long renamenx(final byte[] oldkey, final byte[] newkey) {
        updateTtl(oldkey);
        updateTtl(newkey);
        return this.keysServerOperations.renamenx(oldkey, newkey);
    }

    @Override
    public Set<byte[]> keys(final byte[] pattern) {
        updateAllTtlTimes();
        return this.keysServerOperations.keys(pattern);
    }

    @Override
    public Set<String> keys(final String pattern) {
        Set<byte[]> result = this.keys(toByteArray().convert(pattern));
        return new LinkedHashSet<String>(with(result).convert(toStringValue()));
    }

    @Override
    public Long persist(final String key) {
        return this.persist(toByteArray().convert(key));
    }

    @Override
    public Long persist(final byte[] key) {
        updateTtl(key);
        return this.keysServerOperations.persist(key);
    }

    /**
     * Rename oldkey into newkey but fails if the destination key newkey already
     * exists.
     * <p>
     * Time complexity: O(1)
     * 
     * @param oldkey
     * @param newkey
     * @return Integer reply, specifically: 1 if the key was renamed 0 if the
     *         target key already exist
     */
    @Override
    public Long renamenx(final String oldkey, final String newkey) {
        return this.renamenx(toByteArray().convert(oldkey), toByteArray().convert(newkey));
    }

    @Override
    public Long move(final byte[] key, final int dbIndex) {
        return 0L;
    }

    @Override
    public String set(String key, String value) {
        return this.set(toByteArray().convert(key), toByteArray().convert(value));
    }

    @Override
    public String get(String key) {
        byte[] result = this.get(toByteArray().convert(key));
        return toStringValue().convert(result);
    }

    @Override
    public List<String> mget(String... keys) {
        List<String> result = new ArrayList<String>();
        for (String key : keys) {
            result.add(this.get(key));
        }

        return result;
    }

    @Override
    public List<byte[]> mget(byte[]... keys) {
        List<byte[]> result = new ArrayList<byte[]>();
        for (byte[] key : keys) {
            result.add(this.get(key));
        }

        return result;
    }

    @Override
    public Boolean exists(String key) {
        return this.exists(toByteArray().convert(key));
    }

    @Override
    public String type(String key) {
        return this.type(toByteArray().convert(key));
    }

    @Override
    public Long expire(String key, int seconds) {
        return this.expire(toByteArray().convert(key), seconds);
    }

    @Override
    public Long expireAt(String key, long unixTime) {
        return expireAt(toByteArray().convert(key), unixTime);
    }

    @Override
    public Long ttl(String key) {
        return this.ttl(toByteArray().convert(key));
    }

    @Override
    public Boolean setbit(String key, long offset, boolean value) {
        return this.setbit(toByteArray().convert(key), offset, toBooleanByteArray().convert(value));
    }

    @Override
    public Boolean getbit(String key, long offset) {
        return this.getbit(toByteArray().convert(key), offset);
    }

    @Override
    public Long setrange(String key, long offset, String value) {
        return this.setrange(toByteArray().convert(key), offset, toByteArray().convert(value));
    }

    @Override
    public String getrange(String key, long startOffset, long endOffset) {
        byte[] result = this.getrange(toByteArray().convert(key), startOffset, endOffset);
        return toStringValue().convert(result);
    }

    @Override
    public String getSet(String key, String value) {
        byte[] result = this.getSet(toByteArray().convert(key), toByteArray().convert(value));
        return toStringValue().convert(result);
    }

    @Override
    public Long setnx(String key, String value) {
        return this.setnx(toByteArray().convert(key), toByteArray().convert(value));
    }

    @Override
    public String setex(String key, int seconds, String value) {
        return this.setex(toByteArray().convert(key), seconds, toByteArray().convert(value));
    }

    public String set(final String key, final String value, final String nxxx, final String expx, final int time) {
        return this.stringDatatypeOperations.set(toByteArray().convert(key), toByteArray().convert(value), nxxx, expx,
                time);
    }

    @Override
    public Long decrBy(String key, long integer) {
        return this.decrBy(toByteArray().convert(key), integer);
    }

    @Override
    public Long decr(String key) {
        return this.decr(toByteArray().convert(key));
    }

    @Override
    public Long incrBy(String key, long integer) {
        return this.incrBy(toByteArray().convert(key), integer);
    }

    @Override
    public Long incr(String key) {
        return this.incr(toByteArray().convert(key));
    }

    @Override
    public Long append(String key, String value) {
        return this.append(toByteArray().convert(key), toByteArray().convert(value));
    }

    @Override
    public String substr(String key, int start, int end) {
        byte[] result = this.substr(toByteArray().convert(key), start, end);
        return toStringValue().convert(result);
    }

    @Override
    public Long hset(String key, String field, String value) {
        return this.hset(toByteArray().convert(key), toByteArray().convert(field), toByteArray().convert(value));
    }

    @Override
    public String hget(String key, String field) {
        byte[] result = this.hget(toByteArray().convert(key), toByteArray().convert(field));
        return toStringValue().convert(result);
    }

    @Override
    public Long hsetnx(String key, String field, String value) {
        return this.hsetnx(toByteArray().convert(key), toByteArray().convert(field), toByteArray().convert(value));
    }

    @Override
    public String hmset(String key, Map<String, String> hash) {
        return this.hmset(toByteArray().convert(key), toMapByteArray().convert(hash));
    }

    @Override
    public List<String> hmget(String key, String... fields) {
        byte[][] arrayOfFields = with(fields).convert(toByteArray()).toArray(new byte[fields.length][]);
        List<byte[]> result = this.hmget(toByteArray().convert(key), arrayOfFields);
        return with(result).convert(toStringValue());
    }

    @Override
    public Long hincrBy(String key, String field, long value) {
        return this.hincrBy(toByteArray().convert(key), toByteArray().convert(field), value);
    }

    @Override
    public Boolean hexists(String key, String field) {
        return hexists(toByteArray().convert(key), toByteArray().convert(field));
    }

    @Override
    public Long hdel(String key, String... fields) {
        byte[][] arrayOfFields = with(fields).convert(toByteArray()).toArray(new byte[fields.length][]);
        return this.hdel(toByteArray().convert(key), arrayOfFields);
    }

    @Override
    public Long hlen(String key) {
        return this.hlen(toByteArray().convert(key));
    }

    @Override
    public Set<String> hkeys(String key) {
        Set<byte[]> result = this.hkeys(toByteArray().convert(key));
        return new LinkedHashSet<String>(with(result).convert(toStringValue()));
    }

    @Override
    public List<String> hvals(String key) {
        Collection<byte[]> result = this.hvals(toByteArray().convert(key));
        return new LinkedList<String>(with(result).convert(toStringValue()));
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        Map<byte[], byte[]> result = this.hgetAll(toByteArray().convert(key));
        return toMapString().convert(result);
    }

    @Override
    public Long rpush(String key, String... values) {
        byte[][] arrayOfFields = with(values).convert(toByteArray()).toArray(new byte[values.length][]);
        return this.rpush(toByteArray().convert(key), arrayOfFields);
    }

    @Override
    public Long lpush(String key, String... values) {
        byte[][] arrayOfFields = with(values).convert(toByteArray()).toArray(new byte[values.length][]);
        return this.lpush(toByteArray().convert(key), arrayOfFields);
    }

    @Override
    public Long llen(String key) {
        return this.llen(toByteArray().convert(key));
    }

    @Override
    public List<String> lrange(String key, long start, long end) {
        List<byte[]> result = this.lrange(toByteArray().convert(key), (int) start, (int) end);
        return with(result).convert(toStringValue());
    }

    @Override
    public String ltrim(String key, long start, long end) {
        return this.ltrim(toByteArray().convert(key), (int) start, (int) end);
    }

    @Override
    public String lindex(String key, long index) {
        byte[] result = this.lindex(toByteArray().convert(key), (int) index);
        return toStringValue().convert(result);
    }

    @Override
    public String lset(String key, long index, String value) {
        return this.lset(toByteArray().convert(key), (int) index, toByteArray().convert(value));
    }

    @Override
    public Long lrem(String key, long count, String value) {
        return this.lrem(toByteArray().convert(key), (int) count, toByteArray().convert(value));
    }

    @Override
    public String lpop(String key) {
        byte[] result = this.lpop(toByteArray().convert(key));
        return toStringValue().convert(result);
    }

    @Override
    public String rpop(String key) {
        byte[] result = this.rpop(toByteArray().convert(key));
        return toStringValue().convert(result);
    }

    @Override
    public Long sadd(String key, String... members) {
        byte[][] arrayOfFields = with(members).convert(toByteArray()).toArray(new byte[members.length][]);
        return this.sadd(toByteArray().convert(key), arrayOfFields);
    }

    @Override
    public Set<String> smembers(String key) {
        Set<byte[]> result = this.smembers(toByteArray().convert(key));
        return new LinkedHashSet<String>(with(result).convert(toStringValue()));
    }

    @Override
    public Long srem(String key, String... members) {
        byte[][] arrayOfFields = with(members).convert(toByteArray()).toArray(new byte[members.length][]);
        return this.srem(toByteArray().convert(key), arrayOfFields);
    }

    @Override
    public String spop(String key) {
        byte[] result = this.spop(toByteArray().convert(key));
        return toStringValue().convert(result);
    }

    @Override
    public Long scard(String key) {
        return this.scard(toByteArray().convert(key));
    }

    @Override
    public Boolean sismember(String key, String member) {
        return this.sismember(toByteArray().convert(key), toByteArray().convert(member));
    }

    @Override
    public String srandmember(String key) {
        byte[] result = this.srandmember(toByteArray().convert(key));
        return toStringValue().convert(result);
    }

    @Override
    public Long zadd(String key, double score, String member) {
        return this.zadd(toByteArray().convert(key), score, toByteArray().convert(member));
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        HashMap<byte[], Double> binaryScoreMembers = new HashMap<byte[], Double>();

        for (Map.Entry<String, Double> entry : scoreMembers.entrySet()) {

            binaryScoreMembers.put(SafeEncoder.encode(entry.getKey()), entry.getValue());
        }
        return this.zadd(toByteArray().convert(key), binaryScoreMembers);
    }

    @Override
    public Set<String> zrange(String key, long start, long end) {
        Set<byte[]> result = this.zrange(toByteArray().convert(key), (int) start, (int) end);
        return new LinkedHashSet<String>(with(result).convert(toStringValue()));
    }

    @Override
    public Long zrem(String key, String... members) {
        byte[][] arrayOfFields = with(members).convert(toByteArray()).toArray(new byte[members.length][]);
        return this.zrem(toByteArray().convert(key), arrayOfFields);
    }

    @Override
    public Double zincrby(String key, double score, String member) {
        return this.zincrby(toByteArray().convert(key), score, toByteArray().convert(member));
    }

    @Override
    public Long zrank(String key, String member) {
        return this.zrank(toByteArray().convert(key), toByteArray().convert(member));
    }

    @Override
    public Long zrevrank(String key, String member) {
        return this.zrevrank(toByteArray().convert(key), toByteArray().convert(member));
    }

    @Override
    public Set<String> zrevrange(String key, long start, long end) {
        Set<byte[]> result = this.zrevrange(toByteArray().convert(key), (int) start, (int) end);
        return new LinkedHashSet<String>((with(result).convert(toStringValue())));
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        return this.zrangeWithScores(toByteArray().convert(key), (int) start, (int) end);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        return this.zrevrangeWithScores(toByteArray().convert(key), (int) start, (int) end);
    }

    @Override
    public Long zcard(String key) {
        return this.zcard(toByteArray().convert(key));
    }

    @Override
    public Double zscore(String key, String member) {
        return this.zscore(toByteArray().convert(key), toByteArray().convert(member));
    }

    @Override
    public List<String> sort(String key) {
        List<byte[]> result = this.sort(toByteArray().convert(key));
        return with(result).convert(toStringValue());
    }

    @Override
    public List<String> sort(String key, SortingParams sortingParameters) {
        List<byte[]> result = this.sort(toByteArray().convert(key), sortingParameters);
        return with(result).convert(toStringValue());
    }

    @Override
    public Long zcount(String key, double min, double max) {
        return this.zcount(toByteArray().convert(key), min, max);
    }

    @Override
    public Long zcount(String key, String min, String max) {
        return this.zcount(toByteArray().convert(key), toByteArray().convert(min), toByteArray().convert(max));
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        Set<byte[]> result = this.zrangeByScore(toByteArray().convert(key), min, max);
        return new LinkedHashSet<String>(with(result).convert(toStringValue()));
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) {
        Set<byte[]> result = this.zrangeByScore(toByteArray().convert(key), toByteArray().convert(min), toByteArray()
                .convert(max));
        return new LinkedHashSet<String>(with(result).convert(toStringValue()));
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        Set<byte[]> result = this.zrevrangeByScore(toByteArray().convert(key), max, min);
        return new LinkedHashSet<String>(with(result).convert(toStringValue()));
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        Set<byte[]> result = this.zrangeByScore(toByteArray().convert(key), min, max, offset, count);
        return new LinkedHashSet<String>(with(result).convert(toStringValue()));
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min) {
        Set<byte[]> result = this.zrevrangeByScore(toByteArray().convert(key), toByteArray().convert(max),
                toByteArray().convert(min));
        return new LinkedHashSet<String>(with(result).convert(toStringValue()));
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        Set<byte[]> result = this.zrangeByScore(toByteArray().convert(key), toByteArray().convert(min), toByteArray()
                .convert(max), offset, count);
        return new LinkedHashSet<String>(with(result).convert(toStringValue()));
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        Set<byte[]> result = this.zrevrangeByScore(toByteArray().convert(key), max, min, offset, count);
        return new LinkedHashSet<String>(with(result).convert(toStringValue()));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        return this.zrangeByScoreWithScores(toByteArray().convert(key), min, max);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        return this.zrevrangeByScoreWithScores(toByteArray().convert(key), max, min);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return this.zrangeByScoreWithScores(toByteArray().convert(key), min, max, offset, count);
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        Set<byte[]> result = this.zrevrangeByScore(toByteArray().convert(key), toByteArray().convert(max),
                toByteArray().convert(min), offset, count);
        return new LinkedHashSet<String>(with(result).convert(toStringValue()));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        return this.zrangeByScoreWithScores(toByteArray().convert(key), toByteArray().convert(min), toByteArray()
                .convert(max));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        return this.zrevrangeByScoreWithScores(toByteArray().convert(key), toByteArray().convert(max), toByteArray()
                .convert(min));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        return this.zrangeByScoreWithScores(toByteArray().convert(key), toByteArray().convert(min), toByteArray()
                .convert(max), offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return this.zrevrangeByScoreWithScores(toByteArray().convert(key), max, min, offset, count);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        return this.zrevrangeByScoreWithScores(toByteArray().convert(key), toByteArray().convert(max), toByteArray()
                .convert(min), offset, count);
    }

    @Override
    public Long zremrangeByRank(String key, long start, long end) {
        return this.zremrangeByRank(toByteArray().convert(key), (int) start, (int) end);
    }

    @Override
    public Long zremrangeByScore(String key, double start, double end) {
        return this.zremrangeByScore(toByteArray().convert(key), start, end);
    }

    @Override
    public Long zremrangeByScore(String key, String start, String end) {
        return this.zremrangeByScore(toByteArray().convert(key), toByteArray().convert(start),
                toByteArray().convert(end));
    }

    @Override
    public Long linsert(String key, LIST_POSITION where, String pivot, String value) {
        return this.linsert(toByteArray().convert(key), where, toByteArray().convert(pivot),
                toByteArray().convert(value));
    }

    @Override
    public String auth(final String password) {
        return this.connectionServerOperations.auth(password);
    }

    @Override
    public byte[] echo(final byte[] string) {
        return this.connectionServerOperations.echo(string);
    }

    @Override
    public String echo(final String string) {
        byte[] result = this.echo(toByteArray().convert(string));
        return toStringValue().convert(result);
    }

    @Override
    public String ping() {
        return this.connectionServerOperations.ping();
    }

    @Override
    public String quit() {
        return this.connectionServerOperations.quit();
    }

    @Override
    public String select(final int index) {
        return this.connectionServerOperations.select(index);
    }

    public Pipeline pipelined() {
        return this.transactionServerOperations.pipelined();
    }

    @Override
    public String bgrewriteaof() {
        return this.keysServerOperations.bgrewriteaof();
    }

    @Override
    public String save() {
        return this.keysServerOperations.save();
    }

    @Override
    public String bgsave() {
        return this.keysServerOperations.bgsave();
    }

    @Override
    public List<byte[]> configGet(final byte[] pattern) {
        return this.keysServerOperations.configGet(pattern);
    }

    @Override
    public List<String> configGet(final String pattern) {
        List<byte[]> result = this.configGet(toByteArray().convert(pattern));
        return with(result).convert(toStringValue());
    }

    @Override
    public byte[] configSet(final byte[] parameter, final byte[] value) {
        return this.keysServerOperations.configSet(parameter, value);
    }

    @Override
    public String configSet(final String parameter, final String value) {
        byte[] result = this.configSet(toByteArray().convert(parameter), toByteArray().convert(value));
        return toStringValue().convert(result);
    }

    @Override
    public String configResetStat() {
        return this.keysServerOperations.configResetStat();
    }

    @Override
    public String info() {
        return this.keysServerOperations.info();
    }

    @Override
    public Long lastsave() {
        return this.keysServerOperations.lastsave();
    }

    @Override
    public String shutdown() {
        return this.keysServerOperations.shutdown();
    }

    @Override
    public String slaveof(final String host, final int port) {
        return this.keysServerOperations.slaveof(host, port);
    }

    @Override
    public String slaveofNoOne() {
        return this.keysServerOperations.slaveofNoOne();
    }

    @Override
    public List<Slowlog> slowlogGet() {
        return this.keysServerOperations.slowlogGet();
    }

    @Override
    public List<Slowlog> slowlogGet(long entries) {
        return this.keysServerOperations.slowlogGet(entries);
    }

    @Override
    public String slowlogReset() {
        return this.keysServerOperations.slowlogReset();
    }

    @Override
    public Long slowlogLen() {
        return this.keysServerOperations.slowlogLen();
    }

    @Override
    public List<byte[]> slowlogGetBinary() {
        return this.keysServerOperations.slowlogGetBinary();
    }

    @Override
    public List<byte[]> slowlogGetBinary(long entries) {
        return this.keysServerOperations.slowlogGetBinary(entries);
    }

    @Override
    public Long getDB() {
        return this.keysServerOperations.getDB();
    }

    @Override
    public Long bitcount(byte[] key) {
        return this.stringDatatypeOperations.bitcount(key);
    }

    @Override
    public Long bitcount(byte[] key, long start, long end) {
        return this.stringDatatypeOperations.bitcount(key, start, end);
    }

    @Override
    public List<byte[]> blpop(byte[] key) {
        return this.listDatatypeOperations.blpop(0, key);
    }

    @Override
    public List<byte[]> brpop(byte[] key) {
        return this.listDatatypeOperations.brpop(0, key);
    }

    @Override
    public List<String> blpop(int timeout, String... keys) {
        byte[][] arrayOfKeys = with(keys).convert(toByteArray()).toArray(new byte[keys.length][]);
        List<byte[]> result = this.listDatatypeOperations.blpop(timeout, arrayOfKeys);
        return with(result).convert(toStringValue());
    }

    @Override
    public List<String> brpop(int timeout, String... keys) {
        byte[][] arrayOfKeys = with(keys).convert(toByteArray()).toArray(new byte[keys.length][]);
        List<byte[]> result = this.listDatatypeOperations.brpop(timeout, arrayOfKeys);
        return with(result).convert(toStringValue());
    }

    @Override
    public List<String> blpop(String... keys) {
        byte[][] arrayOfKeys = with(keys).convert(toByteArray()).toArray(new byte[keys.length][]);
        List<byte[]> result = this.listDatatypeOperations.blpop(0, arrayOfKeys);
        return with(result).convert(toStringValue());
    }

    @Override
    public List<String> brpop(String... keys) {
        byte[][] arrayOfKeys = with(keys).convert(toByteArray()).toArray(new byte[keys.length][]);
        List<byte[]> result = this.listDatatypeOperations.brpop(0, arrayOfKeys);
        return with(result).convert(toStringValue());
    }

    @Override
    public List<byte[]> blpop(int timeout, byte[]... keys) {
        return this.listDatatypeOperations.blpop(timeout, keys);
    }

    @Override
    public List<byte[]> brpop(int timeout, byte[]... keys) {
        return this.listDatatypeOperations.brpop(timeout, keys);
    }

    @Override
    public List<byte[]> blpop(byte[]... keys) {
        return this.listDatatypeOperations.blpop(0, keys);
    }

    @Override
    public List<byte[]> brpop(byte[]... keys) {
        return this.listDatatypeOperations.brpop(0, keys);
    }

    @Override
    public Long del(byte[] key) {
        return this.keysServerOperations.del(key);
    }

    @Override
    public Boolean setbit(byte[] key, long offset, boolean value) {
        return this.setbit(new String(key), offset, value);
    }

    @Override
    public Long strlen(byte[] key) {
        return this.stringDatatypeOperations.strlen(key);
    }

    @Override
    public Long bitcount(String key) {
        return this.bitcount(key.getBytes());
    }

    @Override
    public Long bitcount(String key, long start, long end) {
        return this.bitcount(key.getBytes(), start, end);
    }

    @Override
    public List<String> blpop(String key) {
        List<byte[]> result = this.blpop(key.getBytes());
        return new LinkedList<String>(with(result).convert(toStringValue()));
    }

    @Override
    public List<String> brpop(String key) {
        List<byte[]> result = this.brpop(key.getBytes());
        return new LinkedList<String>(with(result).convert(toStringValue()));
    }

    @Override
    public Long del(String key) {
        return this.del(key.getBytes());
    }

    @Override
    public Long lpushx(String key, String... values) {
        long lastSize = this.listDatatypeOperations.llen(key.getBytes());

        for (String value : values) {
            lastSize = this.lpushx(key.getBytes(), value.getBytes());
        }

        return lastSize;
    }

    @Override
    public Long rpushx(String key, String... values) {
        long lastSize = this.listDatatypeOperations.llen(key.getBytes());

        for (String value : values) {
            lastSize = this.rpushx(key, value);
        }

        return lastSize;
    }

    @Override
    public String rpoplpush(String srckey, String dstkey) {
        String result = this.rpop(srckey);
        if (result != null) {
            this.lpush(dstkey, result);
        }
        return result;
    }

    @Override
    public String brpoplpush(String source, String destination, int timeout) {
        List<String> result = this.brpop(timeout, source);
        if (result != null && result.size() > 0) {
            String value = result.get(1);
            this.lpush(destination, value);
            return value;
        }

        return null;
    }

    @Override
    public Boolean setbit(String key, long offset, String value) {
        return this.setbit(key.getBytes(), offset, value.getBytes());
    }

    @Override
    public Long strlen(String key) {
        return this.strlen(key.getBytes());
    }

    @Override
    public Long move(String key, int dbIndex) {
        return this.move(key.getBytes(), dbIndex);
    }

    private void checkValidTypeOrNone(byte[] key, String type) {
        String currentType = keysServerOperations.type(key);
        if (!(KeysServerOperations.NONE.equals(currentType) || currentType.equals(type))) {
            throw new IllegalArgumentException("ERR Operation against a key holding the wrong kind of value");
        }
    }

    private void updateAllTtlTimes() {
        this.keysServerOperations.updateTtl();
    }

    private void updateTtl(byte[] key) {
        this.keysServerOperations.updateTtl(key);
    }

    private Converter<ScoredByteBuffer, Tuple> toTuple() {
        return new Converter<SortsetDatatypeOperations.ScoredByteBuffer, Tuple>() {

            @Override
            public Tuple convert(ScoredByteBuffer from) {
                return new Tuple(from.getByteBuffer().array(), from.getScore());
            }
        };
    }

    private Converter<LIST_POSITION, ListPositionEnum> toListPosition() {
        return new Converter<BinaryClient.LIST_POSITION, ListDatatypeOperations.ListPositionEnum>() {

            @Override
            public ListPositionEnum convert(LIST_POSITION from) {
                switch (from) {
                case BEFORE:
                    return ListPositionEnum.BEFORE;
                case AFTER:
                    return ListPositionEnum.AFTER;
                default:
                    return ListPositionEnum.BEFORE;
                }
            }
        };
    }

    private Converter<Boolean, byte[]> toBooleanByteArray() {
        return new Converter<Boolean, byte[]>() {

            @Override
            public byte[] convert(Boolean from) {
                if (from) {
                    return "1".getBytes();
                } else {
                    return "0".getBytes();
                }
            }
        };
    }

    private Converter<Map<byte[], byte[]>, Map<String, String>> toMapString() {
        return new Converter<Map<byte[], byte[]>, Map<String, String>>() {

            @Override
            public Map<String, String> convert(Map<byte[], byte[]> from) {

                Map<String, String> map = new LinkedHashMap<String, String>(from.size());

                Set<byte[]> keySet = from.keySet();

                for (byte[] key : keySet) {
                    map.put(toStringValue().convert(key), toStringValue().convert(from.get(key)));
                }

                return map;
            }
        };
    }

    private Converter<Map<String, String>, Map<byte[], byte[]>> toMapByteArray() {
        return new Converter<Map<String, String>, Map<byte[], byte[]>>() {

            @Override
            public Map<byte[], byte[]> convert(Map<String, String> from) {

                Map<byte[], byte[]> map = new LinkedHashMap<byte[], byte[]>(from.size());

                Set<String> keySet = from.keySet();

                for (String key : keySet) {
                    map.put(toByteArray().convert(key), toByteArray().convert(from.get(key)));
                }

                return map;
            }
        };
    }

    private Converter<byte[], String> toStringValue() {
        return BYTE_ARRAY_TO_STRING_CONVERTER;
    }

    private Converter<String, byte[]> toByteArray() {
        return STRING_TO_BYTE_ARRAY_CONVERTER;
    }

    // //// BasicCommands /////////

    @Override
    public String info(String section) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String debug(DebugParams params) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long waitReplicas(int replicas, long timeout) {
        throw new UnsupportedOperationException();
    }

    // //// ScriptingCommands /////////

    @Override
    public Object eval(String script, int keyCount, String... params) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object eval(String script) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object evalsha(String script) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object evalsha(String sha1, List<String> keys, List<String> args) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object evalsha(String sha1, int keyCount, String... params) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Boolean scriptExists(String sha1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Boolean> scriptExists(String... sha1) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String scriptLoad(String script) {
        throw new UnsupportedOperationException();
    }

    // ////// MultiKeyBinaryCommands /////////

    @Override
    public String mset(byte[]... keysvalues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long msetnx(byte[]... keysvalues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<byte[]> sdiff(byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sdiffstore(byte[] dstkey, byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<byte[]> sinter(byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sinterstore(byte[] dstkey, byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long smove(byte[] srckey, byte[] dstkey, byte[] member) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sort(byte[] key, byte[] dstkey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<byte[]> sunion(byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sunionstore(byte[] dstkey, byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String watch(byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zinterstore(byte[] dstkey, byte[]... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zunionstore(byte[] dstkey, byte[]... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long publish(byte[] channel, byte[] message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subscribe(BinaryJedisPubSub jedisPubSub, byte[]... channels) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] randomBinaryKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
        throw new UnsupportedOperationException();
    }

    // /////// MultiKeyCommands ////////
    @Override
    public String mset(String... keysvalues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long msetnx(String... keysvalues) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> sdiff(String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sdiffstore(String dstkey, String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> sinter(String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sinterstore(String dstkey, String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long smove(String srckey, String dstkey, String member) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sort(String key, SortingParams sortingParameters, String dstkey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sort(String key, String dstkey) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> sunion(String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long sunionstore(String dstkey, String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String watch(String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String unwatch() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zinterstore(String dstkey, String... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zinterstore(String dstkey, ZParams params, String... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zunionstore(String dstkey, String... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zunionstore(String dstkey, ZParams params, String... sets) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long publish(String channel, String message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subscribe(JedisPubSub jedisPubSub, String... channels) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String randomKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long bitop(BitOP op, String destKey, String... srcKeys) {
        throw new UnsupportedOperationException();
    }

    // //// JedisCommands //////
    @Override
    public ScanResult<String> scan(int cursor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScanResult<String> scan(String cursor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScanResult<Entry<String, String>> hscan(String key, int cursor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScanResult<String> sscan(String key, int cursor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScanResult<Tuple> zscan(String key, int cursor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScanResult<Entry<String, String>> hscan(String key, String cursor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScanResult<String> sscan(String key, String cursor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScanResult<Tuple> zscan(String key, String cursor) {
        throw new UnsupportedOperationException();
    }

    // /// AdvancedBinaryJedisCommands ///

    @Override
    public Long objectRefcount(byte[] key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] objectEncoding(byte[] key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long objectIdletime(byte[] key) {
        throw new UnsupportedOperationException();
    }

    // /// AdvancedJedisCommands /////

    @Override
    public Long objectRefcount(String string) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String objectEncoding(String string) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long objectIdletime(String string) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String pfmerge(byte[] destkey, byte[]... sourcekeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long pfcount(byte[]... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String pfmerge(String destkey, String... sourcekeys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long pfcount(String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Double incrByFloat(byte[] key, double value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Double hincrByFloat(byte[] key, byte[] field, double value) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<byte[]> srandmember(byte[] key, int count) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long zlexcount(byte[] key, byte[] min, byte[] max) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max, int offset, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zremrangeByLex(byte[] key, byte[] min, byte[] max) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long pfadd(byte[] key, byte[]... elements) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long pfcount(byte[] key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String set(String key, String value, String nxxx, String expx, long time) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> srandmember(String key, int count) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Long zlexcount(String key, String min, String max) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long zremrangeByLex(String key, String min, String max) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> blpop(int timeout, String key) {
        return blpop(timeout, new String[] { key });
    }

    @Override
    public List<String> brpop(int timeout, String key) {
        return brpop(timeout, new String[] { key });
    }

    @Override
    public Long pfadd(String key, String... elements) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long pfcount(String key) {
        throw new UnsupportedOperationException();
    }
}
