/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.jigsaw.redis.JedisTemplate.JedisAction;
import com.ericsson.jigsaw.redis.pool.JedisPool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

/**
 * Load and Run the lua scripts and support to reload the script when execution failed.
 */
public class JedisScriptExecutor {
    private static Logger logger = LoggerFactory.getLogger(JedisScriptExecutor.class);

    private JedisTemplate jedisTemplate;

    //Map contains <Script SHA, Script Content> pair
    private Map<String, String> scriptMaps = new HashMap<String, String>();

    public JedisScriptExecutor(JedisPool jedisPool) {
        this.jedisTemplate = new JedisTemplate(jedisPool);
    }

    public JedisScriptExecutor(JedisTemplate jedisTemplate) {
        this.jedisTemplate = jedisTemplate;
    }

    /**
     * Load the script to redis, return the script SHA.
     */
    public synchronized String load(final String script) {
        //check the script exist
        for (Map.Entry<String, String> entry : scriptMaps.entrySet()) {
            if (script.equals(entry.getValue())) {
                return entry.getKey();
            }
        }

        String sha = jedisTemplate.execute(new JedisAction<String>() {//NOSONAR
            @Override
            public String action(Jedis jedis) {
                return jedis.scriptLoad(script);
            }
        });

        scriptMaps.put(sha, script);
        return sha;
    }

    /**
     * Execute the script, auto reload the script if it is not in redis.
     */
    public Object execute(final String sha, final String[] keys, final String[] args) {
        return execute(sha, Arrays.asList(keys), Arrays.asList(args));
    }

    /**
     * Execute the script, auto reload the script if it is not in redis.
     */
    public Object execute(final String sha, final List<String> keys, final List<String> args) {
        if (!scriptMaps.containsKey(sha)) {
            throw new IllegalArgumentException("Script SHA " + sha + " is not loaded in executor");
        }

        try {
            return jedisTemplate.execute(new JedisAction<Object>() {//NOSONAR
                @Override
                public Object action(Jedis jedis) {
                    return jedis.evalsha(sha, keys, args);
                }
            });
        } catch (JedisDataException e) {
            logger.warn(jedisTemplate.getJedisPool().getFormattedPoolName() + "Lua executation error, try to reload the script.", e);
            return reloadAndExecute(sha, keys, args);
        } catch (JigsawJedisException e) {
            if (e.isJedisDataException()) {
                logger.warn(jedisTemplate.getJedisPool().getFormattedPoolName()
                        + "Lua executation error, try to reload the script.", e);
                return reloadAndExecute(sha, keys, args);
            }
            throw e;
        }
    }

    /**
     * Reload the script and execute it again.
     */
    private Object reloadAndExecute(final String sha, final List<String> keys, final List<String> args) {

        return jedisTemplate.execute(new JedisAction<Object>() {
            @Override
            public Object action(Jedis jedis) {
                // concurrent checking again
                if (!jedis.scriptExists(sha)) {
                    String script = scriptMaps.get(sha);
                    jedis.scriptLoad(script);
                }

                return jedis.evalsha(sha, keys, args);
            }
        });
    }

    //just for test.
    public Map<String, String> getLuaMaps() {
        return scriptMaps;
    }
}
