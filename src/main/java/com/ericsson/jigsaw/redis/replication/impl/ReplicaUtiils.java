/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.replication.impl;

import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.ericsson.jigsaw.redis.JedisTemplate;
import com.ericsson.jigsaw.redis.JedisTemplate.JedisAction;
import com.ericsson.jigsaw.redis.JedisTemplate.PipelineActionNoResult;
import com.ericsson.jigsaw.redis.replication.RedisOp;
import com.ericsson.jigsaw.redis.replication.ReplicationConstants;

public class ReplicaUtiils {

    private static Logger logger = LoggerFactory.getLogger(ReplicaUtiils.class);

    private static Map<String, Class<?>[]> pipelineParametersCache = new ConcurrentHashMap<String, Class<?>[]>();
    private static Map<String, Method> pipelineMethodCache = new ConcurrentHashMap<String, Method>();

    private static Object paramLock = new Object();
    private static Object methodLock = new Object();

    public static boolean isSyncedUp(final JedisTemplate template, HostAndPort address) {
        try {
            checkHealthy(template);
        } catch (Exception e) {
            logger.warn("check healthy fail: " + e.getMessage());
            logger.debug("details:", e);
            return false;
        }

        final Properties infoProperties = getInfoProperties(template);

        final boolean isMaster = "master".equals(infoProperties.getProperty("role"));
        if (isMaster) {
            final HostAndPort masterAddress = template.getJedisPool().getAddress();
            logger.info("[{}] is a master, check the slave[{}] syncup status.", masterAddress, address);
            final long masterOffset = Long.valueOf(infoProperties.getProperty("master_repl_offset"));
            final List<String> slaves = getSlaves(infoProperties);
            for (String slave : slaves) {
                final String[] split = slave.split(",");
                final String host = split[0].split("=")[1];
                final Integer port = Integer.valueOf(split[1].split("=")[1]);
                if (address.getHost().equals(host) && (address.getPort() == port)) {
                    final String state = split[2].split("=")[1];
                    final long slaveOffset = Long.valueOf(split[3].split("=")[1]);
                    logger.info("slave[{}] state is {}", address, state);
                    logger.info("master[{}] offset={}, slave[{}] offset={}, gap={}", masterAddress, masterOffset,
                            address, slaveOffset, masterOffset - slaveOffset);
                    final boolean online = "online".equals(state);
                    final boolean aligned = masterOffset == slaveOffset;
                    return online && aligned;
                }
            }
            logger.info("[{}] is not a slave of [{}] yet", address, masterAddress);
            return false;
        }

        final String masterStatus = infoProperties.getProperty("master_link_status");
        logger.info("master status: " + masterStatus);
        final boolean masterUp = "up".equals(masterStatus);

        final String progress = infoProperties.getProperty("master_sync_in_progress");
        final boolean syncDone = "0".equals(progress);
        if (masterUp) {
            if (syncDone) {
                logger.info("full sync up is done");
            } else {
                logger.info("sync up is in progress, left: " + infoProperties.getProperty("master_sync_left_bytes")
                        + " byte");
            }
        }
        final boolean syncedUp = masterUp && syncDone;
        return syncedUp;
    }

    public static Properties getInfoProperties(final JedisTemplate template) {
        final String info = template.execute(new JedisAction<String>() {

            @Override
            public String action(Jedis jedis) {
                return jedis.info("Replication");
            }
        });

        final Properties infoProperties = new Properties();
        try {
            infoProperties.load(new StringReader(info));
        } catch (IOException e) {
            logger.error("loading redis info got error", e);
        }
        return infoProperties;
    }

    private static List<String> getSlaves(Properties properties) {
        final List<String> slaves = new ArrayList<String>();
        for (String key : properties.stringPropertyNames()) {
            if ((key != null) && key.startsWith("slave")) {
                slaves.add(properties.getProperty(key));
            }
        }
        return slaves;
    }

    public static void checkHealthy(final JedisTemplate template) {
        final String status = template.ping();
        if (!"PONG".equals(status)) {
            throw new JedisConnectionException("cannot get PONG by PING from redis");
        }
    }

    public static void replay(JedisTemplate template, final List<RedisOp> operations) {

        final List<Response<?>> pinelineResp = new ArrayList<Response<?>>();

        template.execute(new PipelineActionNoResult() {

            @Override
            public void action(Pipeline pipeline) {
                for (RedisOp operation : operations) {
                    try {
                        final String command = operation.getCmd();
                        final Object[] args = operation.getArgs().toArray();
                        final List<String> parameterTypes = operation.getParamTypes();

                        Class<?>[] typeClassList = forNames(parameterTypes);
                        normalize(typeClassList, args);
                        Method method = getMethod(command, typeClassList);
                        method.setAccessible(true);
                        pinelineResp.add((Response<?>) method.invoke(pipeline, args));
                    } catch (Exception e) {
                        logger.error("replay invoke error, skip it: {}", operation);
                        logger.debug("details:", e);
                        continue;
                    }
                }
            }
        });
        for (Response<?> response : pinelineResp) {
            response.get();
        }
    }

    public static boolean isMaster(JedisTemplate template) {
        final Properties infoProperties = getInfoProperties(template);
        return "master".equals(infoProperties.getProperty("role"));
    }

    public static boolean isMasterOf(JedisTemplate template, HostAndPort address) {
        final HostAndPort masterAddress = template.getJedisPool().getAddress();
        final Properties infoProperties = getInfoProperties(template);
        final List<String> slaves = getSlaves(infoProperties);
        for (String slave : slaves) {
            logger.info("[{}] is master of [{}]", masterAddress, slave);
            final String[] split = slave.split(",");
            final String host = split[0].split("=")[1];
            final Integer port = Integer.valueOf(split[1].split("=")[1]);
            if (address.getHost().equals(host) && (address.getPort() == port)) {
                return true;
            }
        }
        return false;
    }

    private static Method getMethod(String methodName, Class<?>[] typeClassList) throws SecurityException,
            NoSuchMethodException {
        final String methodKey = getMethodKey(methodName, typeClassList);

        Method method = pipelineMethodCache.get(methodKey);
        if (method == null) {
            synchronized (methodLock) {
                method = pipelineMethodCache.get(methodKey);
                if (method == null) {
                    method = Pipeline.class.getMethod(methodName, typeClassList);
                    pipelineMethodCache.put(methodKey, method);
                }
            }
        }
        return method;
    }

    private static String getMethodKey(String methodName, Class<?>[] typeClassList) {
        return methodName + ":" + getParametersKey(typeClassList);
    }

    private static String getParametersKey(Class<?>[] typeClassList) {
        StringBuilder keyBuilder = new StringBuilder();
        for (Class<?> type : typeClassList) {
            keyBuilder.append(type.getName());
        }
        return keyBuilder.toString();
    }

    private static Class<?>[] forNames(List<String> parameterTypes) {
        final String parametersKey = getParametersKey(parameterTypes);
        Class<?>[] typeClassList = pipelineParametersCache.get(parametersKey);
        if (typeClassList == null) {
            synchronized (paramLock) {
                typeClassList = pipelineParametersCache.get(parametersKey);
                if (typeClassList == null) {
                    typeClassList = new Class<?>[parameterTypes.size()];

                    for (int i = 0; i < parameterTypes.size(); i++) {
                        try {
                            typeClassList[i] = Class.forName(parameterTypes.get(i));
                        } catch (ClassNotFoundException e) {
                            typeClassList[i] = ReplicationConstants.PRIMITIVE_CLASS_MAP.get(parameterTypes.get(i));
                        }
                    }
                    pipelineParametersCache.put(parametersKey, typeClassList);
                }
            }
        }
        return typeClassList;
    }

    private static String getParametersKey(List<String> parameterTypes) {
        StringBuilder keyBuilder = new StringBuilder();
        for (String type : parameterTypes) {
            keyBuilder.append(type);
        }
        return keyBuilder.toString();
    }

    private static void normalize(Class<?>[] typeClassList, Object[] args) {
        for (int i = 0; i < typeClassList.length; i++) {
            if (typeClassList[i].isArray()) {
                List<?> argList = (List<?>) args[i];
                final Object argArray = Array.newInstance(typeClassList[i].getComponentType(), argList.size());
                for (int j = 0; j < argList.size(); j++) {
                    Array.set(argArray, j, argList.get(j));
                }
                args[i] = argArray;
            }
        }
    }
}
