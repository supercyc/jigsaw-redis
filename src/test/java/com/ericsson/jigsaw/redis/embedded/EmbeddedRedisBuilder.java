package com.ericsson.jigsaw.redis.embedded;

import redis.clients.jedis.Jedis;

public class EmbeddedRedisBuilder {

	public Jedis createEmbeddedJedis() {
		return RedirectProxy.createProxy(NoArgsJedis.class, new EmbeddedJedis());
	}
	
}
