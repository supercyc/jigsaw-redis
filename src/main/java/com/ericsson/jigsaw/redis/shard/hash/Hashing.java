/*
 * ----------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 * ----------------------------------------------------------------------
 */

package com.ericsson.jigsaw.redis.shard.hash;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public interface Hashing {
  public static final Hashing MURMUR_HASH = new MurmurHash();
  public ThreadLocal<MessageDigest> md5Holder = new ThreadLocal<MessageDigest>();

  public static final redis.clients.util.Hashing MD5 = new redis.clients.util.Hashing() {
    public long hash(String key) {
      return hash(SafeEncoder.encode(key));
    }

    public long hash(byte[] key) {
      try {
        if (md5Holder.get() == null) {
          md5Holder.set(MessageDigest.getInstance("MD5"));
        }
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException("++++ no md5 algorythm found");
      }
      MessageDigest md5 = md5Holder.get();

      md5.reset();
      md5.update(key);
      byte[] bKey = md5.digest();
      long res = ((long) (bKey[3] & 0xFF) << 24) | ((long) (bKey[2] & 0xFF) << 16)
          | ((long) (bKey[1] & 0xFF) << 8) | (long) (bKey[0] & 0xFF);
      return res;
    }
  };

  public long hash(String key);

  public long hash(byte[] key);
}