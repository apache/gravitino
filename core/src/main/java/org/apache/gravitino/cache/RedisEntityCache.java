/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.metrics.MetricsSystem;
import org.apache.gravitino.metrics.source.RedisEntityCacheMetricsSource;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.ConnectionPool;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisNoScriptException;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

/**
 * An {@link EntityCache} that keeps one copy of every cached entity in Redis, shared by all nodes
 * of a Gravitino cluster. It is selected with {@code gravitino.cache.implementation=redis} and
 * reports {@link Coherence#SHARED}.
 *
 * <p>This is a shared cache, not a strongly consistent database and cache pair. The cache is only
 * touched after the entity store has committed, and an invalidation never updates a value in place,
 * so most Redis failures degrade to a cache miss. The remaining ways a node can serve a stale copy
 * are listed in the server configuration docs: an invalidation that does not reach Redis, a node
 * that dies between the commit and the invalidation, and a Redis failover that loses an
 * acknowledged invalidation. Each is bounded by the next change-log replay of that change, which
 * every node runs for a shared cache too (see {@code RelationalEntityStore}), and at the latest by
 * the value TTL. Callers that cannot tolerate that must read the store directly.
 *
 * <p><b>Keyspace.</b> See {@link RedisKeyspace}. All keys of one metalake share a Redis Cluster
 * hash slot, so every script here is single-slot and declares every key it touches in {@code KEYS};
 * the cost is that one metalake's entries are bounded by one cluster node.
 *
 * <p><b>Lazy invalidation.</b> Every value carries the stamp it was filled under: the fence
 * generations of its identifier and of every ancestor. A read serves the value only if the stamp
 * still matches the current fences, and deletes it otherwise. {@link #invalidate(NameIdentifier,
 * Entity.EntityType)} therefore only moves one fence to a fresh generation and deletes the one
 * value, in constant time whatever the size of the subtree below it; every descendant stops being
 * served at once and is reclaimed by its next read or by its TTL.
 *
 * <p><b>Stale-fill guard.</b> A read miss records, for the calling thread, the stamp it observed.
 * The write that fills the entry afterwards compares the stamp again inside its script and refuses
 * to write if any fence moved, so a load that began before an invalidation of the entity, or of any
 * container above it, cannot refill the key once the invalidation committed. The guard fails
 * closed: a write with no recorded stamp, whether because no miss preceded it on this thread or
 * because the miss failed to reach Redis, is not written at all. A record left by an earlier miss
 * of the same key on the same thread still guards the write, so it is never unsafe.
 *
 * <p><b>Fence lifetime.</b> Fences and the per-metalake generation counter carry no TTL, so no
 * {@code volatile-*} eviction policy can remove them and a generation is never reused. An {@code
 * allkeys-*} policy could evict either, so the cache refuses to start when Redis reports one. A
 * metalake's fences are deleted only when the metalake itself is invalidated, which moves the
 * metalake fence that every stamp under it includes.
 *
 * <p><b>Failure policy.</b> Reads and fills are optimizations: a Redis error or timeout makes them
 * a miss or a no-op. A failed invalidation is retried once, then logged and counted, and the call
 * returns normally: the store has already committed, and failing the caller would report a
 * successful write as failed. An unreachable Redis at startup fails fast. {@link #close()} releases
 * only this node's client; the shared data is left for the other nodes.
 *
 * <p><b>Maintenance.</b> {@link #clear()} and {@link #size()} scan the keyspace on every primary.
 * They have no production caller for a shared cache and are meant for operators and tests; {@link
 * #clear()} moves the fence of every metalake it finds, which rejects the fills in flight there,
 * but gives no such guarantee for a metalake with nothing cached when the scan passes it.
 */
public class RedisEntityCache extends BaseEntityCache {

  private static final Logger LOG = LoggerFactory.getLogger(RedisEntityCache.class);

  /** Per-thread bound on recorded stamps awaiting the write that consumes them. */
  @VisibleForTesting static final int MAX_PENDING_STAMPS = 1024;

  private static final long FAILURE_LOG_INTERVAL_MS = 30_000L;
  private static final int SCAN_BATCH = 1000;
  private static final String MAXMEMORY_POLICY = "maxmemory-policy";

  /**
   * Reads a value. {@code KEYS[1]} is the value key and {@code KEYS[2]} the metalake's fences;
   * {@code ARGV} lists the fence paths of the stamp. Returns {@code {1, value}} on a hit, {@code
   * {0, stamp}} on a miss, and {@code {2, stamp}} when a value was found under an outdated stamp
   * and deleted. An absent fence reads as {@code "0"}.
   */
  @VisibleForTesting
  static final String READ_SCRIPT =
      "local f = redis.call('HMGET', KEYS[2], unpack(ARGV))\n"
          + "for i = 1, #ARGV do f[i] = f[i] or '0' end\n"
          + "local stamp = table.concat(f, ',')\n"
          + "local e = redis.call('HMGET', KEYS[1], 'v', 's')\n"
          + "if e[1] then\n"
          + "  if e[2] == stamp then return {1, e[1]} end\n"
          + "  redis.call('DEL', KEYS[1])\n"
          + "  return {2, stamp}\n"
          + "end\n"
          + "return {0, stamp}\n";

  /**
   * Reads several values of one metalake without recording anything. {@code KEYS[1]} is the
   * metalake's fences, followed by the value keys; {@code ARGV} holds, for each value key in order,
   * the number of its fence paths followed by the paths. Returns one element per value key: the
   * value, or nil when absent or outdated. An outdated value is deleted.
   */
  @VisibleForTesting
  static final String PEEK_SCRIPT =
      "local r = {}\n"
          + "local a = 1\n"
          + "for k = 2, #KEYS do\n"
          + "  local c = tonumber(ARGV[a])\n"
          + "  local f = redis.call('HMGET', KEYS[1], unpack(ARGV, a + 1, a + c))\n"
          + "  for i = 1, c do f[i] = f[i] or '0' end\n"
          + "  a = a + c + 1\n"
          + "  local e = redis.call('HMGET', KEYS[k], 'v', 's')\n"
          + "  if e[1] and e[2] == table.concat(f, ',') then\n"
          + "    r[#r + 1] = e[1]\n"
          + "  else\n"
          + "    if e[1] then redis.call('DEL', KEYS[k]) end\n"
          + "    r[#r + 1] = false\n"
          + "  end\n"
          + "end\n"
          + "return r\n";

  /**
   * Writes a value unless its stamp changed. {@code KEYS[1]} is the value key and {@code KEYS[2]}
   * the metalake's fences; {@code ARGV[1]} is the value, {@code ARGV[2]} the TTL in milliseconds,
   * {@code ARGV[3]} the stamp observed by the miss, followed by the fence paths of the stamp.
   * Returns 1 if written and 0 if rejected.
   */
  @VisibleForTesting
  static final String PUT_SCRIPT =
      "local f = redis.call('HMGET', KEYS[2], unpack(ARGV, 4, #ARGV))\n"
          + "for i = 1, #ARGV - 3 do f[i] = f[i] or '0' end\n"
          + "if table.concat(f, ',') ~= ARGV[3] then return 0 end\n"
          + "redis.call('DEL', KEYS[1])\n"
          + "redis.call('HSET', KEYS[1], 'v', ARGV[1], 's', ARGV[3])\n"
          + "redis.call('PEXPIRE', KEYS[1], ARGV[2])\n"
          + "return 1\n";

  /**
   * Invalidates an identifier and, lazily, everything below it. {@code KEYS[1]} is the metalake's
   * generation counter, {@code KEYS[2]} its fences and {@code KEYS[3]} the value key; {@code
   * ARGV[1]} is the fence path of the identifier and {@code ARGV[2]} is {@code "1"} when the
   * identifier is the metalake itself, whose fences are then discarded: the metalake fence moves
   * with them, so every stamp under the metalake is outdated anyway. Runs in constant time. Returns
   * the number of values deleted.
   */
  @VisibleForTesting
  static final String DROP_SCRIPT =
      "local g = redis.call('INCR', KEYS[1])\n"
          + "if ARGV[2] == '1' then redis.call('UNLINK', KEYS[2]) end\n"
          + "redis.call('HSET', KEYS[2], ARGV[1], g)\n"
          + "return redis.call('DEL', KEYS[3])\n";

  /**
   * Discards a value only if it still holds the bytes the caller read. {@code KEYS[1]} is the value
   * key; {@code ARGV[1]} the bytes observed. A concurrent fill that replaced the value is left
   * intact. Returns 1 if discarded and 0 otherwise.
   */
  @VisibleForTesting
  static final String DISCARD_SCRIPT =
      "if redis.call('HGET', KEYS[1], 'v') == ARGV[1] then\n"
          + "  return redis.call('DEL', KEYS[1])\n"
          + "end\n"
          + "return 0\n";

  private static final Script READ = new Script(READ_SCRIPT);
  private static final Script PEEK = new Script(PEEK_SCRIPT);
  private static final Script PUT = new Script(PUT_SCRIPT);
  private static final Script DROP = new Script(DROP_SCRIPT);
  private static final Script DISCARD = new Script(DISCARD_SCRIPT);

  private final UnifiedJedis jedis;
  private final RedisKeyspace keyspace;
  private final KryoEntitySerializer serializer;
  private final SegmentedLock segmentedLock;
  private final long valueTtlMs;
  private final RedisEntityCacheMetricsSource metrics = new RedisEntityCacheMetricsSource();
  private final AtomicLong lastFailureLogMs = new AtomicLong();
  private volatile boolean metricsRegistered;

  /**
   * Set by {@link #close()}. A cluster client reconnects on demand after it was closed, so the
   * client alone cannot tell a closed cache from an open one; every operation checks this first.
   */
  private volatile boolean closed;

  /**
   * Stamps observed by a read miss on this thread, keyed by value key, consumed by the write that
   * fills the entry. Bounded and access-ordered so an entry whose write never happens (the store
   * had no such entity) is eventually evicted; a write whose record was evicted is not performed,
   * never performed unguarded.
   */
  private final ThreadLocal<Map<String, StampRecord>> pendingStamps =
      ThreadLocal.withInitial(
          () ->
              new LinkedHashMap<String, StampRecord>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, StampRecord> eldest) {
                  return size() > MAX_PENDING_STAMPS;
                }
              });

  /**
   * Constructs a new {@link RedisEntityCache} connected to the Redis deployment described by the
   * {@code gravitino.cache.redis.*} configuration.
   *
   * @param cacheConfig the cache configuration
   */
  public RedisEntityCache(Config cacheConfig) {
    this(cacheConfig, createClient(cacheConfig));
    probe(cacheConfig.get(Configs.CACHE_REDIS_ADDRESS));
    MetricsSystem metricsSystem = GravitinoEnv.getInstance().metricsSystem();
    if (metricsSystem != null) {
      metricsSystem.register(metrics);
      metricsRegistered = true;
    }
  }

  /**
   * Constructs a new {@link RedisEntityCache} over an existing client, without probing it.
   *
   * @param cacheConfig the cache configuration
   * @param jedis the Redis client, standalone or cluster
   */
  @VisibleForTesting
  RedisEntityCache(Config cacheConfig, UnifiedJedis jedis) {
    super(cacheConfig);
    Preconditions.checkArgument(jedis != null, "jedis must not be null");
    this.jedis = jedis;
    this.keyspace = new RedisKeyspace(cacheConfig.get(Configs.CACHE_REDIS_NAMESPACE));
    // Only the Kryo serializer exists; the config entry validates the name.
    cacheConfig.get(Configs.CACHE_REDIS_SERIALIZER);
    this.serializer = new KryoEntitySerializer();
    this.segmentedLock = new SegmentedLock(cacheConfig.get(Configs.CACHE_LOCK_SEGMENTS));
    this.valueTtlMs = cacheConfig.get(Configs.CACHE_EXPIRATION_TIME);
    // An outdated value is reclaimed by its next read or by its TTL. Without a TTL, a value nobody
    // reads again after an ancestor was invalidated would stay in Redis forever.
    Preconditions.checkArgument(
        valueTtlMs > 0,
        "%s must be positive when %s is 'redis'",
        Configs.CACHE_EXPIRATION_TIME.getKey(),
        Configs.CACHE_IMPLEMENTATION.getKey());
  }

  /** {@inheritDoc} */
  @Override
  public Coherence coherence() {
    return Coherence.SHARED;
  }

  /**
   * {@inheritDoc}
   *
   * <p>On a miss, records the observed stamp for the calling thread, so that the write which
   * follows can be rejected if an invalidation commits in between. A Redis failure is a miss that
   * records nothing, so the write which follows it is not performed.
   */
  @Override
  public <E extends Entity & HasIdentifier> Optional<E> getIfPresent(
      NameIdentifier ident, Entity.EntityType type) {
    checkArguments(ident, type);
    if (!isCacheable(type)) {
      return Optional.empty();
    }
    EntityCacheKey key = EntityCacheKey.of(ident, type);
    String valueKey = keyspace.valueKey(key);
    List<byte[]> paths = fencePathArgs(ident);

    List<?> reply;
    try {
      reply =
          (List<?>)
              run(READ, ImmutableList.of(utf8(valueKey), utf8(keyspace.fencesKey(ident))), paths);
    } catch (JedisException e) {
      // A record left by an earlier miss must not vouch for a load this read did not bound.
      pendingStamps.get().remove(valueKey);
      logFailure("read", key, e);
      return Optional.empty();
    }

    long status = (Long) reply.get(0);
    if (status == 1L) {
      byte[] bytes = (byte[]) reply.get(1);
      try {
        E entity = convertEntity(serializer.deserialize(bytes));
        metrics.hit();
        return Optional.of(entity);
      } catch (RuntimeException e) {
        LOG.warn("Discarding cache entry {} that could not be deserialized", key, e);
        pendingStamps.get().remove(valueKey);
        discardQuietly(key, bytes);
        metrics.miss();
        return Optional.empty();
      }
    }

    if (status == 2L) {
      metrics.staleRead();
    }
    metrics.miss();
    pendingStamps.get().put(valueKey, new StampRecord(paths, (byte[]) reply.get(1)));
    return Optional.empty();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Keys of one metalake are read in a single script, so resolving the ancestors of an
   * identifier costs one round trip. Nothing is recorded for a later write, and a Redis failure
   * reports every key as absent.
   */
  @Override
  public List<Optional<HasIdentifier>> peekAll(List<EntityCacheKey> keys) {
    Preconditions.checkArgument(keys != null, "keys cannot be null");
    checkOpen();
    List<Optional<HasIdentifier>> result = Lists.newArrayListWithCapacity(keys.size());
    if (keys.isEmpty()) {
      return result;
    }

    String metalake = RedisKeyspace.hashTag(keys.get(0).identifier());
    for (EntityCacheKey key : keys) {
      if (!metalake.equals(RedisKeyspace.hashTag(key.identifier()))) {
        // Keys of different metalakes live in different slots; read each on its own.
        for (EntityCacheKey single : keys) {
          result.add(peekAll(ImmutableList.of(single)).get(0));
        }
        return result;
      }
    }

    List<byte[]> scriptKeys = Lists.newArrayList(utf8(keyspace.fencesKey(metalake)));
    List<byte[]> args = Lists.newArrayList();
    List<Integer> positions = Lists.newArrayList();
    for (int i = 0; i < keys.size(); i++) {
      EntityCacheKey key = keys.get(i);
      result.add(Optional.empty());
      if (!isCacheable(key.entityType())) {
        continue;
      }
      List<byte[]> paths = fencePathArgs(key.identifier());
      scriptKeys.add(utf8(keyspace.valueKey(key)));
      args.add(utf8(Integer.toString(paths.size())));
      args.addAll(paths);
      positions.add(i);
    }
    if (positions.isEmpty()) {
      return result;
    }

    List<?> reply;
    try {
      reply = (List<?>) run(PEEK, scriptKeys, args);
    } catch (JedisException e) {
      logFailure("peek", keys.get(0), e);
      return result;
    }

    for (int j = 0; j < positions.size(); j++) {
      int i = positions.get(j);
      byte[] bytes = (byte[]) reply.get(j);
      if (bytes == null) {
        metrics.miss();
        continue;
      }
      try {
        result.set(i, Optional.of((HasIdentifier) convertEntity(serializer.deserialize(bytes))));
        metrics.hit();
      } catch (RuntimeException e) {
        LOG.warn("Discarding cache entry {} that could not be deserialized", keys.get(i), e);
        discardQuietly(keys.get(i), bytes);
        metrics.miss();
      }
    }
    return result;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Moves the fence of the identifier to a fresh generation and deletes its value, in constant
   * time; descendants stop being served because their stamps include that fence. A Redis failure is
   * retried once, then logged and counted, and this returns {@code false} instead of throwing: the
   * store has already committed, and the change-log replay on every node repeats the invalidation.
   */
  @Override
  public boolean invalidate(NameIdentifier ident, Entity.EntityType type) {
    checkArguments(ident, type);
    EntityCacheKey key = EntityCacheKey.of(ident, type);
    List<byte[]> paths = fencePathArgs(ident);
    List<byte[]> keys =
        ImmutableList.of(
            utf8(keyspace.generationKey(ident)),
            utf8(keyspace.fencesKey(ident)),
            utf8(keyspace.valueKey(key)));
    List<byte[]> args =
        ImmutableList.of(paths.get(paths.size() - 1), utf8(ident.hasNamespace() ? "0" : "1"));

    return segmentedLock.withLock(
        key,
        () -> {
          JedisException failure = null;
          for (int attempt = 0; attempt < 2; attempt++) {
            try {
              run(DROP, keys, args);
              return true;
            } catch (JedisException e) {
              failure = e;
            }
          }
          metrics.invalidationFailed();
          LOG.error(
              "Failed to invalidate entity cache entry {} in Redis; other nodes may serve the "
                  + "previous copy until the change log replays this change or it expires",
              key,
              failure);
          return false;
        });
  }

  /** {@inheritDoc} Checks the stamp like a read does; a Redis failure is reported as absent. */
  @Override
  public boolean contains(NameIdentifier ident, Entity.EntityType type) {
    checkArguments(ident, type);
    return peekAll(ImmutableList.of(EntityCacheKey.of(ident, type))).get(0).isPresent();
  }

  /**
   * {@inheritDoc}
   *
   * <p>A maintenance operation: counts the value keys of this namespace on every primary. Values
   * that are outdated but not yet reclaimed are counted too.
   */
  @Override
  public long size() {
    checkOpen();
    AtomicLong total = new AtomicLong();
    forEachPrimary(
        node ->
            scan(
                node,
                keyspace.allValueKeysPattern(),
                valueKey -> {
                  if (keyspace.metalakeOfValueKey(valueKey) != null) {
                    total.incrementAndGet();
                  }
                }));
    return total.get();
  }

  /**
   * {@inheritDoc}
   *
   * <p>A maintenance operation: scans every primary, and for each metalake it finds moves the
   * metalake fence, which outdates every value and rejects every fill in flight there, then deletes
   * the values it finds. A metalake with nothing cached when the scan passes it is not fenced, so a
   * fill in flight there may still land. A Redis failure is propagated.
   */
  @Override
  public void clear() {
    checkOpen();
    segmentedLock.withGlobalLock(
        () -> {
          pendingStamps.get().clear();
          Set<String> fenced = new HashSet<>();
          forEachPrimary(
              node ->
                  scan(
                      node,
                      keyspace.allValueKeysPattern(),
                      valueKey -> {
                        String metalake = keyspace.metalakeOfValueKey(valueKey);
                        if (metalake == null) {
                          return;
                        }
                        if (fenced.add(metalake)) {
                          dropMetalake(metalake);
                        }
                        jedis.unlink(valueKey);
                      }));
        });
  }

  /**
   * {@inheritDoc}
   *
   * <p>Guarded by the stamp recorded by the miss that preceded it on this thread. Fails closed:
   * with no record nothing is written and the next read loads the entity under a fresh record. A
   * Redis failure discards the write.
   */
  @Override
  protected <E extends Entity & HasIdentifier> void doPut(E entity) {
    checkOpen();
    NameIdentifier ident = getIdentFromEntity(entity);
    EntityCacheKey key = EntityCacheKey.of(ident, entity.type());
    String valueKey = keyspace.valueKey(key);
    StampRecord record = pendingStamps.get().remove(valueKey);
    if (record == null) {
      metrics.fillSkipped();
      LOG.debug(
          "Not caching {}: no miss on this thread recorded the stamp that would bound its load",
          key);
      return;
    }

    byte[] value;
    try {
      value = serializer.serialize(entity);
    } catch (RuntimeException e) {
      LOG.warn("Not caching entity {}: serialization failed", key, e);
      return;
    }

    List<byte[]> keys = ImmutableList.of(utf8(valueKey), utf8(keyspace.fencesKey(ident)));
    List<byte[]> args = Lists.newArrayList();
    args.add(value);
    args.add(utf8(Long.toString(valueTtlMs)));
    args.add(record.stamp);
    args.addAll(record.paths);

    try {
      Object written = run(PUT, keys, args);
      if (Long.valueOf(0L).equals(written)) {
        metrics.fillRejected();
        LOG.debug("Rejected write of {}: the entry was invalidated while it was loading", key);
      } else {
        metrics.fillWritten();
      }
    } catch (JedisException e) {
      logFailure("write", key, e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Entity & HasIdentifier> void invalidateOnKeyChange(E entity) {
    // Every cacheable entity is self-contained (see BaseEntityCache#isCacheable), so inserting one
    // never requires invalidating a different key.
  }

  /** {@inheritDoc} */
  @Override
  public <E extends Exception> void withCacheLock(EntityCacheKey key, ThrowingRunnable<E> action)
      throws E {
    Preconditions.checkArgument(key != null, "Key cannot be null");
    Preconditions.checkArgument(action != null, "Action cannot be null");
    try {
      segmentedLock.withLockAndThrow(key, action);
    } finally {
      pendingStamps.get().remove(keyspace.valueKey(key));
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T, E extends Exception> T withCacheLock(EntityCacheKey key, ThrowingSupplier<T, E> action)
      throws E {
    Preconditions.checkArgument(key != null, "Key cannot be null");
    Preconditions.checkArgument(action != null, "Action cannot be null");
    try {
      return segmentedLock.withLockAndThrow(key, action);
    } finally {
      pendingStamps.get().remove(keyspace.valueKey(key));
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Redis expires values on its own and keeps no index of them, so there is nothing to do.
   */
  @Override
  protected void invalidateExpiredItem(EntityCacheKey key) {}

  /**
   * {@inheritDoc}
   *
   * <p>Releases this node's Redis client only. The cached data is shared with the other nodes and
   * is left in place; clearing it is a separate, explicit operation. Every operation after this one
   * fails with an {@link IllegalStateException}: a closed cache must not quietly reconnect.
   */
  @Override
  public void close() {
    closed = true;
    if (metricsRegistered) {
      MetricsSystem metricsSystem = GravitinoEnv.getInstance().metricsSystem();
      if (metricsSystem != null) {
        metricsSystem.unregister(metrics);
      }
      metricsRegistered = false;
    }
    jedis.close();
  }

  @VisibleForTesting
  UnifiedJedis client() {
    return jedis;
  }

  @VisibleForTesting
  RedisKeyspace keyspace() {
    return keyspace;
  }

  @VisibleForTesting
  RedisEntityCacheMetricsSource metrics() {
    return metrics;
  }

  private static UnifiedJedis createClient(Config config) {
    String address = config.get(Configs.CACHE_REDIS_ADDRESS);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(address),
        "%s must be set when %s is 'redis'",
        Configs.CACHE_REDIS_ADDRESS.getKey(),
        Configs.CACHE_IMPLEMENTATION.getKey());

    Set<HostAndPort> nodes = new LinkedHashSet<>();
    for (String node : address.split(",")) {
      if (StringUtils.isNotBlank(node)) {
        nodes.add(HostAndPort.from(node.trim()));
      }
    }
    Preconditions.checkArgument(
        !nodes.isEmpty(), "%s lists no address", Configs.CACHE_REDIS_ADDRESS.getKey());

    int timeoutMs = config.get(Configs.CACHE_REDIS_TIMEOUT_MS);
    DefaultJedisClientConfig.Builder clientConfig =
        DefaultJedisClientConfig.builder()
            .timeoutMillis(timeoutMs)
            .ssl(config.get(Configs.CACHE_REDIS_SSL));
    String username = config.get(Configs.CACHE_REDIS_USERNAME);
    if (StringUtils.isNotBlank(username)) {
      clientConfig.user(username);
    }
    String password = config.get(Configs.CACHE_REDIS_PASSWORD);
    if (StringUtils.isNotBlank(password)) {
      clientConfig.password(password);
    }
    JedisClientConfig built = clientConfig.build();

    if (config.get(Configs.CACHE_REDIS_CLUSTER)) {
      return new JedisCluster(nodes, built);
    }
    Preconditions.checkArgument(
        nodes.size() == 1,
        "%s lists %s addresses; a standalone Redis takes exactly one, set %s=true for a cluster",
        Configs.CACHE_REDIS_ADDRESS.getKey(),
        nodes.size(),
        Configs.CACHE_REDIS_CLUSTER.getKey());
    return new JedisPooled(nodes.iterator().next(), built);
  }

  /**
   * Fails fast if Redis cannot be reached or evicts keys that carry no TTL, so a misconfiguration
   * never degrades silently.
   */
  private void probe(String address) {
    try {
      jedis.exists(keyspace.generationKey(""));
    } catch (JedisException e) {
      jedis.close();
      throw new IllegalStateException("Cannot reach the Redis entity cache at " + address, e);
    }
    try {
      checkEvictionPolicy();
    } catch (IllegalStateException e) {
      jedis.close();
      throw e;
    }
  }

  /**
   * Refuses an {@code allkeys-*} eviction policy, under which Redis may evict the fences and the
   * generation counter that the stale-fill guard relies on. Managed offerings often disable {@code
   * CONFIG}; the policy is then unknown and only a warning is logged.
   */
  private void checkEvictionPolicy() {
    List<String> policies = Lists.newArrayList();
    try {
      forEachPrimary(
          node ->
              policies.add(node.configGet(MAXMEMORY_POLICY).getOrDefault(MAXMEMORY_POLICY, "")));
    } catch (JedisException e) {
      LOG.warn(
          "Could not read {} from Redis ({}). The Redis entity cache requires a policy that "
              + "never evicts keys without a TTL: noeviction or one of volatile-*. Under "
              + "allkeys-* a stale entry can be served after an invalidation.",
          MAXMEMORY_POLICY,
          e.getMessage());
      return;
    }
    for (String policy : policies) {
      if (policy.startsWith("allkeys-")) {
        throw new IllegalStateException(
            String.format(
                "Redis reports %s=%s. The Redis entity cache requires noeviction or one of "
                    + "volatile-*, because allkeys-* can evict the fences that keep stale "
                    + "entries from being served",
                MAXMEMORY_POLICY, policy));
      }
    }
  }

  /** Invalidates a metalake, outdating every value under it. A Redis failure is propagated. */
  private void dropMetalake(String metalake) {
    EntityCacheKey key = EntityCacheKey.of(NameIdentifier.of(metalake), Entity.EntityType.METALAKE);
    run(
        DROP,
        ImmutableList.of(
            utf8(keyspace.generationKey(metalake)),
            utf8(keyspace.fencesKey(metalake)),
            utf8(keyspace.valueKey(key))),
        ImmutableList.of(utf8(metalake), utf8("1")));
  }

  /** Discards an entry atomically, and only if it still holds the bytes that were read. */
  private void discardQuietly(EntityCacheKey key, byte[] observed) {
    try {
      run(DISCARD, ImmutableList.of(utf8(keyspace.valueKey(key))), ImmutableList.of(observed));
    } catch (JedisException e) {
      logFailure("discard", key, e);
    }
  }

  /**
   * Runs a script by its digest, sending the body only when Redis does not have it cached yet, for
   * example after a restart or on a node that never ran it.
   */
  private Object run(Script script, List<byte[]> keys, List<byte[]> args) {
    try {
      try {
        return jedis.evalsha(script.sha, keys, args);
      } catch (JedisNoScriptException e) {
        return jedis.eval(script.body, keys, args);
      }
    } catch (JedisException e) {
      metrics.failure();
      throw e;
    }
  }

  /**
   * Runs an action on the standalone server, or on every primary of the cluster. Replicas are
   * skipped: a key they report is then operated on through the cluster client, which routes by slot
   * and follows redirections, so a topology change during a scan costs at most a retried command.
   */
  private void forEachPrimary(Consumer<Jedis> action) {
    if (jedis instanceof JedisPooled) {
      try (Jedis node = new Jedis(((JedisPooled) jedis).getPool().getResource())) {
        action.accept(node);
      }
      return;
    }
    if (!(jedis instanceof JedisCluster)) {
      throw new UnsupportedOperationException(
          "Unsupported Redis client " + jedis.getClass().getName());
    }
    for (ConnectionPool pool : ((JedisCluster) jedis).getClusterNodes().values()) {
      try (Jedis node = new Jedis(pool.getResource())) {
        if (node.info("replication").contains("role:master")) {
          action.accept(node);
        }
      }
    }
  }

  private static void scan(Jedis node, String pattern, Consumer<String> onKey) {
    ScanParams params = new ScanParams().match(pattern).count(SCAN_BATCH);
    String cursor = ScanParams.SCAN_POINTER_START;
    do {
      ScanResult<String> result = node.scan(cursor, params);
      result.getResult().forEach(onKey);
      cursor = result.getCursor();
    } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
  }

  private void logFailure(String operation, EntityCacheKey key, JedisException e) {
    long now = System.currentTimeMillis();
    long last = lastFailureLogMs.get();
    if (now - last >= FAILURE_LOG_INTERVAL_MS && lastFailureLogMs.compareAndSet(last, now)) {
      LOG.warn(
          "Redis entity cache {} of {} failed; treating as a miss. Further failures are logged "
              + "at DEBUG for {} ms and counted in the redis-entity-cache metrics",
          operation,
          key,
          FAILURE_LOG_INTERVAL_MS,
          e);
    } else {
      LOG.debug("Redis entity cache {} of {} failed; treating as a miss", operation, key, e);
    }
  }

  private static List<byte[]> fencePathArgs(NameIdentifier ident) {
    List<String> paths = RedisKeyspace.fencePaths(ident, schemaSeparator());
    List<byte[]> args = Lists.newArrayListWithCapacity(paths.size());
    for (String path : paths) {
      args.add(utf8(path));
    }
    return args;
  }

  private static String schemaSeparator() {
    return HierarchicalSchemaUtil.schemaSeparator();
  }

  private static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private void checkArguments(NameIdentifier ident, Entity.EntityType type) {
    Preconditions.checkArgument(ident != null, "NameIdentifier cannot be null");
    Preconditions.checkArgument(type != null, "EntityType cannot be null");
    checkOpen();
  }

  private void checkOpen() {
    Preconditions.checkState(!closed, "The Redis entity cache has been closed");
  }

  /** A Lua script with its SHA-1 digest, for {@code EVALSHA}. */
  private static final class Script {
    private final byte[] body;
    private final byte[] sha;

    private Script(String body) {
      this.body = utf8(body);
      try {
        byte[] digest = MessageDigest.getInstance("SHA-1").digest(this.body);
        StringBuilder hex = new StringBuilder(digest.length * 2);
        for (byte b : digest) {
          hex.append(String.format("%02x", b));
        }
        this.sha = utf8(hex.toString());
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException("SHA-1 is not available", e);
      }
    }
  }

  /** The stamp observed by a read miss, passed back to the write that fills the entry. */
  private static final class StampRecord {
    private final List<byte[]> paths;
    private final byte[] stamp;

    private StampRecord(List<byte[]> paths, byte[] stamp) {
      this.paths = paths;
      this.stamp = stamp;
    }
  }
}
