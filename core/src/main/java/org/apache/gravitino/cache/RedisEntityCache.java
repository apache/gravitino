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
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.NameIdentifier;
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
import redis.clients.jedis.commands.KeyCommands;
import redis.clients.jedis.commands.SortedSetCommands;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

/**
 * An {@link EntityCache} that keeps one copy of every cached entity in Redis, shared by all nodes
 * of a Gravitino cluster. It is selected with {@code gravitino.cache.implementation=redis} and
 * reports {@link Coherence#SHARED}: a write on any node invalidates the single shared copy, so no
 * per-node propagation is needed.
 *
 * <p>This is a shared cache, not a strongly consistent database and cache pair. The cache is only
 * touched after the entity store has committed, and an invalidation deletes the entry rather than
 * updating it in place, so a Redis failure degrades to a cache miss and never to a stale hit. What
 * remains is the window between a store commit and the invalidation that follows it: a node that
 * dies inside that window leaves the entry readable until its TTL expires. Callers that cannot
 * tolerate that must read the store directly.
 *
 * <p><b>Keyspace.</b> See {@link RedisKeyspace}. All keys of one metalake share a Redis Cluster
 * hash slot, so every script here is single-slot at any depth; the cost is that one metalake's
 * entries are bounded by one cluster node.
 *
 * <p><b>Container drop.</b> {@link #invalidate(NameIdentifier, Entity.EntityType)} runs one Lua
 * script that bumps the version fence of the dropped identifier, deletes its value, and walks the
 * lexicographic index range of its descendants deleting each one. The script is atomic, so a
 * concurrent reader sees the subtree either complete or gone, never half dropped.
 *
 * <p><b>Stale-write guard.</b> A read miss records the current fence of the identifier and of each
 * of its ancestors for the calling thread. The write that fills the entry afterwards is a Lua
 * script that compares those fences again and refuses to write if any has moved. A load that began
 * before a drop of the entity, or of any container above it, therefore cannot refill the key once
 * the drop has committed, whether or not the entity was indexed at the time. Fences outlive the
 * value TTL (see {@code gravitino.cache.redis.fenceTtlMs}), so a slow reader cannot win by
 * outliving the value. A write with no recorded fences, such as caching a freshly inserted entity,
 * is written unconditionally.
 *
 * <p><b>Failure policy.</b> Reads and fills are optimizations: a Redis error or timeout makes them
 * a miss or a no-op. An invalidation is a correctness obligation: a failure is propagated as a
 * {@link RuntimeException} so it is never silently dropped, and the entry expires by TTL at the
 * latest. An unreachable Redis at startup fails fast.
 */
public class RedisEntityCache extends BaseEntityCache {

  private static final Logger LOG = LoggerFactory.getLogger(RedisEntityCache.class);

  /** Per-thread bound on recorded fences awaiting the write that consumes them. */
  private static final int MAX_PENDING_FENCES = 1024;

  private static final long FAILURE_LOG_INTERVAL_MS = 30_000L;
  private static final int SCAN_BATCH = 500;

  /**
   * Reads a value, or on a miss the fences guarding a later fill. {@code KEYS[1]} is the value key;
   * {@code ARGV} lists the fence keys. Returns {@code {1, value}} on a hit and {@code {0,
   * fence...}} on a miss, with an absent fence reported as {@code "0"}.
   */
  @VisibleForTesting
  static final String READ_SCRIPT =
      "local v = redis.call('GET', KEYS[1])\n"
          + "if v then return {1, v} end\n"
          + "local r = {0}\n"
          + "for i = 1, #ARGV do r[#r + 1] = redis.call('GET', ARGV[i]) or '0' end\n"
          + "return r\n";

  /**
   * Writes a value unless a guarding fence moved. {@code KEYS[1]} is the index key and {@code
   * KEYS[2]} the value key; {@code ARGV[1]} is the index member, {@code ARGV[2]} the value, {@code
   * ARGV[3]} the TTL in milliseconds (0 for none), followed by (fence key, expected value) pairs.
   * Returns 1 if written and 0 if rejected.
   */
  @VisibleForTesting
  static final String PUT_SCRIPT =
      "for i = 4, #ARGV, 2 do\n"
          + "  if (redis.call('GET', ARGV[i]) or '0') ~= ARGV[i + 1] then return 0 end\n"
          + "end\n"
          + "if tonumber(ARGV[3]) > 0 then\n"
          + "  redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3])\n"
          + "else\n"
          + "  redis.call('SET', KEYS[2], ARGV[2])\n"
          + "end\n"
          + "redis.call('ZADD', KEYS[1], 0, ARGV[1])\n"
          + "return 1\n";

  /**
   * Drops an entry and its indexed descendants. {@code KEYS[1]} is the index key; {@code ARGV[1]}
   * is the slot prefix, {@code ARGV[2]} the index member, {@code ARGV[3]} the identifier whose
   * fence to bump, {@code ARGV[4]} the fence TTL in milliseconds (0 for none), followed by the
   * descendant prefixes to range over. The upper bound {@code prefix\xff} is above every UTF-8
   * member starting with the prefix. Returns the number of values deleted.
   */
  @VisibleForTesting
  static final String DROP_SCRIPT =
      "local idx = KEYS[1]\n"
          + "local prefix = ARGV[1]\n"
          + "local fence = prefix .. 'F:' .. ARGV[3]\n"
          + "redis.call('INCR', fence)\n"
          + "if tonumber(ARGV[4]) > 0 then redis.call('PEXPIRE', fence, ARGV[4]) end\n"
          + "local removed = redis.call('DEL', prefix .. 'D:' .. ARGV[2])\n"
          + "redis.call('ZREM', idx, ARGV[2])\n"
          + "for i = 5, #ARGV do\n"
          + "  local members = redis.call('ZRANGEBYLEX', idx, '[' .. ARGV[i], "
          + "'[' .. ARGV[i] .. '\\255')\n"
          + "  for _, m in ipairs(members) do\n"
          + "    removed = removed + redis.call('DEL', prefix .. 'D:' .. m)\n"
          + "    redis.call('ZREM', idx, m)\n"
          + "  end\n"
          + "end\n"
          + "return removed\n";

  private final UnifiedJedis jedis;
  private final RedisKeyspace keyspace;
  private final KryoEntitySerializer serializer;
  private final SegmentedLock segmentedLock;
  private final long valueTtlMs;
  private final long fenceTtlMs;
  private final byte[] readScript;
  private final byte[] putScript;
  private final byte[] dropScript;
  private final AtomicLong lastFailureLogMs = new AtomicLong();

  /**
   * Fences observed by a read miss on this thread, keyed by index member, consumed by the write
   * that fills the entry. Bounded and access-ordered so an entry whose write never happens (the
   * store had no such entity) is eventually evicted; a leftover entry can only make a later write
   * more conservative, never less.
   */
  private final ThreadLocal<Map<String, FenceSnapshot>> pendingFences =
      ThreadLocal.withInitial(
          () ->
              new LinkedHashMap<String, FenceSnapshot>(16, 0.75f, true) {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, FenceSnapshot> eldest) {
                  return size() > MAX_PENDING_FENCES;
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
    long configuredFenceTtl = cacheConfig.get(Configs.CACHE_REDIS_FENCE_TTL_MS);
    this.fenceTtlMs = configuredFenceTtl == 0 ? 2 * valueTtlMs : configuredFenceTtl;
    Preconditions.checkArgument(
        valueTtlMs == 0 || fenceTtlMs > valueTtlMs,
        "%s (%s ms) must exceed %s (%s ms)",
        Configs.CACHE_REDIS_FENCE_TTL_MS.getKey(),
        fenceTtlMs,
        Configs.CACHE_EXPIRATION_TIME.getKey(),
        valueTtlMs);
    this.readScript = utf8(READ_SCRIPT);
    this.putScript = utf8(PUT_SCRIPT);
    this.dropScript = utf8(DROP_SCRIPT);
  }

  /** {@inheritDoc} */
  @Override
  public Coherence coherence() {
    return Coherence.SHARED;
  }

  /**
   * {@inheritDoc}
   *
   * <p>On a miss, records the fences guarding this key for the calling thread so that the write
   * which follows can be rejected if a drop commits in between. A Redis failure is a miss.
   */
  @Override
  public <E extends Entity & HasIdentifier> Optional<E> getIfPresent(
      NameIdentifier ident, Entity.EntityType type) {
    checkArguments(ident, type);
    EntityCacheKey key = EntityCacheKey.of(ident, type);
    List<String> fencePaths = RedisKeyspace.fencePaths(ident, schemaSeparator());
    List<byte[]> fenceKeys = Lists.newArrayListWithCapacity(fencePaths.size());
    for (String path : fencePaths) {
      fenceKeys.add(utf8(keyspace.fenceKey(ident, path)));
    }

    List<?> reply;
    try {
      reply =
          (List<?>)
              jedis.eval(readScript, ImmutableList.of(utf8(keyspace.valueKey(key))), fenceKeys);
    } catch (JedisException e) {
      logFailure("read", key, e);
      return Optional.empty();
    }

    if (((Long) reply.get(0)) == 1L) {
      byte[] bytes = (byte[]) reply.get(1);
      try {
        return Optional.of(convertEntity(serializer.deserialize(bytes)));
      } catch (RuntimeException e) {
        LOG.warn("Discarding cache entry {} that could not be deserialized", key, e);
        discardQuietly(key);
        return Optional.empty();
      }
    }

    List<byte[]> epochs = Lists.newArrayListWithCapacity(fenceKeys.size());
    for (int i = 1; i < reply.size(); i++) {
      epochs.add((byte[]) reply.get(i));
    }
    pendingFences.get().put(RedisKeyspace.member(key), new FenceSnapshot(fenceKeys, epochs));
    return Optional.empty();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Bumps the fence of the identifier and deletes the entry together with every indexed
   * descendant in one atomic script. A Redis failure is propagated, because a dropped invalidation
   * would leave a stale value readable by every node.
   */
  @Override
  public boolean invalidate(NameIdentifier ident, Entity.EntityType type) {
    checkArguments(ident, type);
    EntityCacheKey key = EntityCacheKey.of(ident, type);
    List<byte[]> args = Lists.newArrayList();
    args.add(utf8(keyspace.slotPrefix(ident)));
    args.add(utf8(RedisKeyspace.member(key)));
    args.add(utf8(ident.toString()));
    args.add(utf8(Long.toString(fenceTtlMs)));
    for (String prefix : RedisKeyspace.descendantPrefixes(key, schemaSeparator())) {
      args.add(utf8(prefix));
    }
    List<byte[]> keys = ImmutableList.of(utf8(keyspace.indexKey(ident)));

    return segmentedLock.withLock(
        key,
        () -> {
          try {
            jedis.eval(dropScript, keys, args);
            return true;
          } catch (JedisException e) {
            throw new RuntimeException(
                "Failed to invalidate entity cache entry "
                    + key
                    + " in Redis; a stale entry may remain readable until it expires",
                e);
          }
        });
  }

  /** {@inheritDoc} A Redis failure is reported as absent. */
  @Override
  public boolean contains(NameIdentifier ident, Entity.EntityType type) {
    checkArguments(ident, type);
    EntityCacheKey key = EntityCacheKey.of(ident, type);
    try {
      return jedis.exists(utf8(keyspace.valueKey(key)));
    } catch (JedisException e) {
      logFailure("contains", key, e);
      return false;
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Sums the index of every metalake, scanning each cluster node, so the result is a
   * point-in-time estimate that may briefly exceed the number of live values: an index member is
   * removed by the next drop that ranges over it, not when its value expires.
   */
  @Override
  public long size() {
    AtomicLong total = new AtomicLong();
    forEachNode(
        node ->
            scan(
                node,
                keyspace.allIndexKeysPattern(),
                indexKey -> total.addAndGet(node.zcard(indexKey))));
    return total.get();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Removes every value and index key of this namespace on every node. Fences are left in place
   * so that a load in flight during the clear is still rejected; they expire on their own.
   */
  @Override
  public void clear() {
    segmentedLock.withGlobalLock(
        () -> {
          pendingFences.get().clear();
          forEachNode(
              node ->
                  scan(
                      node,
                      keyspace.allKeysPattern(),
                      key -> {
                        // One key per command: on a cluster node the keys scanned belong to many
                        // slots, and a multi-key UNLINK across slots is rejected.
                        if (!RedisKeyspace.isFenceKey(key)) {
                          node.unlink(key);
                        }
                      }));
        });
  }

  /**
   * {@inheritDoc}
   *
   * <p>Guarded by the fences recorded by the miss that preceded it on this thread, if any. A Redis
   * failure discards the write; the next read loads again.
   */
  @Override
  protected <E extends Entity & HasIdentifier> void doPut(E entity) {
    NameIdentifier ident = getIdentFromEntity(entity);
    EntityCacheKey key = EntityCacheKey.of(ident, entity.type());
    String member = RedisKeyspace.member(key);
    FenceSnapshot snapshot = pendingFences.get().remove(member);

    byte[] value;
    try {
      value = serializer.serialize(entity);
    } catch (RuntimeException e) {
      LOG.warn("Not caching entity {}: serialization failed", key, e);
      return;
    }

    List<byte[]> keys =
        ImmutableList.of(utf8(keyspace.indexKey(ident)), utf8(keyspace.valueKey(key)));
    List<byte[]> args = Lists.newArrayList();
    args.add(utf8(member));
    args.add(value);
    args.add(utf8(Long.toString(valueTtlMs)));
    if (snapshot != null) {
      for (int i = 0; i < snapshot.fenceKeys.size(); i++) {
        args.add(snapshot.fenceKeys.get(i));
        args.add(snapshot.epochs.get(i));
      }
    }

    try {
      Object written = jedis.eval(putScript, keys, args);
      if (Long.valueOf(0L).equals(written)) {
        LOG.debug("Rejected write of {}: the entry was invalidated while it was loading", key);
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
      pendingFences.get().remove(RedisKeyspace.member(key));
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
      pendingFences.get().remove(RedisKeyspace.member(key));
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>Redis expires values on its own; this only drops the index member, best effort.
   */
  @Override
  protected void invalidateExpiredItem(EntityCacheKey key) {
    try {
      jedis.zrem(keyspace.indexKey(key.identifier()), RedisKeyspace.member(key));
    } catch (JedisException e) {
      logFailure("unindex", key, e);
    }
  }

  /**
   * Closes the underlying Redis client. Not part of the {@link EntityCache} SPI: the entity store
   * keeps its cache for the life of the process, so this exists for tests and embedded use.
   */
  public void close() {
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
        DefaultJedisClientConfig.builder().timeoutMillis(timeoutMs);
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

  /** Fails fast if Redis cannot be reached, so a misconfiguration never degrades silently. */
  private void probe(String address) {
    try {
      jedis.exists(keyspace.allKeysPattern());
    } catch (JedisException e) {
      jedis.close();
      throw new IllegalStateException("Cannot reach the Redis entity cache at " + address, e);
    }
  }

  private void discardQuietly(EntityCacheKey key) {
    try {
      jedis.unlink(keyspace.valueKey(key));
      jedis.zrem(keyspace.indexKey(key.identifier()), RedisKeyspace.member(key));
    } catch (JedisException e) {
      logFailure("discard", key, e);
    }
  }

  /** Runs an action against the standalone server, or against every node of the cluster. */
  private void forEachNode(Consumer<NodeCommands> action) {
    if (jedis instanceof JedisCluster) {
      for (ConnectionPool pool : ((JedisCluster) jedis).getClusterNodes().values()) {
        try (Jedis node = new Jedis(pool.getResource())) {
          action.accept(new NodeCommands(node, node));
        }
      }
    } else {
      action.accept(new NodeCommands(jedis, jedis));
    }
  }

  private static void scan(NodeCommands node, String pattern, Consumer<String> onKey) {
    ScanParams params = new ScanParams().match(pattern).count(SCAN_BATCH);
    String cursor = ScanParams.SCAN_POINTER_START;
    do {
      ScanResult<String> result = node.keys.scan(cursor, params);
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
              + "at DEBUG for {} ms",
          operation,
          key,
          FAILURE_LOG_INTERVAL_MS,
          e);
    } else {
      LOG.debug("Redis entity cache {} of {} failed; treating as a miss", operation, key, e);
    }
  }

  private static String schemaSeparator() {
    return HierarchicalSchemaUtil.schemaSeparator();
  }

  private static byte[] utf8(String value) {
    return value.getBytes(StandardCharsets.UTF_8);
  }

  private static void checkArguments(NameIdentifier ident, Entity.EntityType type) {
    Preconditions.checkArgument(ident != null, "NameIdentifier cannot be null");
    Preconditions.checkArgument(type != null, "EntityType cannot be null");
  }

  /** The fences observed by a read miss, passed back to the write that fills the entry. */
  private static final class FenceSnapshot {
    private final List<byte[]> fenceKeys;
    private final List<byte[]> epochs;

    private FenceSnapshot(List<byte[]> fenceKeys, List<byte[]> epochs) {
      Preconditions.checkArgument(
          fenceKeys.size() == epochs.size(), "fence keys and epochs must pair up");
      this.fenceKeys = fenceKeys;
      this.epochs = epochs;
    }
  }

  /** The two command groups the node-wide operations need, over a standalone or node client. */
  private static final class NodeCommands {
    private final KeyCommands keys;
    private final SortedSetCommands sortedSets;

    private NodeCommands(KeyCommands keys, SortedSetCommands sortedSets) {
      this.keys = keys;
      this.sortedSets = sortedSets;
    }

    private long zcard(String key) {
      return sortedSets.zcard(key);
    }

    private void unlink(String key) {
      keys.unlink(key);
    }
  }
}
