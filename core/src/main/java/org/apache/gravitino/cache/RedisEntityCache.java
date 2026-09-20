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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
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
 * script that moves the fence of the dropped identifier to a fresh generation, deletes its value,
 * and walks the lexicographic index range of its descendants deleting each one. The script is
 * atomic, so a concurrent reader sees the subtree either complete or gone, never half dropped.
 *
 * <p><b>Stale-fill guard.</b> A read miss records, for the calling thread, the fences of the
 * identifier and of each of its ancestors as they were at the miss. The write that fills the entry
 * afterwards is a Lua script that compares those fences again and refuses to write if any moved, so
 * a load that began before a drop of the entity, or of any container above it, cannot refill the
 * key once the drop committed. The guard fails closed: a write with no recorded fences, whether
 * because no miss preceded it on this thread (caching a freshly inserted entity), because the miss
 * failed to reach Redis, or because the record was evicted from the per-thread bound, is not
 * written at all; the next read loads it under a fresh record.
 *
 * <p><b>Fence lifetime.</b> Fence values are generations drawn from a per-metalake counter that
 * never expires, so a recreated fence can never repeat the value a stale reader observed. Fence
 * keys themselves may expire after {@code gravitino.cache.redis.fenceTtlMs}, and a fill whose
 * record is older than that lifetime is discarded on the client, so a fence can only be missing for
 * a fill that is too old to be accepted anyway. Together these close both reuse sequences: a fence
 * that expires and is recreated, and a fence that was absent, set, and expired again.
 *
 * <p><b>Clear.</b> {@link #clear()} is atomic per metalake: one script moves the metalake's own
 * fence to a fresh generation, which rejects every fill in flight for that metalake, and deletes
 * the values and the index together, so a concurrent fill can never leave a value without its index
 * member. Fences are left in place and expire on their own.
 *
 * <p><b>Index reclamation.</b> Redis expires values on its own and leaves their index members
 * behind. A bounded reaper removes members whose value is gone, one batch per {@value
 * #REAP_EVERY_N_WRITES} writes into a metalake and one batch per index visited by {@link #size()},
 * checking absence and removing inside one script so it can never unindex a concurrent refill.
 *
 * <p><b>Failure policy.</b> Reads and fills are optimizations: a Redis error or timeout makes them
 * a miss or a no-op. An invalidation is a correctness obligation: a failure is propagated as a
 * {@link RuntimeException} so it is never silently dropped, and the entry expires by TTL at the
 * latest. An unreachable Redis at startup fails fast. {@link #close()} releases only this node's
 * client; the shared data is left for the other nodes.
 */
public class RedisEntityCache extends BaseEntityCache {

  private static final Logger LOG = LoggerFactory.getLogger(RedisEntityCache.class);

  /** Per-thread bound on recorded fences awaiting the write that consumes them. */
  @VisibleForTesting static final int MAX_PENDING_FENCES = 1024;

  /** Writes into one metalake between two reaper batches over its index. */
  @VisibleForTesting static final int REAP_EVERY_N_WRITES = 64;

  /** Index members one reaper batch examines. */
  @VisibleForTesting static final int REAP_BATCH = 256;

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
   * fence to move, {@code ARGV[4]} the fence TTL in milliseconds (0 for none), followed by the
   * descendant prefixes to range over. The fence takes the next generation of the metalake's
   * never-expiring counter. The upper bound {@code prefix\xff} is above every UTF-8 member starting
   * with the prefix. Returns the number of values deleted.
   */
  @VisibleForTesting
  static final String DROP_SCRIPT =
      "local idx = KEYS[1]\n"
          + "local prefix = ARGV[1]\n"
          + "local fence = prefix .. 'F:' .. ARGV[3]\n"
          + "local generation = redis.call('INCR', prefix .. 'G')\n"
          + "redis.call('SET', fence, generation)\n"
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

  /**
   * Clears one metalake atomically. {@code KEYS[1]} is the index key; {@code ARGV[1]} is the slot
   * prefix, {@code ARGV[2]} the fence key of the metalake itself, {@code ARGV[3]} the fence TTL in
   * milliseconds (0 for none). Moves the metalake fence to a fresh generation, so every fill in
   * flight for the metalake is rejected, then deletes every indexed value and the index in the same
   * script, so no fill can slip between them. Returns the number of values deleted.
   */
  @VisibleForTesting
  static final String CLEAR_SCRIPT =
      "local idx = KEYS[1]\n"
          + "local prefix = ARGV[1]\n"
          + "local generation = redis.call('INCR', prefix .. 'G')\n"
          + "redis.call('SET', ARGV[2], generation)\n"
          + "if tonumber(ARGV[3]) > 0 then redis.call('PEXPIRE', ARGV[2], ARGV[3]) end\n"
          + "local members = redis.call('ZRANGE', idx, 0, -1)\n"
          + "local removed = 0\n"
          + "for _, m in ipairs(members) do\n"
          + "  removed = removed + redis.call('DEL', prefix .. 'D:' .. m)\n"
          + "end\n"
          + "redis.call('DEL', idx)\n"
          + "return removed\n";

  /**
   * Discards an entry only if it still holds the bytes the caller read. {@code KEYS[1]} is the
   * index key and {@code KEYS[2]} the value key; {@code ARGV[1]} is the index member, {@code
   * ARGV[2]} the bytes observed. A concurrent fill that replaced the value is left intact together
   * with its index member. Returns 1 if discarded and 0 otherwise.
   */
  @VisibleForTesting
  static final String DISCARD_SCRIPT =
      "if redis.call('GET', KEYS[2]) == ARGV[2] then\n"
          + "  redis.call('DEL', KEYS[2])\n"
          + "  redis.call('ZREM', KEYS[1], ARGV[1])\n"
          + "  return 1\n"
          + "end\n"
          + "return 0\n";

  /**
   * Removes index members whose value no longer exists, one batch at a time. {@code KEYS[1]} is the
   * index key; {@code ARGV[1]} is the slot prefix, {@code ARGV[2]} the member to resume after
   * (empty to start), {@code ARGV[3]} the batch size. The absence check and the removal run in one
   * script, so a member re-added by a concurrent fill is never removed. Returns {@code {removed,
   * examined, last member}}.
   */
  @VisibleForTesting
  static final String REAP_SCRIPT =
      "local idx = KEYS[1]\n"
          + "local prefix = ARGV[1]\n"
          + "local lower = '-'\n"
          + "if ARGV[2] ~= '' then lower = '(' .. ARGV[2] end\n"
          + "local members = redis.call('ZRANGEBYLEX', idx, lower, '+', 'LIMIT', 0, "
          + "tonumber(ARGV[3]))\n"
          + "local removed = 0\n"
          + "for _, m in ipairs(members) do\n"
          + "  if redis.call('EXISTS', prefix .. 'D:' .. m) == 0 then\n"
          + "    removed = removed + redis.call('ZREM', idx, m)\n"
          + "  end\n"
          + "end\n"
          + "local last = ''\n"
          + "if #members > 0 then last = members[#members] end\n"
          + "return {removed, #members, last}\n";

  private final UnifiedJedis jedis;
  private final RedisKeyspace keyspace;
  private final KryoEntitySerializer serializer;
  private final SegmentedLock segmentedLock;
  private final long valueTtlMs;
  private final long fenceTtlMs;
  private final byte[] readScript;
  private final byte[] putScript;
  private final byte[] dropScript;
  private final byte[] clearScript;
  private final byte[] discardScript;
  private final byte[] reapScript;
  private final AtomicLong lastFailureLogMs = new AtomicLong();
  private final AtomicLong writes = new AtomicLong();

  /**
   * Set by {@link #close()}. A cluster client reconnects on demand after it was closed, so the
   * client alone cannot tell a closed cache from an open one; every operation checks this first.
   */
  private volatile boolean closed;

  /** Where the reaper resumes in each index, keyed by index key; absent means from the start. */
  private final ConcurrentMap<String, String> reapCursors = new ConcurrentHashMap<>();

  /**
   * Fences observed by a read miss on this thread, keyed by index member, consumed by the write
   * that fills the entry. Bounded and access-ordered so an entry whose write never happens (the
   * store had no such entity) is eventually evicted; a write whose record was evicted is not
   * performed, never performed unguarded.
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
    this.clearScript = utf8(CLEAR_SCRIPT);
    this.discardScript = utf8(DISCARD_SCRIPT);
    this.reapScript = utf8(REAP_SCRIPT);
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
   * which follows can be rejected if a drop commits in between. A Redis failure is a miss that
   * records nothing, so the write which follows it is not performed.
   */
  @Override
  public <E extends Entity & HasIdentifier> Optional<E> getIfPresent(
      NameIdentifier ident, Entity.EntityType type) {
    checkArguments(ident, type);
    EntityCacheKey key = EntityCacheKey.of(ident, type);
    String member = RedisKeyspace.member(key);
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
      // A record left by an earlier miss must not vouch for a load this read did not bound.
      pendingFences.get().remove(member);
      logFailure("read", key, e);
      return Optional.empty();
    }

    if (((Long) reply.get(0)) == 1L) {
      byte[] bytes = (byte[]) reply.get(1);
      try {
        return Optional.of(convertEntity(serializer.deserialize(bytes)));
      } catch (RuntimeException e) {
        LOG.warn("Discarding cache entry {} that could not be deserialized", key, e);
        pendingFences.get().remove(member);
        discardQuietly(key, bytes);
        return Optional.empty();
      }
    }

    List<byte[]> epochs = Lists.newArrayListWithCapacity(fenceKeys.size());
    for (int i = 1; i < reply.size(); i++) {
      epochs.add((byte[]) reply.get(i));
    }
    pendingFences.get().put(member, new FenceSnapshot(fenceKeys, epochs));
    return Optional.empty();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Moves the fence of the identifier to a fresh generation and deletes the entry together with
   * every indexed descendant in one atomic script. A Redis failure is propagated, because a dropped
   * invalidation would leave a stale value readable by every node.
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
   * <p>Sums the index of every metalake, scanning each cluster primary, so the result is a
   * point-in-time estimate. Each index visited also gets one reaper batch, so members left behind
   * by expired values are reclaimed by repeated calls even on an idle cache.
   */
  @Override
  public long size() {
    checkOpen();
    AtomicLong total = new AtomicLong();
    forEachPrimary(
        scanner ->
            scan(
                scanner,
                keyspace.allIndexKeysPattern(),
                indexKey -> {
                  if (!keyspace.isIndexKey(indexKey)) {
                    return;
                  }
                  reapBatch(indexKey);
                  total.addAndGet(jedis.zcard(indexKey));
                }));
    return total.get();
  }

  /**
   * {@inheritDoc}
   *
   * <p>Clears one metalake at a time, each atomically with respect to the fills of that metalake:
   * the metalake's own fence moves to a fresh generation, rejecting every fill in flight, and the
   * values and index are deleted in the same script. Fences are left in place and expire on their
   * own. Only keys owned by this namespace, matched exactly, are touched.
   */
  @Override
  public void clear() {
    checkOpen();
    segmentedLock.withGlobalLock(
        () -> {
          pendingFences.get().clear();
          reapCursors.clear();
          forEachPrimary(
              scanner ->
                  scan(
                      scanner,
                      keyspace.allIndexKeysPattern(),
                      indexKey -> {
                        if (!keyspace.isIndexKey(indexKey)) {
                          return;
                        }
                        List<byte[]> args =
                            ImmutableList.of(
                                utf8(keyspace.slotPrefixOf(indexKey)),
                                utf8(keyspace.metalakeFenceKeyOf(indexKey)),
                                utf8(Long.toString(fenceTtlMs)));
                        jedis.eval(clearScript, ImmutableList.of(utf8(indexKey)), args);
                      }));
        });
  }

  /**
   * {@inheritDoc}
   *
   * <p>Guarded by the fences recorded by the miss that preceded it on this thread. Fails closed:
   * with no record, or a record older than the fence lifetime, nothing is written and the next read
   * loads the entity under a fresh record. A Redis failure discards the write.
   */
  @Override
  protected <E extends Entity & HasIdentifier> void doPut(E entity) {
    checkOpen();
    NameIdentifier ident = getIdentFromEntity(entity);
    EntityCacheKey key = EntityCacheKey.of(ident, entity.type());
    String member = RedisKeyspace.member(key);
    FenceSnapshot snapshot = pendingFences.get().remove(member);
    if (snapshot == null) {
      LOG.debug(
          "Not caching {}: no miss on this thread recorded the fences that would bound its load",
          key);
      return;
    }
    if (fenceTtlMs > 0 && snapshot.ageMs() >= fenceTtlMs) {
      LOG.debug(
          "Not caching {}: its fence record is {} ms old, at least the fence lifetime of {} ms",
          key,
          snapshot.ageMs(),
          fenceTtlMs);
      return;
    }

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
    for (int i = 0; i < snapshot.fenceKeys.size(); i++) {
      args.add(snapshot.fenceKeys.get(i));
      args.add(snapshot.epochs.get(i));
    }

    try {
      Object written = jedis.eval(putScript, keys, args);
      if (Long.valueOf(0L).equals(written)) {
        LOG.debug("Rejected write of {}: the entry was invalidated while it was loading", key);
      }
      if (writes.incrementAndGet() % REAP_EVERY_N_WRITES == 0) {
        reapBatch(keyspace.indexKey(ident));
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
   * <p>Redis expires values on its own and nothing calls this hook for them; the index members they
   * leave behind are reclaimed by the reaper instead. Kept for callers that learn of an expiry by
   * other means: removes the member only if the value is indeed gone.
   */
  @Override
  protected void invalidateExpiredItem(EntityCacheKey key) {
    try {
      jedis.eval(
          discardScript,
          ImmutableList.of(utf8(keyspace.indexKey(key.identifier())), utf8(keyspace.valueKey(key))),
          ImmutableList.of(utf8(RedisKeyspace.member(key)), new byte[0]));
    } catch (JedisException e) {
      logFailure("unindex", key, e);
    }
  }

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
    jedis.close();
  }

  /**
   * Runs the reaper over the whole index of the given metalake, batch after batch, until it has
   * been examined end to end.
   *
   * @param metalake the metalake whose index to reap
   * @return the number of index members removed
   */
  @VisibleForTesting
  long reapIndex(NameIdentifier metalake) {
    String indexKey = keyspace.indexKey(metalake);
    reapCursors.remove(indexKey);
    long removed = 0;
    do {
      removed += reapBatch(indexKey);
    } while (reapCursors.containsKey(indexKey));
    return removed;
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
      jedis.exists(keyspace.allIndexKeysPattern());
    } catch (JedisException e) {
      jedis.close();
      throw new IllegalStateException("Cannot reach the Redis entity cache at " + address, e);
    }
  }

  /** Discards an entry atomically, and only if it still holds the bytes that were read. */
  private void discardQuietly(EntityCacheKey key, byte[] observed) {
    try {
      jedis.eval(
          discardScript,
          ImmutableList.of(utf8(keyspace.indexKey(key.identifier())), utf8(keyspace.valueKey(key))),
          ImmutableList.of(utf8(RedisKeyspace.member(key)), observed));
    } catch (JedisException e) {
      logFailure("discard", key, e);
    }
  }

  /**
   * Runs one reaper batch over an index, resuming where the previous batch stopped. The cursor is
   * dropped once the index has been examined end to end, so the next batch starts over.
   *
   * @return the number of index members removed, 0 on a Redis failure
   */
  private long reapBatch(String indexKey) {
    String cursor = reapCursors.getOrDefault(indexKey, "");
    List<byte[]> args =
        ImmutableList.of(
            utf8(keyspace.slotPrefixOf(indexKey)),
            utf8(cursor),
            utf8(Integer.toString(REAP_BATCH)));
    try {
      List<?> reply = (List<?>) jedis.eval(reapScript, ImmutableList.of(utf8(indexKey)), args);
      long removed = (Long) reply.get(0);
      long examined = (Long) reply.get(1);
      if (examined < REAP_BATCH) {
        reapCursors.remove(indexKey);
      } else {
        reapCursors.put(indexKey, new String((byte[]) reply.get(2), StandardCharsets.UTF_8));
      }
      if (removed > 0) {
        LOG.debug("Reclaimed {} index members of expired values from {}", removed, indexKey);
      }
      return removed;
    } catch (JedisException e) {
      reapCursors.remove(indexKey);
      LOG.debug("Index reclamation of {} failed; it resumes on a later write", indexKey, e);
      return 0;
    }
  }

  /**
   * Runs an action with a scanner over the standalone server, or over every primary of the cluster.
   * Replicas are skipped: a key they report would then be operated on through the cluster client,
   * which routes by slot and follows redirections, so a topology change during the scan costs at
   * most a retried command.
   */
  private void forEachPrimary(Consumer<Scanner> action) {
    if (!(jedis instanceof JedisCluster)) {
      action.accept(jedis::scan);
      return;
    }
    for (ConnectionPool pool : ((JedisCluster) jedis).getClusterNodes().values()) {
      try (Jedis node = new Jedis(pool.getResource())) {
        if (node.info("replication").contains("role:master")) {
          action.accept(node::scan);
        }
      }
    }
  }

  private static void scan(Scanner scanner, String pattern, Consumer<String> onKey) {
    ScanParams params = new ScanParams().match(pattern).count(SCAN_BATCH);
    String cursor = ScanParams.SCAN_POINTER_START;
    do {
      ScanResult<String> result = scanner.scan(cursor, params);
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

  private void checkArguments(NameIdentifier ident, Entity.EntityType type) {
    Preconditions.checkArgument(ident != null, "NameIdentifier cannot be null");
    Preconditions.checkArgument(type != null, "EntityType cannot be null");
    checkOpen();
  }

  private void checkOpen() {
    Preconditions.checkState(!closed, "The Redis entity cache has been closed");
  }

  /** The fences observed by a read miss, passed back to the write that fills the entry. */
  private static final class FenceSnapshot {
    private final List<byte[]> fenceKeys;
    private final List<byte[]> epochs;
    private final long takenAtNanos = System.nanoTime();

    private FenceSnapshot(List<byte[]> fenceKeys, List<byte[]> epochs) {
      Preconditions.checkArgument(
          fenceKeys.size() == epochs.size(), "fence keys and epochs must pair up");
      this.fenceKeys = fenceKeys;
      this.epochs = epochs;
    }

    private long ageMs() {
      return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - takenAtNanos);
    }
  }

  /** A {@code SCAN} over one server: the standalone server or one cluster primary. */
  @FunctionalInterface
  private interface Scanner {
    ScanResult<String> scan(String cursor, ScanParams params);
  }
}
