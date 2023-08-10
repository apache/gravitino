/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage.kv;

import static com.datastrato.graviton.Configs.ENTITY_KV_STORE;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.Entity;
import com.datastrato.graviton.Entity.EntityIdentifer;
import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.EntityAlreadyExistsException;
import com.datastrato.graviton.EntitySerDe;
import com.datastrato.graviton.EntityStore;
import com.datastrato.graviton.HasIdentifier;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.NoSuchEntityException;
import com.datastrato.graviton.util.Bytes;
import com.datastrato.graviton.util.Executable;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KV store to store entities. This means we can store entities in a key value store. I.e. RocksDB,
 * Cassandra, etc. If you want to use a different backend, you can implement the {@link KvBackend}
 * interface
 */
public class KvEntityStore implements EntityStore {
  public static final Logger LOGGER = LoggerFactory.getLogger(KvEntityStore.class);
  public static final ImmutableMap<String, String> KV_BACKENDS =
      ImmutableMap.of("RocksDBKvBackend", RocksDBKvBackend.class.getCanonicalName());

  @Getter @VisibleForTesting private KvBackend backend;
  private EntityKeyEncoder entityKeyEncoder;
  private EntitySerDe serDe;

  @Override
  public void initialize(Config config) throws RuntimeException {
    this.backend = createKvEntityBackend(config);
    this.entityKeyEncoder = new CustomEntityKeyEncoder(backend);
  }

  @Override
  public void setSerDe(EntitySerDe entitySerDe) {
    this.serDe = entitySerDe;
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Class<E> e, EntityType type) throws IOException {
    // Star means it's a wildcard
    NameIdentifier identifier = NameIdentifier.of(namespace, "*");
    byte[] startKey = entityKeyEncoder.encode(EntityIdentifer.of(identifier, type), false);
    byte[] endKey = Bytes.increment(Bytes.wrap(startKey)).get();
    List<Pair<byte[], byte[]>> kvs =
        backend.scan(
            new KvRangeScan.KvRangeScanBuilder()
                .start(startKey)
                .end(endKey)
                .startInclusive(true)
                .endInclusive(false)
                .limit(Integer.MAX_VALUE)
                .build());

    List<E> entities = Lists.newArrayList();
    for (Pair<byte[], byte[]> pairs : kvs) {
      entities.add(serDe.deserialize(pairs.getRight(), e));
    }
    // TODO (yuqi), if the list is too large, we need to do pagination or streaming
    return entities;
  }

  @Override
  public boolean exists(NameIdentifier ident, EntityType entityType) throws IOException {
    return backend.get(entityKeyEncoder.encode(EntityIdentifer.of(ident, entityType), false))
        != null;
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    executeInTransaction(
        () -> {
          byte[] key =
              entityKeyEncoder.encode(EntityIdentifer.of(e.nameIdentifier(), e.type()), true);
          byte[] value = serDe.serialize(e);
          backend.put(key, value, overwritten);
          return null;
        });
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Class<E> type, EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException {
    return executeInTransaction(
        () -> {
          byte[] key = entityKeyEncoder.encode(EntityIdentifer.of(ident, entityType), false);
          byte[] value = backend.get(key);
          if (value == null) {
            throw new NoSuchEntityException(ident.toString());
          }

          E e = serDe.deserialize(value, type);
          E updatedE = updater.apply(e);
          if (!updatedE.nameIdentifier().equals(ident)) {
            delete(ident, entityType);
          }

          put(updatedE, true /* overwritten */);
          return updatedE;
        });
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, EntityType entityType, Class<E> e)
      throws NoSuchEntityException, IOException {
    byte[] key = entityKeyEncoder.encode(EntityIdentifer.of(ident, entityType), false);
    // refer type from key;
    byte[] value = backend.get(key);
    if (value == null) {
      throw new NoSuchEntityException(ident.toString());
    }
    return serDe.deserialize(value, e);
  }

  @Override
  public boolean delete(NameIdentifier ident, EntityType entityType) throws IOException {
    return backend.delete(entityKeyEncoder.encode(EntityIdentifer.of(ident, entityType), false));
  }

  @Override
  public <R, E extends Exception> R executeInTransaction(Executable<R, E> executable)
      throws E, IOException {
    return backend.executeInTransaction(executable);
  }

  @Override
  public void close() throws IOException {
    backend.close();
  }

  private static KvBackend createKvEntityBackend(Config config) {
    String backendName = config.get(ENTITY_KV_STORE);
    String className = KV_BACKENDS.getOrDefault(backendName, backendName);
    if (Objects.isNull(className)) {
      throw new RuntimeException("Unsupported backend type..." + backendName);
    }

    try {
      KvBackend kvBackend = (KvBackend) Class.forName(className).newInstance();
      kvBackend.initialize(config);
      return kvBackend;
    } catch (Exception e) {
      LOGGER.error("Failed to create and initialize KvBackend by name '{}'.", backendName, e);
      throw new RuntimeException(
          "Failed to create and initialize KvBackend by name: " + backendName, e);
    }
  }
}
