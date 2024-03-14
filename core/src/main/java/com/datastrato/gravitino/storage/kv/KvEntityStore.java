/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import static com.datastrato.gravitino.Configs.ENTITY_KV_STORE;
import static com.datastrato.gravitino.Entity.EntityType.CATALOG;
import static com.datastrato.gravitino.Entity.EntityType.FILESET;
import static com.datastrato.gravitino.Entity.EntityType.SCHEMA;
import static com.datastrato.gravitino.Entity.EntityType.TABLE;
import static com.datastrato.gravitino.storage.kv.BinaryEntityKeyEncoder.LOG;
import static com.datastrato.gravitino.storage.kv.BinaryEntityKeyEncoder.NAMESPACE_SEPARATOR;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.Entity.EntityType;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.EntitySerDe;
import com.datastrato.gravitino.EntitySerDeFactory;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NonEmptyEntityException;
import com.datastrato.gravitino.storage.EntityKeyEncoder;
import com.datastrato.gravitino.storage.FunctionUtils;
import com.datastrato.gravitino.storage.NameMappingService;
import com.datastrato.gravitino.storage.StorageLayoutVersion;
import com.datastrato.gravitino.storage.TransactionIdGenerator;
import com.datastrato.gravitino.utils.Bytes;
import com.datastrato.gravitino.utils.Executable;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KV store to store entities. This means we can store entities in a key value store. I.e., RocksDB,
 * Cassandra, etc. If you want to use a different backend, you can implement the {@link KvBackend}
 * interface
 */
public class KvEntityStore implements EntityStore {

  private static final String NO_SUCH_ENTITY_MSG = "No such entity:%s";

  public static final Logger LOGGER = LoggerFactory.getLogger(KvEntityStore.class);
  public static final ImmutableMap<String, String> KV_BACKENDS =
      ImmutableMap.of("RocksDBKvBackend", RocksDBKvBackend.class.getCanonicalName());
  public static final byte[] LAYOUT_VERSION_KEY =
      Bytes.concat(
          new byte[] {0x1D, 0x00, 0x02}, "layout_version".getBytes(StandardCharsets.UTF_8));

  @Getter @VisibleForTesting KvBackend backend;

  // Lock to control the concurrency of the entity store, to be more exact, the concurrency of
  // accessing the underlying kv store.
  private ReentrantReadWriteLock reentrantReadWriteLock;
  @VisibleForTesting EntityKeyEncoder<byte[]> entityKeyEncoder;
  @VisibleForTesting NameMappingService nameMappingService;
  private EntitySerDe serDe;
  // We will use storageLayoutVersion to check whether the layout of the storage is compatible with
  // the current version of the code.
  // Note: If we change the layout of the storage in the future, please update the value of
  // storageLayoutVersion if it's necessary.
  @VisibleForTesting StorageLayoutVersion storageLayoutVersion;

  private TransactionIdGenerator txIdGenerator;
  @VisibleForTesting KvGarbageCollector kvGarbageCollector;
  private TransactionalKvBackend transactionalKvBackend;

  @Override
  public void initialize(Config config) throws RuntimeException {
    this.backend = createKvEntityBackend(config);
    // TODO(yuqi) Currently, KvNameMappingService and KvEntityStore shares the same backend
    //  instance, We should make it configurable in the future.
    this.txIdGenerator = new TransactionIdGeneratorImpl(backend, config);
    txIdGenerator.start();

    this.transactionalKvBackend = new TransactionalKvBackendImpl(backend, txIdGenerator);

    this.reentrantReadWriteLock = new ReentrantReadWriteLock();

    this.nameMappingService =
        new KvNameMappingService(transactionalKvBackend, reentrantReadWriteLock);
    this.entityKeyEncoder = new BinaryEntityKeyEncoder(nameMappingService);

    this.kvGarbageCollector = new KvGarbageCollector(backend, config, entityKeyEncoder);
    kvGarbageCollector.start();

    this.storageLayoutVersion = initStorageVersionInfo();
    this.serDe = EntitySerDeFactory.createEntitySerDe(config);
  }

  @Override
  public void setSerDe(EntitySerDe entitySerDe) {
    this.serDe = entitySerDe;
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Class<E> e, EntityType type) throws IOException {
    // Star means it's a wildcard
    List<E> entities = Lists.newArrayList();
    NameIdentifier identifier = NameIdentifier.of(namespace, BinaryEntityKeyEncoder.WILD_CARD);
    byte[] startKey = entityKeyEncoder.encode(identifier, type, true);
    if (startKey == null) {
      return entities;
    }

    byte[] endKey = Bytes.increment(Bytes.wrap(startKey)).get();
    List<Pair<byte[], byte[]>> kvs =
        executeInTransaction(
            () ->
                transactionalKvBackend.scan(
                    new KvRange.KvRangeBuilder()
                        .start(startKey)
                        .end(endKey)
                        .startInclusive(true)
                        .endInclusive(false)
                        .limit(Integer.MAX_VALUE)
                        .build()));
    for (Pair<byte[], byte[]> pairs : kvs) {
      entities.add(serDe.deserialize(pairs.getRight(), e));
    }
    // TODO (yuqi), if the list is too large, we need to do pagination or streaming
    return entities;
  }

  @Override
  public boolean exists(NameIdentifier ident, EntityType entityType) throws IOException {
    return executeInTransaction(
        () -> {
          byte[] key = entityKeyEncoder.encode(ident, entityType, true);
          if (key == null) {
            return false;
          }
          return transactionalKvBackend.get(key) != null;
        });
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    executeInTransaction(
        () -> {
          byte[] key = entityKeyEncoder.encode(e.nameIdentifier(), e.type());
          byte[] value = serDe.serialize(e);
          transactionalKvBackend.put(key, value, overwritten);
          return null;
        });
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Class<E> type, EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, AlreadyExistsException {
    return executeInTransaction(
        () -> {
          byte[] key = entityKeyEncoder.encode(ident, entityType);
          byte[] value = transactionalKvBackend.get(key);
          if (value == null) {
            throw new NoSuchEntityException(NO_SUCH_ENTITY_MSG, ident.toString());
          }

          E e = serDe.deserialize(value, type);
          E updatedE = updater.apply(e);
          if (updatedE.nameIdentifier().equals(ident)) {
            transactionalKvBackend.put(key, serDe.serialize(updatedE), true);
            return updatedE;
          }

          // If we have changed the name of the entity, We would do the following steps:
          // Check whether the new entities already existed
          boolean newEntityExist = exists(updatedE.nameIdentifier(), entityType);
          if (newEntityExist) {
            throw new AlreadyExistsException(
                "Entity %s already exist, please check again", updatedE.nameIdentifier());
          }

          // Update the name mapping
          nameMappingService.updateName(
              generateKeyForMapping(ident), generateKeyForMapping(updatedE.nameIdentifier()));

          // Update the entity to store
          transactionalKvBackend.put(key, serDe.serialize(updatedE), true);
          return updatedE;
        });
  }

  private String concatIdAndName(long[] namespaceIds, String name) {
    String context =
        Joiner.on(NAMESPACE_SEPARATOR)
            .join(
                Arrays.stream(namespaceIds).mapToObj(String::valueOf).collect(Collectors.toList()));
    return StringUtils.isBlank(context) ? name : context + NAMESPACE_SEPARATOR + name;
  }

  /**
   * Generate the key for name to id mapping. Currently, the mapping is as following.
   *
   * <pre>
   *   Assume we have the following entities:
   *   metalake: a1        ---- 1
   *   catalog : a1.b1     ---- 2
   *   schema  : a1.b1.c   ---- 3
   *
   *   metalake: a2        ---- 4
   *   catalog : a2.b2     ---- 5
   *   schema  : a2.b2.c   ---- 6
   *   schema  : a2.b2.c1  ---- 7
   *
   *   metalake: a1        ---- 1 means the name of metalake is a1 and the corresponding id is 1
   * </pre>
   *
   * Then we will store the name to id mapping as follows
   *
   * <pre>
   *  a1         -- 1
   * 	1/b1       -- 2
   * 	1/2/c      -- 3
   * 	a2         -- 4
   * 	4/b2       -- 5
   * 	4/5/c      -- 6
   * 	4/5/c1     -- 7
   * </pre>
   *
   * @param nameIdentifier name of a specific entity
   * @return key that maps to the id of a specific entity. See above, The key maybe like '4/5/c1'
   * @throws IOException if error occurs
   */
  public String generateKeyForMapping(NameIdentifier nameIdentifier) throws IOException {
    if (nameIdentifier.namespace().isEmpty()) {
      return nameIdentifier.name();
    }
    Namespace namespace = nameIdentifier.namespace();
    String name = nameIdentifier.name();

    long[] ids = new long[namespace.length()];
    for (int i = 0; i < ids.length; i++) {
      ids[i] =
          nameMappingService.getIdByName(
              concatIdAndName(ArrayUtils.subarray(ids, 0, i), namespace.level(i)));
    }

    return concatIdAndName(ids, name);
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, EntityType entityType, Class<E> e)
      throws NoSuchEntityException, IOException {
    byte[] value =
        executeInTransaction(
            () -> {
              byte[] key = entityKeyEncoder.encode(ident, entityType, true);
              if (key == null) {
                throw new NoSuchEntityException(NO_SUCH_ENTITY_MSG, ident.toString());
              }
              return transactionalKvBackend.get(key);
            });
    if (value == null) {
      throw new NoSuchEntityException(NO_SUCH_ENTITY_MSG, ident.toString());
    }
    return serDe.deserialize(value, e);
  }

  /**
   * Get key prefix of all sub-entities under a specific entities. For example, as a metalake will
   * start with `ml_{metalake_id}`, sub-entities under this metalake will have the prefix
   *
   * <pre>
   *   catalog: ca_{metalake_id}
   *   schema:  sc_{metalake_id}
   *   table:   ta_{metalake_id}
   * </pre>
   *
   * Why the sub-entities under this metalake start with those prefixes, please see {@link
   * KvEntityStore} java class doc.
   *
   * @param ident identifier of an entity
   * @param type type of entity
   * @return list of sub-entities prefix
   * @throws IOException if error occurs
   */
  private List<byte[]> getSubEntitiesPrefix(NameIdentifier ident, EntityType type)
      throws IOException {
    List<byte[]> prefixes = Lists.newArrayList();
    byte[] encode = entityKeyEncoder.encode(ident, type, true);
    switch (type) {
      case METALAKE:
        prefixes.add(replacePrefixTypeInfo(encode, CATALOG.getShortName()));
        prefixes.add(replacePrefixTypeInfo(encode, SCHEMA.getShortName()));
        prefixes.add(replacePrefixTypeInfo(encode, TABLE.getShortName()));
        prefixes.add(replacePrefixTypeInfo(encode, FILESET.getShortName()));
        break;
      case CATALOG:
        prefixes.add(replacePrefixTypeInfo(encode, SCHEMA.getShortName()));
        prefixes.add(replacePrefixTypeInfo(encode, TABLE.getShortName()));
        prefixes.add(replacePrefixTypeInfo(encode, FILESET.getShortName()));
        break;
      case SCHEMA:
        prefixes.add(replacePrefixTypeInfo(encode, TABLE.getShortName()));
        prefixes.add(replacePrefixTypeInfo(encode, FILESET.getShortName()));
        break;
      case TABLE:
      case FILESET:
        break;
      default:
        LOG.warn("Currently unknown type: {}, please check it", type);
    }
    Collections.reverse(prefixes);
    return prefixes;
  }

  private byte[] replacePrefixTypeInfo(byte[] encode, String subTypePrefix) {
    byte[] result = new byte[encode.length];
    System.arraycopy(encode, 0, result, 0, encode.length);
    byte[] bytes = subTypePrefix.getBytes(StandardCharsets.UTF_8);
    result[0] = bytes[0];
    result[1] = bytes[1];

    return result;
  }

  @Override
  public boolean delete(NameIdentifier ident, EntityType entityType, boolean cascade)
      throws IOException {
    return executeInTransaction(
        () -> {
          if (!exists(ident, entityType)) {
            return false;
          }

          byte[] dataKey = entityKeyEncoder.encode(ident, entityType, true);
          List<byte[]> subEntityPrefix = getSubEntitiesPrefix(ident, entityType);
          if (subEntityPrefix.isEmpty()) {
            // has no sub-entities
            return transactionalKvBackend.delete(dataKey);
          }

          byte[] directChild = Iterables.getLast(subEntityPrefix);
          byte[] endKey = Bytes.increment(Bytes.wrap(directChild)).get();
          List<Pair<byte[], byte[]>> kvs =
              transactionalKvBackend.scan(
                  new KvRange.KvRangeBuilder()
                      .start(directChild)
                      .end(endKey)
                      .startInclusive(true)
                      .endInclusive(false)
                      .limit(1)
                      .build());

          if (!cascade && !kvs.isEmpty()) {
            List<NameIdentifier> subEntities = Lists.newArrayListWithCapacity(kvs.size());
            for (Pair<byte[], byte[]> pair : kvs) {
              subEntities.add(entityKeyEncoder.decode(pair.getLeft()).getLeft());
            }

            throw new NonEmptyEntityException(
                "Entity %s has sub-entities %s, you should remove sub-entities first",
                ident, subEntities);
          }

          for (byte[] prefix : subEntityPrefix) {
            transactionalKvBackend.deleteRange(
                new KvRange.KvRangeBuilder()
                    .start(prefix)
                    .startInclusive(true)
                    .end(Bytes.increment(Bytes.wrap(prefix)).get())
                    .build());
          }

          return transactionalKvBackend.delete(dataKey);
        });
  }

  @Override
  public <R, E extends Exception> R executeInTransaction(Executable<R, E> executable)
      throws E, IOException {
    return FunctionUtils.executeInTransaction(executable, transactionalKvBackend);
  }

  @Override
  public void close() throws IOException {
    txIdGenerator.close();
    kvGarbageCollector.close();
    backend.close();
  }

  private static KvBackend createKvEntityBackend(Config config) {
    String backendName = config.get(ENTITY_KV_STORE);
    String className = KV_BACKENDS.getOrDefault(backendName, backendName);
    if (Objects.isNull(className)) {
      throw new RuntimeException("Unsupported backend type..." + backendName);
    }

    try {
      KvBackend kvBackend =
          (KvBackend) Class.forName(className).getDeclaredConstructor().newInstance();
      kvBackend.initialize(config);
      return kvBackend;
    } catch (Exception e) {
      LOGGER.error("Failed to create and initialize KvBackend by name '{}'.", backendName, e);
      throw new RuntimeException(
          "Failed to create and initialize KvBackend by name: " + backendName, e);
    }
  }

  private StorageLayoutVersion initStorageVersionInfo() {
    byte[] bytes;
    try {
      bytes = backend.get(LAYOUT_VERSION_KEY);
      if (bytes == null) {
        // If the layout version is not set, we will set it to the default version.
        backend.put(
            LAYOUT_VERSION_KEY,
            StorageLayoutVersion.V1.getVersion().getBytes(StandardCharsets.UTF_8),
            true);
        return StorageLayoutVersion.V1;
      }

      return StorageLayoutVersion.fromString(new String(bytes, StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new IllegalStateException("Failed to get/put layout version information", e);
    }
  }
}
