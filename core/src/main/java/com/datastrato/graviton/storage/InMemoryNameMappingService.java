/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;

/**
 * In memory implemention for {@link NameMappingService}
 *
 * <p>Note, This class is only for test usage, please do not use it in production.
 */
public class InMemoryNameMappingService implements NameMappingService {
  private final Map<String, Long> nameToId = Maps.newHashMap();

  private final IdGenerator idGenerator;

  public static final InMemoryNameMappingService INSTANCE = new InMemoryNameMappingService();

  public InMemoryNameMappingService() {
    // Make configurable.
    this.idGenerator = new RandomIdGenerator();
  }

  @Override
  public synchronized Long get(String name) {
    return nameToId.get(name);
  }

  @Override
  public synchronized Long create(String name) {
    // Should handle race condition.
    long nextId = idGenerator.nextId();
    nameToId.put(name, nextId);
    return nextId;
  }

  @Override
  public synchronized boolean update(String oldName, String newName) {
    Preconditions.checkState(nameToId.containsKey(oldName), "Name %s does not exist", oldName);
    long originId = nameToId.get(oldName);
    nameToId.remove(oldName, originId);
    nameToId.put(newName, originId);
    return true;
  }

  @Override
  public IdGenerator getIdGenerator() {
    return idGenerator;
  }

  @Override
  public boolean delete(String name) throws IOException {
    return nameToId.remove(name) != null;
  }
}
