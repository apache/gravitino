/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.storage;

import com.datastrato.graviton.util.Executable;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Map;

/**
 * In memory implemention for {@link NameMappingService} Note, This class is only for test usage,
 * please do not use it in production.
 */
public class InMemoryNameMappingService implements NameMappingService {
  private Map<String, Long> nameToId = Maps.newHashMap();

  private IdGenerator idGenerator;

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
    long nextId = idGenerator.nextId();
    nameToId.put(name, nextId);
    return nextId;
  }

  @Override
  public boolean update(String name, long id) {
    Preconditions.checkState(nameToId.containsKey(name), "Name %s does not exist", name);
    nameToId.put(name, id);
    return true;
  }

  @Override
  public IdGenerator getIdGenerator() {
    return idGenerator;
  }

  @Override
  public <R, E extends Exception> R executeInTransaction(Executable<R, E> executable) throws E {
    return null;
  }
}
