package com.datastrato.graviton.store.memory;

import com.datastrato.graviton.Config;
import com.datastrato.graviton.Entity;
import com.datastrato.graviton.EntityAlreadyExistsException;
import com.datastrato.graviton.EntitySerDe;
import com.datastrato.graviton.EntityStore;
import com.datastrato.graviton.HasIdentifier;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.NoSuchEntityException;
import com.datastrato.graviton.util.Executable;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class InMemoryEntityStore implements EntityStore {
  private final Map<NameIdentifier, Entity> entityMap;

  private EntitySerDe serde;

  private final Lock lock;

  public InMemoryEntityStore() {
    this.entityMap = Maps.newConcurrentMap();
    this.lock = new ReentrantLock();
  }

  @Override
  public void initialize(Config config) throws RuntimeException {}

  @Override
  public void setSerDe(EntitySerDe entitySerDe) {
    this.serde = entitySerDe;
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(Namespace namespace, Class<E> type)
      throws IOException {
    return entityMap.entrySet().stream()
        .filter(e -> e.getKey().namespace().equals(namespace))
        .map(entry -> (E) entry.getValue())
        .collect(Collectors.toList());
  }

  @Override
  public boolean exists(NameIdentifier ident) throws IOException {
    return entityMap.containsKey(ident);
  }

  @Override
  public <E extends Entity & HasIdentifier> void put(NameIdentifier ident, E e, boolean overwritten)
      throws IOException, EntityAlreadyExistsException {
    if (overwritten) {
      entityMap.put(ident, e);
    } else {
      executeInTransaction(
          () -> {
            if (exists(ident)) {
              throw new EntityAlreadyExistsException("Entity " + ident + " already exists");
            }
            entityMap.put(ident, e);
            return null;
          });
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(NameIdentifier ident, Class<E> type)
      throws NoSuchEntityException {
    E e = (E) entityMap.get(ident);
    if (e == null) {
      throw new NoSuchEntityException("Entity " + ident + " does not exist");
    }

    return e;
  }

  @Override
  public boolean delete(NameIdentifier ident) throws IOException {
    Entity prev = entityMap.remove(ident);
    return prev != null;
  }

  @Override
  public <R> R executeInTransaction(Executable<R> executable) throws IOException {
    try {
      lock.lock();
      return executable.execute();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() throws IOException {
    entityMap.clear();
  }
}
