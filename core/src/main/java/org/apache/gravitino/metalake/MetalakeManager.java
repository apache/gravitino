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
package org.apache.gravitino.metalake;

import static org.apache.gravitino.Metalake.PROPERTY_IN_USE;

import com.google.common.collect.Maps;
import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeInUseException;
import org.apache.gravitino.exceptions.MetalakeNotInUseException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.exceptions.NonEmptyMetalakeException;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.Executable;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages Metalakes within the Apache Gravitino system. */
public class MetalakeManager implements MetalakeDispatcher, Closeable {

  private static final String METALAKE_DOES_NOT_EXIST_MSG = "Metalake %s does not exist";

  private static final Logger LOG = LoggerFactory.getLogger(MetalakeManager.class);

  private final EntityStore store;

  private final IdGenerator idGenerator;

  @Override
  public void close() {
    // do nothing
  }

  /**
   * Constructs a MetalakeManager instance.
   *
   * @param store The EntityStore to use for managing Metalakes.
   * @param idGenerator The IdGenerator to use for generating Metalake identifiers.
   */
  public MetalakeManager(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;

    // preload all metalakes and put them into cache, this is useful when user load schema/table
    // directly without list/get metalake first.
    BaseMetalake[] baseMetalakes = listMetalakes();
    for (BaseMetalake baseMetalake : baseMetalakes) {
      loadMetalake(baseMetalake.nameIdentifier());
    }
  }

  /**
   * Check whether the metalake is available
   *
   * @param ident The identifier of the Metalake to check.
   * @param store The EntityStore to use for managing Metalakes.
   * @throws NoSuchMetalakeException If the Metalake with the given identifier does not exist.
   * @throws MetalakeNotInUseException If the Metalake is not in use.
   */
  public static void checkMetalake(NameIdentifier ident, EntityStore store)
      throws NoSuchMetalakeException, MetalakeNotInUseException {
    boolean metalakeInUse = metalakeInUse(store, ident);
    if (!metalakeInUse) {
      throw new MetalakeNotInUseException(
          "Metalake %s is not in use, please enable it first", ident);
    }
  }

  /**
   * Return true if the metalake is in used, false otherwise.
   *
   * @param store The EntityStore to use for managing Metalakes.
   * @param ident The identifier of the Metalake to check.
   * @return True if the metalake is in use, false otherwise.
   * @throws NoSuchMetalakeException If the Metalake with the given identifier does not exist.
   */
  public static boolean metalakeInUse(EntityStore store, NameIdentifier ident)
      throws NoSuchMetalakeException {
    try {
      BaseMetalake metalake = store.get(ident, EntityType.METALAKE, BaseMetalake.class);
      return (boolean)
          metalake.propertiesMetadata().getOrDefault(metalake.properties(), PROPERTY_IN_USE);
    } catch (NoSuchEntityException e) {
      LOG.warn("Metalake {} does not exist", ident, e);
      throw new NoSuchMetalakeException(METALAKE_DOES_NOT_EXIST_MSG, ident);

    } catch (IOException e) {
      LOG.error("Failed to do store operation", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Lists all available Metalakes.
   *
   * @return An array of Metalake instances representing the available Metalakes.
   * @throws RuntimeException If listing Metalakes encounters storage issues.
   */
  @Override
  public BaseMetalake[] listMetalakes() {
    try {
      List<BaseMetalake> metalakes =
          TreeLockUtils.doWithRootTreeLock(
              LockType.READ,
              () -> store.list(Namespace.empty(), BaseMetalake.class, EntityType.METALAKE));

      return metalakes.stream()
          .map(this::newMetalakeWithResolvedProperties)
          .toArray(BaseMetalake[]::new);
    } catch (IOException ioe) {
      LOG.error("Listing Metalakes failed due to storage issues.", ioe);
      throw new RuntimeException(ioe);
    }
  }
  /**
   * Loads a Metalake.
   *
   * @param ident The identifier of the Metalake to load.
   * @return The loaded Metalake instance.
   * @throws NoSuchMetalakeException If the Metalake with the given identifier does not exist.
   * @throws RuntimeException If loading the Metalake encounters storage issues.
   */
  @Override
  public BaseMetalake loadMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    return TreeLockUtils.doWithTreeLock(
        ident,
        LockType.READ,
        () -> {
          try {
            BaseMetalake baseMetalake = store.get(ident, EntityType.METALAKE, BaseMetalake.class);
            return newMetalakeWithResolvedProperties(baseMetalake);
          } catch (NoSuchEntityException e) {
            LOG.warn("Metalake {} does not exist", ident, e);
            throw new NoSuchMetalakeException(METALAKE_DOES_NOT_EXIST_MSG, ident);
          } catch (IOException ioe) {
            LOG.error("Loading Metalake {} failed due to storage issues", ident, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  private BaseMetalake newMetalakeWithResolvedProperties(BaseMetalake metalakeEntity) {
    Map<String, String> newProps =
        metalakeEntity.properties() == null
            ? new HashMap<>()
            : new HashMap<>(metalakeEntity.properties());
    newProps
        .entrySet()
        .removeIf(e -> metalakeEntity.propertiesMetadata().isHiddenProperty(e.getKey()));
    newProps.putIfAbsent(
        PROPERTY_IN_USE,
        metalakeEntity.propertiesMetadata().getDefaultValue(PROPERTY_IN_USE).toString());

    return BaseMetalake.builder()
        .withId(metalakeEntity.id())
        .withName(metalakeEntity.name())
        .withComment(metalakeEntity.comment())
        .withProperties(newProps)
        .withVersion(metalakeEntity.getVersion())
        .withAuditInfo(metalakeEntity.auditInfo())
        .build();
  }

  /**
   * Creates a new Metalake.
   *
   * @param ident The identifier of the new Metalake.
   * @param comment A comment or description for the Metalake.
   * @param properties Additional properties for the Metalake.
   * @return The created Metalake instance.
   * @throws MetalakeAlreadyExistsException If a Metalake with the same identifier already exists.
   * @throws RuntimeException If creating the Metalake encounters storage issues.
   */
  @Override
  public BaseMetalake createMetalake(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws MetalakeAlreadyExistsException {
    long uid = idGenerator.nextId();

    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(uid)
            .withName(ident.name())
            .withComment(comment)
            .withProperties(properties)
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    return TreeLockUtils.doWithRootTreeLock(
        LockType.WRITE,
        () -> {
          try {
            store.put(metalake, false /* overwritten */);
            return metalake;
          } catch (EntityAlreadyExistsException | AlreadyExistsException e) {
            LOG.warn("Metalake {} already exists", ident, e);
            throw new MetalakeAlreadyExistsException("Metalake %s already exists", ident);
          } catch (IOException ioe) {
            LOG.error("Loading Metalake {} failed due to storage issues", ident, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  /**
   * Alters a Metalake by applying specified changes.
   *
   * @param ident The identifier of the Metalake to be altered.
   * @param changes The array of MetalakeChange objects representing the changes to apply.
   * @return The altered Metalake instance after applying the changes.
   * @throws NoSuchMetalakeException If the Metalake with the given identifier does not exist.
   * @throws IllegalArgumentException If the provided changes are invalid.
   * @throws RuntimeException If altering the Metalake encounters storage issues.
   */
  @Override
  public BaseMetalake alterMetalake(NameIdentifier ident, MetalakeChange... changes)
      throws NoSuchMetalakeException, IllegalArgumentException {
    Executable<BaseMetalake, RuntimeException> exceptionExecutable =
        () -> {
          try {
            if (!metalakeInUse(store, ident)) {
              throw new MetalakeNotInUseException(
                  "Metalake %s is not in use, please enable it first", ident);
            }

            return store.update(
                ident,
                BaseMetalake.class,
                EntityType.METALAKE,
                metalake -> {
                  BaseMetalake.Builder builder = newMetalakeBuilder(metalake);
                  Map<String, String> newProps =
                      metalake.properties() == null
                          ? Maps.newHashMap()
                          : Maps.newHashMap(metalake.properties());
                  builder = updateEntity(builder, newProps, changes);

                  return builder.build();
                });
          } catch (NoSuchEntityException ne) {
            LOG.warn("Metalake {} does not exist", ident, ne);
            throw new NoSuchMetalakeException(METALAKE_DOES_NOT_EXIST_MSG, ident);

          } catch (IllegalArgumentException iae) {
            LOG.warn("Altering Metalake {} failed due to invalid changes", ident, iae);
            throw iae;

          } catch (IOException ioe) {
            LOG.error("Loading Metalake {} failed due to storage issues", ident, ioe);
            throw new RuntimeException(ioe);
          }
        };

    boolean containsRenameMetalake =
        Arrays.stream(changes).anyMatch(c -> c instanceof MetalakeChange.RenameMetalake);
    if (containsRenameMetalake) {
      return TreeLockUtils.doWithRootTreeLock(LockType.WRITE, exceptionExecutable);
    }

    return TreeLockUtils.doWithTreeLock(ident, LockType.WRITE, exceptionExecutable);
  }

  @Override
  public boolean dropMetalake(NameIdentifier ident, boolean force)
      throws NonEmptyEntityException, MetalakeInUseException {
    return TreeLockUtils.doWithRootTreeLock(
        LockType.WRITE,
        () -> {
          try {
            boolean inUse = metalakeInUse(store, ident);
            if (inUse && !force) {
              throw new MetalakeInUseException(
                  "Metalake %s is in use, please disable it first or use force option", ident);
            }

            List<CatalogEntity> catalogEntities =
                store.list(Namespace.of(ident.name()), CatalogEntity.class, EntityType.CATALOG);
            if (!catalogEntities.isEmpty() && !force) {
              throw new NonEmptyMetalakeException(
                  "Metalake %s has catalogs, please drop them first or use force option", ident);
            }

            return store.delete(ident, EntityType.METALAKE, true);
          } catch (NoSuchMetalakeException e) {
            return false;

          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public void enableMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    TreeLockUtils.doWithTreeLock(
        ident,
        LockType.WRITE,
        () -> {
          try {
            boolean inUse = metalakeInUse(store, ident);
            if (!inUse) {
              store.update(
                  ident,
                  BaseMetalake.class,
                  EntityType.METALAKE,
                  metalake -> {
                    BaseMetalake.Builder builder = newMetalakeBuilder(metalake);

                    Map<String, String> newProps =
                        metalake.properties() == null
                            ? Maps.newHashMap()
                            : Maps.newHashMap(metalake.properties());
                    newProps.put(PROPERTY_IN_USE, "true");
                    builder.withProperties(newProps);

                    return builder.build();
                  });
            }

            return null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public void disableMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    TreeLockUtils.doWithTreeLock(
        ident,
        LockType.WRITE,
        () -> {
          try {
            boolean inUse = metalakeInUse(store, ident);
            if (inUse) {
              store.update(
                  ident,
                  BaseMetalake.class,
                  EntityType.METALAKE,
                  metalake -> {
                    BaseMetalake.Builder builder = newMetalakeBuilder(metalake);

                    Map<String, String> newProps =
                        metalake.properties() == null
                            ? Maps.newHashMap()
                            : Maps.newHashMap(metalake.properties());
                    newProps.put(PROPERTY_IN_USE, "false");
                    builder.withProperties(newProps);

                    return builder.build();
                  });
            }
            return null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private BaseMetalake.Builder newMetalakeBuilder(BaseMetalake metalake) {
    BaseMetalake.Builder builder =
        BaseMetalake.builder()
            .withId(metalake.id())
            .withName(metalake.name())
            .withComment(metalake.comment())
            .withProperties(metalake.properties())
            .withVersion(metalake.getVersion());

    AuditInfo newInfo =
        AuditInfo.builder()
            .withCreator(metalake.auditInfo().creator())
            .withCreateTime(metalake.auditInfo().createTime())
            .withLastModifier(PrincipalUtils.getCurrentUserName())
            .withLastModifiedTime(Instant.now())
            .build();
    return builder.withAuditInfo(newInfo);
  }

  /**
   * Updates an entity with the provided changes.
   *
   * @param builder The builder for the entity.
   * @param newProps The new properties to apply.
   * @param changes The changes to apply.
   * @return The updated entity builder.
   * @throws IllegalArgumentException If an unknown MetalakeChange is encountered.
   */
  private BaseMetalake.Builder updateEntity(
      BaseMetalake.Builder builder, Map<String, String> newProps, MetalakeChange... changes) {
    for (MetalakeChange change : changes) {
      if (change instanceof MetalakeChange.RenameMetalake) {
        MetalakeChange.RenameMetalake rename = (MetalakeChange.RenameMetalake) change;
        builder.withName(rename.getNewName());

      } else if (change instanceof MetalakeChange.UpdateMetalakeComment) {
        MetalakeChange.UpdateMetalakeComment comment =
            (MetalakeChange.UpdateMetalakeComment) change;
        builder.withComment(comment.getNewComment());

      } else if (change instanceof MetalakeChange.SetProperty) {
        MetalakeChange.SetProperty setProperty = (MetalakeChange.SetProperty) change;
        newProps.put(setProperty.getProperty(), setProperty.getValue());

      } else if (change instanceof MetalakeChange.RemoveProperty) {
        MetalakeChange.RemoveProperty removeProperty = (MetalakeChange.RemoveProperty) change;
        newProps.remove(removeProperty.getProperty());

      } else {
        throw new IllegalArgumentException("Unknown metalake change type: " + change);
      }
    }

    return builder.withProperties(newProps);
  }
}
