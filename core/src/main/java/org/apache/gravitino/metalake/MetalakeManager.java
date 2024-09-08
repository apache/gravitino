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
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.EntityInUseException;
import org.apache.gravitino.exceptions.MetalakeAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages Metalakes within the Apache Gravitino system. */
public class MetalakeManager implements MetalakeDispatcher {

  private static final String METALAKE_DOES_NOT_EXIST_MSG = "Metalake %s does not exist";

  private static final Logger LOG = LoggerFactory.getLogger(MetalakeManager.class);

  private final EntityStore store;

  private final IdGenerator idGenerator;

  /**
   * Constructs a MetalakeManager instance.
   *
   * @param store The EntityStore to use for managing Metalakes.
   * @param idGenerator The IdGenerator to use for generating Metalake identifiers.
   */
  public MetalakeManager(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
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
      return store.list(Namespace.empty(), BaseMetalake.class, EntityType.METALAKE).stream()
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
    try {
      return store.get(ident, EntityType.METALAKE, BaseMetalake.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("Metalake {} does not exist", ident, e);
      throw new NoSuchMetalakeException(METALAKE_DOES_NOT_EXIST_MSG, ident);
    } catch (IOException ioe) {
      LOG.error("Loading Metalake {} failed due to storage issues", ident, ioe);
      throw new RuntimeException(ioe);
    }
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
    try {
      if (!inUse(ident)) {
        throw new EntityInUseException(
            "Metalake %s is not in use, please activate it first", ident);
      }

      return store.update(
          ident,
          BaseMetalake.class,
          EntityType.METALAKE,
          metalake -> {
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
                    .withLastModifier(
                        metalake.auditInfo().creator()) /*TODO: Use real user later on.  */
                    .withLastModifiedTime(Instant.now())
                    .build();
            builder.withAuditInfo(newInfo);

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
  }

  @Override
  public boolean dropMetalake(NameIdentifier ident, boolean force)
      throws NonEmptyEntityException, EntityInUseException {
    try {
      if (!store.exists(ident, EntityType.METALAKE)) {
        return false;
      }

      BaseMetalake metalake = store.get(ident, EntityType.METALAKE, BaseMetalake.class);
      boolean inUse = metalake.inUse();
      if (inUse && !force) {
        throw new EntityInUseException(
            "Metalake %s is in use, please inactivate it first or use force option", ident);
      }

      List<CatalogEntity> catalogEntities =
          store.list(Namespace.of(ident.name()), CatalogEntity.class, EntityType.CATALOG);
      if (!catalogEntities.isEmpty() && !force) {
        throw new NonEmptyEntityException(
            "Metalake %s has catalogs, please drop them first or use force option", ident);
      }

      if (inUse) {
        inactivateMetalake(ident);
      }

      return store.delete(ident, EntityType.METALAKE, true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void activateMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    try {

      boolean inUse = inUse(ident);
      if (!inUse) {
        BaseMetalake metalake = store.get(ident, EntityType.METALAKE, BaseMetalake.class);
        Map<String, String> newProps = Maps.newHashMap(metalake.properties());
        newProps.put(PROPERTY_IN_USE, "true");

        BaseMetalake.Builder builder =
            BaseMetalake.builder()
                .withId(metalake.id())
                .withName(metalake.name())
                .withComment(metalake.comment())
                .withProperties(newProps)
                .withVersion(metalake.getVersion())
                .withAuditInfo(metalake.auditInfo());

        store.put(builder.build(), true);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void inactivateMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    try {
      boolean inUse = inUse(ident);
      if (inUse) {
        BaseMetalake metalake = store.get(ident, EntityType.METALAKE, BaseMetalake.class);
        Map<String, String> newProps = Maps.newHashMap(metalake.properties());
        newProps.put(PROPERTY_IN_USE, "false");

        BaseMetalake.Builder builder =
            BaseMetalake.builder()
                .withId(metalake.id())
                .withName(metalake.name())
                .withComment(metalake.comment())
                .withProperties(newProps)
                .withVersion(metalake.getVersion())
                .withAuditInfo(metalake.auditInfo());

        store.put(builder.build(), true);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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

  private boolean inUse(NameIdentifier ident) throws IOException {
    if (!store.exists(ident, EntityType.METALAKE)) {
      throw new NoSuchMetalakeException(METALAKE_DOES_NOT_EXIST_MSG, ident);
    }

    BaseMetalake metalake = store.get(ident, EntityType.METALAKE, BaseMetalake.class);
    return (boolean) metalake.propertiesMetadata().getDefaultValue(PROPERTY_IN_USE);
  }
}
