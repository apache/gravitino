/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.meta;

import com.datastrato.graviton.Entity.EntityIdentifer;
import com.datastrato.graviton.Entity.EntityType;
import com.datastrato.graviton.EntityAlreadyExistsException;
import com.datastrato.graviton.EntityStore;
import com.datastrato.graviton.MetalakeChange;
import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.NoSuchEntityException;
import com.datastrato.graviton.SupportsMetalakes;
import com.datastrato.graviton.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchMetalakeException;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetalakeManager implements SupportsMetalakes {

  private static final Logger LOG = LoggerFactory.getLogger(MetalakeManager.class);

  private final EntityStore store;

  public MetalakeManager(EntityStore store) {
    this.store = store;
  }

  @Override
  public BaseMetalake[] listMetalakes() {
    try {
      // Star means we want to get all the metalakes
      NameIdentifier nameIdentifier =
          NameIdentifier.of(Namespace.empty(), NameIdentifier.WILDCARD_FLAG);
      return store
          .list(EntityIdentifer.of(nameIdentifier, EntityType.METALAKE), BaseMetalake.class)
          .toArray(new BaseMetalake[0]);
    } catch (IOException ioe) {
      LOG.error("Failed to list metalakes due to storage issues", ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public BaseMetalake loadMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    try {
      return store.get(EntityIdentifer.of(ident, EntityType.METALAKE), BaseMetalake.class);
    } catch (NoSuchEntityException e) {
      LOG.warn("Metalake {} does not exist", ident, e);
      throw new NoSuchMetalakeException("Metalake " + ident + " does not exist");
    } catch (IOException ioe) {
      LOG.error("Failed to load metalake {} due to storage issues", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public BaseMetalake createMetalake(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws MetalakeAlreadyExistsException {
    BaseMetalake metalake =
        new BaseMetalake.Builder()
            .withId(1L /* TODO. use ID generator */)
            .withName(ident.name())
            .withComment(comment)
            .withProperties(properties)
            .withVersion(SchemaVersion.V_0_1)
            .withAuditInfo(
                new AuditInfo.Builder()
                    .withCreator("graviton") /*TODO. we should real user later on*/
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    try {
      store.put(metalake, false /* overwritten */);
      return metalake;
    } catch (EntityAlreadyExistsException e) {
      LOG.warn("Metalake {} already exists", ident, e);
      throw new MetalakeAlreadyExistsException("Metalake " + ident + " already exists");
    } catch (IOException ioe) {
      LOG.error("Failed to create metalake {} due to storage issues", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public BaseMetalake alterMetalake(NameIdentifier ident, MetalakeChange... changes)
      throws NoSuchMetalakeException, IllegalArgumentException {
    try {
      EntityIdentifer entityIdentifer = EntityIdentifer.of(ident, EntityType.METALAKE);
      return store.update(
          entityIdentifer,
          BaseMetalake.class,
          metalake -> {
            BaseMetalake.Builder builder =
                new BaseMetalake.Builder()
                    .withId(metalake.getId())
                    .withName(metalake.name())
                    .withComment(metalake.comment())
                    .withProperties(metalake.properties())
                    .withVersion(metalake.getVersion());

            AuditInfo newInfo =
                new AuditInfo.Builder()
                    .withCreator(metalake.auditInfo().creator())
                    .withCreateTime(metalake.auditInfo().createTime())
                    .withLastModifier(
                        metalake.auditInfo().creator()) /*TODO. we should real user later on*/
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
      throw new NoSuchMetalakeException("Metalake " + ident + " does not exist");

    } catch (IllegalArgumentException iae) {
      LOG.warn("Failed to alter metalake {} due to invalid changes", ident, iae);
      throw iae;

    } catch (IOException ioe) {
      LOG.error("Failed to alter metalake {} due to storage issues", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public boolean dropMetalake(NameIdentifier ident) {
    try {
      return store.delete(EntityIdentifer.of(ident, EntityType.METALAKE));
    } catch (IOException ioe) {
      LOG.error("Failed to delete metalake {} due to storage issues", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

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
        throw new IllegalArgumentException("Unknown metalake change: " + change);
      }
    }

    return builder.withProperties(newProps);
  }
}
