/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.storage.relational.mapper.CatalogMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import com.datastrato.gravitino.storage.relational.po.CatalogPO;
import com.datastrato.gravitino.storage.relational.utils.POConverters;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * The service class for catalog metadata. It provides the basic database operations for catalog.
 */
public class CatalogMetaService {
  private static final CatalogMetaService INSTANCE = new CatalogMetaService();

  public static CatalogMetaService getInstance() {
    return INSTANCE;
  }

  private CatalogMetaService() {}

  public CatalogEntity getCatalogByIdentifier(NameIdentifier identifier) {
    NameIdentifier.checkCatalog(identifier);
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.name();
    Long metalakeId =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(metalakeName));
    if (metalakeId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, identifier.namespace().toString());
    }
    CatalogPO catalogPO =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogMetaByMetalakeIdAndName(metalakeId, catalogName));
    if (catalogPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, identifier.toString());
    }
    return POConverters.fromCatalogPO(catalogPO, identifier.namespace());
  }

  public List<CatalogEntity> listCatalogsByNamespace(Namespace namespace) {
    Namespace.checkCatalog(namespace);
    String metalakeName = namespace.level(0);
    Long metalakeId =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(metalakeName));
    if (metalakeId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, namespace.toString());
    }
    List<CatalogPO> catalogPOS =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.listCatalogPOsByMetalakeId(metalakeId));
    return POConverters.fromCatalogPOs(catalogPOS, namespace);
  }

  public void insertCatalog(CatalogEntity catalogEntity, boolean overwrite) {
    try {
      NameIdentifier.checkCatalog(catalogEntity.nameIdentifier());
      Long metalakeId =
          SessionUtils.getWithoutCommit(
              MetalakeMetaMapper.class,
              mapper -> mapper.selectMetalakeIdMetaByName(catalogEntity.namespace().level(0)));
      if (metalakeId == null) {
        throw new NoSuchEntityException(
            NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, catalogEntity.namespace().toString());
      }
      SessionUtils.doWithCommit(
          CatalogMetaMapper.class,
          mapper -> {
            CatalogPO po = POConverters.initializeCatalogPOWithVersion(catalogEntity, metalakeId);
            if (overwrite) {
              mapper.insertCatalogMetaOnDuplicateKeyUpdate(po);
            } else {
              mapper.insertCatalogMeta(po);
            }
          });
    } catch (RuntimeException re) {
      if (re.getCause() != null
          && re.getCause().getCause() != null
          && re.getCause().getCause() instanceof SQLIntegrityConstraintViolationException) {
        // TODO We should make more fine-grained exception judgments
        // Usually throwing `SQLIntegrityConstraintViolationException` means that
        // SQL violates the constraints of `primary key` and `unique key`.
        // We simply think that the entity already exists at this time.
        throw new EntityAlreadyExistsException(
            String.format("Catalog entity: %s already exists", catalogEntity.nameIdentifier()));
      }
      throw re;
    }
  }

  public <E extends Entity & HasIdentifier> CatalogEntity updateCatalog(
      NameIdentifier identifier, Function<E, E> updater) throws IOException {
    NameIdentifier.checkCatalog(identifier);
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.name();
    Long metalakeId =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(metalakeName));
    if (metalakeId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, identifier.namespace().toString());
    }

    CatalogPO oldCatalogPO =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogMetaByMetalakeIdAndName(metalakeId, catalogName));
    if (oldCatalogPO == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, identifier.toString());
    }

    CatalogEntity oldCatalogEntity =
        POConverters.fromCatalogPO(oldCatalogPO, identifier.namespace());
    CatalogEntity newEntity = (CatalogEntity) updater.apply((E) oldCatalogEntity);
    Preconditions.checkArgument(
        Objects.equals(oldCatalogEntity.id(), newEntity.id()),
        "The updated catalog entity id: %s should be same with the catalog entity id before: %s",
        newEntity.id(),
        oldCatalogEntity.id());

    Integer updateResult =
        SessionUtils.doWithCommitAndFetchResult(
            CatalogMetaMapper.class,
            mapper ->
                mapper.updateCatalogMeta(
                    POConverters.updateCatalogPOWithVersion(oldCatalogPO, newEntity, metalakeId),
                    oldCatalogPO));

    if (updateResult > 0) {
      return newEntity;
    } else {
      throw new IOException("Failed to update the entity: " + identifier);
    }
  }

  public boolean deleteCatalog(NameIdentifier identifier, boolean cascade) {
    NameIdentifier.checkCatalog(identifier);
    String metalakeName = identifier.namespace().level(0);
    String catalogName = identifier.name();
    Long metalakeId =
        SessionUtils.getWithoutCommit(
            MetalakeMetaMapper.class, mapper -> mapper.selectMetalakeIdMetaByName(metalakeName));
    if (metalakeId == null) {
      throw new NoSuchEntityException(
          NoSuchEntityException.NO_SUCH_ENTITY_MESSAGE, identifier.namespace().toString());
    }
    Long catalogId =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class,
            mapper -> mapper.selectCatalogIdByMetalakeIdAndName(metalakeId, catalogName));
    if (catalogId != null) {
      if (cascade) {
        SessionUtils.doMultipleWithCommit(
            () ->
                SessionUtils.doWithoutCommit(
                    CatalogMetaMapper.class,
                    mapper -> mapper.softDeleteCatalogMetasByCatalogId(catalogId)),
            () -> {
              // TODO We will cascade delete the metadata of sub-resources under the catalog
            });
      } else {
        // TODO Check whether the sub-resources are empty. If the sub-resources are not empty,
        //  deletion is not allowed.
        SessionUtils.doWithCommit(
            CatalogMetaMapper.class, mapper -> mapper.softDeleteCatalogMetasByCatalogId(catalogId));
      }
    }
    return true;
  }
}
