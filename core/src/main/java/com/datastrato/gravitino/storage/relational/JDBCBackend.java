/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational;

import static com.datastrato.gravitino.Configs.GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.UnsupportedEntityTypeException;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.meta.TopicEntity;
import com.datastrato.gravitino.storage.relational.converters.SQLExceptionConverterFactory;
import com.datastrato.gravitino.storage.relational.po.FilesetVersionPO;
import com.datastrato.gravitino.storage.relational.service.CatalogMetaService;
import com.datastrato.gravitino.storage.relational.service.FilesetMetaService;
import com.datastrato.gravitino.storage.relational.service.MetalakeMetaService;
import com.datastrato.gravitino.storage.relational.service.SchemaMetaService;
import com.datastrato.gravitino.storage.relational.service.TableMetaService;
import com.datastrato.gravitino.storage.relational.service.TopicMetaService;
import com.datastrato.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link JDBCBackend} is a jdbc implementation of {@link RelationalBackend} interface. You can use
 * a database that supports the JDBC protocol as storage. If the specified database has special SQL
 * syntax, please implement the SQL statements and methods in MyBatis Mapper separately and switch
 * according to the {@link Configs#ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY} parameter.
 */
public class JDBCBackend implements RelationalBackend {

  private static final Logger LOG = LoggerFactory.getLogger(JDBCBackend.class);

  /** Initialize the jdbc backend instance. */
  @Override
  public void initialize(Config config) {
    SqlSessionFactoryHelper.getInstance().init(config);
    SQLExceptionConverterFactory.initConverter(config);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Entity.EntityType entityType) {
    switch (entityType) {
      case METALAKE:
        return (List<E>) MetalakeMetaService.getInstance().listMetalakes();
      case CATALOG:
        return (List<E>) CatalogMetaService.getInstance().listCatalogsByNamespace(namespace);
      case SCHEMA:
        return (List<E>) SchemaMetaService.getInstance().listSchemasByNamespace(namespace);
      case TABLE:
        return (List<E>) TableMetaService.getInstance().listTablesByNamespace(namespace);
      case FILESET:
        return (List<E>) FilesetMetaService.getInstance().listFilesetsByNamespace(namespace);
      case TOPIC:
        return (List<E>) TopicMetaService.getInstance().listTopicsByNamespace(namespace);
      default:
        throw new UnsupportedEntityTypeException(
            "Unsupported entity type: %s for list operation", entityType);
    }
  }

  @Override
  public boolean exists(NameIdentifier ident, Entity.EntityType entityType) {
    try {
      Entity entity = get(ident, entityType);
      return entity != null;
    } catch (NoSuchEntityException ne) {
      return false;
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> void insert(E e, boolean overwritten)
      throws EntityAlreadyExistsException {
    if (e instanceof BaseMetalake) {
      MetalakeMetaService.getInstance().insertMetalake((BaseMetalake) e, overwritten);
    } else if (e instanceof CatalogEntity) {
      CatalogMetaService.getInstance().insertCatalog((CatalogEntity) e, overwritten);
    } else if (e instanceof SchemaEntity) {
      SchemaMetaService.getInstance().insertSchema((SchemaEntity) e, overwritten);
    } else if (e instanceof TableEntity) {
      TableMetaService.getInstance().insertTable((TableEntity) e, overwritten);
    } else if (e instanceof FilesetEntity) {
      FilesetMetaService.getInstance().insertFileset((FilesetEntity) e, overwritten);
    } else if (e instanceof TopicEntity) {
      TopicMetaService.getInstance().insertTopic((TopicEntity) e, overwritten);
    } else {
      throw new UnsupportedEntityTypeException(
          "Unsupported entity type: %s for insert operation", e.getClass());
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, AlreadyExistsException {
    switch (entityType) {
      case METALAKE:
        return (E) MetalakeMetaService.getInstance().updateMetalake(ident, updater);
      case CATALOG:
        return (E) CatalogMetaService.getInstance().updateCatalog(ident, updater);
      case SCHEMA:
        return (E) SchemaMetaService.getInstance().updateSchema(ident, updater);
      case TABLE:
        return (E) TableMetaService.getInstance().updateTable(ident, updater);
      case FILESET:
        return (E) FilesetMetaService.getInstance().updateFileset(ident, updater);
      case TOPIC:
        return (E) TopicMetaService.getInstance().updateTopic(ident, updater);
      default:
        throw new UnsupportedEntityTypeException(
            "Unsupported entity type: %s for update operation", entityType);
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType) throws NoSuchEntityException {
    switch (entityType) {
      case METALAKE:
        return (E) MetalakeMetaService.getInstance().getMetalakeByIdentifier(ident);
      case CATALOG:
        return (E) CatalogMetaService.getInstance().getCatalogByIdentifier(ident);
      case SCHEMA:
        return (E) SchemaMetaService.getInstance().getSchemaByIdentifier(ident);
      case TABLE:
        return (E) TableMetaService.getInstance().getTableByIdentifier(ident);
      case FILESET:
        return (E) FilesetMetaService.getInstance().getFilesetByIdentifier(ident);
      case TOPIC:
        return (E) TopicMetaService.getInstance().getTopicByIdentifier(ident);
      default:
        throw new UnsupportedEntityTypeException(
            "Unsupported entity type: %s for get operation", entityType);
    }
  }

  @Override
  public boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade) {
    switch (entityType) {
      case METALAKE:
        return MetalakeMetaService.getInstance().deleteMetalake(ident, cascade);
      case CATALOG:
        return CatalogMetaService.getInstance().deleteCatalog(ident, cascade);
      case SCHEMA:
        return SchemaMetaService.getInstance().deleteSchema(ident, cascade);
      case TABLE:
        return TableMetaService.getInstance().deleteTable(ident);
      case FILESET:
        return FilesetMetaService.getInstance().deleteFileset(ident);
      case TOPIC:
        return TopicMetaService.getInstance().deleteTopic(ident);
      default:
        throw new UnsupportedEntityTypeException(
            "Unsupported entity type: %s for delete operation", entityType);
    }
  }

  @Override
  public void hardDeleteLegacyData(long legacyTimeLine) {
    LOG.info(
        "Try to physically delete legacy data that has been marked deleted before {}",
        legacyTimeLine);

    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      switch (entityType) {
        case METALAKE:
          MetalakeMetaService.getInstance()
              .deleteMetalakeMetasByLegacyTimeLine(
                  legacyTimeLine, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
          break;
        case CATALOG:
          CatalogMetaService.getInstance()
              .deleteCatalogMetasByLegacyTimeLine(
                  legacyTimeLine, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
          break;
        case SCHEMA:
          SchemaMetaService.getInstance()
              .deleteSchemaMetasByLegacyTimeLine(
                  legacyTimeLine, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
          break;
        case TABLE:
          TableMetaService.getInstance()
              .deleteTableMetasByLegacyTimeLine(
                  legacyTimeLine, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
          break;
        case FILESET:
          FilesetMetaService.getInstance()
              .deleteFilesetAndVersionMetasByLegacyTimeLine(
                  legacyTimeLine, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
          break;
        case TOPIC:
          TopicMetaService.getInstance()
              .deleteTopicMetasByLegacyTimeLine(
                  legacyTimeLine, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
          break;

        case COLUMN:
        case USER:
        case GROUP:
        case AUDIT:
        case ROLE:
          continue;
          // TODO: Implement hard delete logic for these entity types.

        default:
          throw new IllegalArgumentException(
              "Unsupported entity type when collectAndRemoveLegacyData: " + entityType);
      }
    }
  }

  @Override
  public void hardDeleteOldVersionData(long versionRetentionCount) {
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      switch (entityType) {
        case METALAKE:
        case CATALOG:
        case SCHEMA:
        case TABLE:
        case COLUMN:
        case TOPIC:
        case USER:
        case GROUP:
        case AUDIT:
        case ROLE:
          // These entity types have not implemented multi-versions, so we can skip.
          continue;

        case FILESET:
          // Get the current version of all filesets.
          List<FilesetVersionPO> filesetCurVersions =
              FilesetMetaService.getInstance()
                  .getFilesetVersionPOsByRetentionCount(versionRetentionCount);

          // Delete old versions that are older than or equal to (currentVersion -
          // versionRetentionCount).
          for (FilesetVersionPO filesetVersionPO : filesetCurVersions) {
            long versionRetentionLine =
                filesetVersionPO.getVersion().longValue() - versionRetentionCount;
            int deletedCount =
                FilesetMetaService.getInstance()
                    .deleteFilesetVersionsByRetentionLine(
                        filesetVersionPO.getFilesetId(),
                        versionRetentionLine,
                        GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);

            // Log the deletion by current fileset version.
            LOG.info(
                "Physically delete filesetVersions count: {} which versions are older than or equal to"
                    + " versionRetentionLine: {}, the current FilesetVersion is: {}.",
                deletedCount,
                versionRetentionLine,
                filesetVersionPO);
          }
          break;

        default:
          throw new IllegalArgumentException(
              "Unsupported entity type when collectAndRemoveOldVersionData: " + entityType);
      }
    }
  }

  @Override
  public void close() throws IOException {
    SqlSessionFactoryHelper.getInstance().close();
  }
}
