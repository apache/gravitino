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
package org.apache.gravitino.catalog;

import static org.apache.gravitino.Catalog.PROPERTY_IN_USE;
import static org.apache.gravitino.Catalog.Type.FILESET;
import static org.apache.gravitino.StringIdentifier.DUMMY_ID;
import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForAlter;
import static org.apache.gravitino.catalog.PropertiesMetadataHelpers.validatePropertyForCreate;
import static org.apache.gravitino.connector.BaseCatalogPropertiesMetadata.BASIC_CATALOG_PROPERTIES_METADATA;
import static org.apache.gravitino.metalake.MetalakeManager.checkMetalake;
import static org.apache.gravitino.metalake.MetalakeManager.metalakeInUse;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.CatalogChange.RemoveProperty;
import org.apache.gravitino.CatalogChange.SetProperty;
import org.apache.gravitino.CatalogProvider;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.connector.authorization.BaseAuthorization;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.CatalogAlreadyExistsException;
import org.apache.gravitino.exceptions.CatalogInUseException;
import org.apache.gravitino.exceptions.CatalogNotInUseException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.MetalakeNotInUseException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyCatalogException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.file.FilesetCatalog;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.messaging.TopicCatalog;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.model.ModelCatalog;
import org.apache.gravitino.rel.SupportsPartitions;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.gravitino.utils.ThrowableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages the catalog instances and operations. */
public class CatalogManager implements CatalogDispatcher, Closeable {

  private static final String CATALOG_DOES_NOT_EXIST_MSG = "Catalog %s does not exist";

  private static final Logger LOG = LoggerFactory.getLogger(CatalogManager.class);

  public void checkCatalogInUse(EntityStore store, NameIdentifier ident)
      throws NoSuchMetalakeException, NoSuchCatalogException, CatalogNotInUseException,
          MetalakeNotInUseException {
    NameIdentifier metalakeIdent = NameIdentifier.of(ident.namespace().levels());
    checkMetalake(metalakeIdent, store);

    if (!getCatalogInUseValue(store, ident)) {
      throw new CatalogNotInUseException("Catalog %s is not in use, please enable it first", ident);
    }
  }

  /** Wrapper class for a catalog instance and its class loader. */
  public static class CatalogWrapper {

    private BaseCatalog catalog;
    private IsolatedClassLoader classLoader;

    public CatalogWrapper(BaseCatalog catalog, IsolatedClassLoader classLoader) {
      this.catalog = catalog;
      this.classLoader = classLoader;
    }

    public BaseCatalog catalog() {
      return catalog;
    }

    public <R> R doWithSchemaOps(ThrowableFunction<SupportsSchemas, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asSchemas() == null) {
              throw new UnsupportedOperationException("Catalog does not support schema operations");
            }
            return fn.apply(asSchemas());
          });
    }

    public <R> R doWithTableOps(ThrowableFunction<TableCatalog, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asTables() == null) {
              throw new UnsupportedOperationException("Catalog does not support table operations");
            }
            return fn.apply(asTables());
          });
    }

    public <R> R doWithFilesetOps(ThrowableFunction<FilesetCatalog, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asFilesets() == null) {
              throw new UnsupportedOperationException(
                  "Catalog does not support fileset operations");
            }
            return fn.apply(asFilesets());
          });
    }

    public <R> R doWithFilesetFileOps(ThrowableFunction<FilesetFileOps, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asFilesetFileOps() == null) {
              throw new UnsupportedOperationException(
                  "Catalog does not support fileset file operations");
            }
            return fn.apply(asFilesetFileOps());
          });
    }

    public <R> R doWithCredentialOps(ThrowableFunction<BaseCatalog, R> fn) throws Exception {
      return classLoader.withClassLoader(cl -> fn.apply(catalog));
    }

    public <R> R doWithTopicOps(ThrowableFunction<TopicCatalog, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asTopics() == null) {
              throw new UnsupportedOperationException("Catalog does not support topic operations");
            }
            return fn.apply(asTopics());
          });
    }

    public <R> R doWithModelOps(ThrowableFunction<ModelCatalog, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            if (asModels() == null) {
              throw new UnsupportedOperationException("Catalog does not support model operations");
            }
            return fn.apply(asModels());
          });
    }

    public <R> R doWithCatalogOps(ThrowableFunction<CatalogOperations, R> fn) throws Exception {
      return classLoader.withClassLoader(cl -> fn.apply(catalog.ops()));
    }

    public <R> R doWithPartitionOps(
        NameIdentifier tableIdent, ThrowableFunction<SupportsPartitions, R> fn) throws Exception {
      return classLoader.withClassLoader(
          cl -> {
            Preconditions.checkArgument(
                asTables() != null, "Catalog does not support table operations");
            Table table = asTables().loadTable(tableIdent);
            SupportsPartitions partitionOps = table.supportPartitions();
            Preconditions.checkArgument(
                partitionOps != null, "Table does not support partition operations");
            return fn.apply(partitionOps);
          });
    }

    public <R> R doWithPropertiesMeta(ThrowableFunction<HasPropertyMetadata, R> fn)
        throws Exception {
      return classLoader.withClassLoader(cl -> fn.apply(catalog));
    }

    public Capability capabilities() throws Exception {
      return classLoader.withClassLoader(cl -> catalog.capability());
    }

    public void close() {
      try {
        classLoader.withClassLoader(
            cl -> {
              if (catalog != null) {
                catalog.close();
              }
              catalog = null;
              return null;
            });
      } catch (Exception e) {
        LOG.warn("Failed to close catalog", e);
      }

      classLoader.close();
    }

    private SupportsSchemas asSchemas() {
      return catalog.ops() instanceof SupportsSchemas ? (SupportsSchemas) catalog.ops() : null;
    }

    private TableCatalog asTables() {
      return catalog.ops() instanceof TableCatalog ? (TableCatalog) catalog.ops() : null;
    }

    private FilesetCatalog asFilesets() {
      return catalog.ops() instanceof FilesetCatalog ? (FilesetCatalog) catalog.ops() : null;
    }

    private FilesetFileOps asFilesetFileOps() {
      return catalog.ops() instanceof FilesetFileOps ? (FilesetFileOps) catalog.ops() : null;
    }

    private TopicCatalog asTopics() {
      return catalog.ops() instanceof TopicCatalog ? (TopicCatalog) catalog.ops() : null;
    }

    private ModelCatalog asModels() {
      return catalog.ops() instanceof ModelCatalog ? (ModelCatalog) catalog.ops() : null;
    }
  }

  private final Config config;

  @Getter private final Cache<NameIdentifier, CatalogWrapper> catalogCache;

  private final EntityStore store;

  private final IdGenerator idGenerator;

  /**
   * Constructs a CatalogManager instance.
   *
   * @param config The configuration for the manager.
   * @param store The entity store to use.
   * @param idGenerator The id generator to use.
   */
  public CatalogManager(Config config, EntityStore store, IdGenerator idGenerator) {
    this.config = config;
    this.store = store;
    this.idGenerator = idGenerator;

    long cacheEvictionIntervalInMs = config.get(Configs.CATALOG_CACHE_EVICTION_INTERVAL_MS);
    this.catalogCache =
        Caffeine.newBuilder()
            .expireAfterAccess(cacheEvictionIntervalInMs, TimeUnit.MILLISECONDS)
            .removalListener(
                (k, v, c) -> {
                  LOG.info("Closing catalog {}.", k);
                  ((CatalogWrapper) v).close();
                })
            .scheduler(
                Scheduler.forScheduledExecutorService(
                    new ScheduledThreadPoolExecutor(
                        1,
                        new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("catalog-cleaner-%d")
                            .build())))
            .build();
  }

  /**
   * Closes the CatalogManager and releases any resources associated with it. This method
   * invalidates all cached catalog instances and clears the cache.
   */
  @Override
  public void close() {
    catalogCache.invalidateAll();
  }

  /**
   * Lists the catalogs within the specified namespace.
   *
   * @param namespace The namespace for which to list catalogs.
   * @return An array of NameIdentifier objects representing the catalogs.
   * @throws NoSuchMetalakeException If the specified metalake does not exist.
   */
  @Override
  public NameIdentifier[] listCatalogs(Namespace namespace) throws NoSuchMetalakeException {
    NameIdentifier metalakeIdent = NameIdentifier.of(namespace.levels());

    return TreeLockUtils.doWithTreeLock(
        metalakeIdent,
        LockType.READ,
        () -> {
          checkMetalake(metalakeIdent, store);
          try {
            return store.list(namespace, CatalogEntity.class, EntityType.CATALOG).stream()
                .map(entity -> NameIdentifier.of(namespace, entity.name()))
                .toArray(NameIdentifier[]::new);

          } catch (IOException ioe) {
            LOG.error("Failed to list catalogs in metalake {}", metalakeIdent, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public Catalog[] listCatalogsInfo(Namespace namespace) throws NoSuchMetalakeException {
    NameIdentifier metalakeIdent = NameIdentifier.of(namespace.levels());
    try {
      List<CatalogEntity> catalogEntities =
          TreeLockUtils.doWithTreeLock(
              metalakeIdent,
              LockType.READ,
              () -> {
                checkMetalake(metalakeIdent, store);
                return store.list(namespace, CatalogEntity.class, EntityType.CATALOG);
              });
      return catalogEntities.stream()
          // The old fileset catalog's provider is "hadoop", whereas the new fileset catalog's
          // provider is "fileset", still using "hadoop" will lead to catalog loading issue. So
          // after reading the catalog entity, we convert it to the new fileset catalog entity.
          .map(this::convertFilesetCatalogEntity)
          .map(e -> e.toCatalogInfoWithResolvedProps(getResolvedProperties(e)))
          .toArray(Catalog[]::new);
    } catch (IOException ioe) {
      LOG.error("Failed to list catalogs in metalake {}", metalakeIdent, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Loads the catalog with the specified identifier.
   *
   * @param ident The identifier of the catalog to load.
   * @return The loaded catalog.
   * @throws NoSuchCatalogException If the specified catalog does not exist.
   */
  @Override
  public Catalog loadCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    NameIdentifier metalakeIdent = NameIdentifier.of(ident.namespace().levels());
    return TreeLockUtils.doWithTreeLock(
        ident,
        LockType.READ,
        () -> {
          checkMetalake(metalakeIdent, store);
          return loadCatalogAndWrap(ident).catalog;
        });
  }

  /**
   * Creates a new catalog with the provided details.
   *
   * @param ident The identifier of the new catalog.
   * @param type The type of the new catalog.
   * @param provider The provider of the new catalog.
   * @param comment The comment for the new catalog.
   * @param properties The properties of the new catalog.
   * @return The created catalog.
   * @throws NoSuchMetalakeException If the specified metalake does not exist.
   * @throws CatalogAlreadyExistsException If a catalog with the same identifier already exists.
   */
  @Override
  public Catalog createCatalog(
      NameIdentifier ident,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    NameIdentifier metalakeIdent = NameIdentifier.of(ident.namespace().levels());

    Map<String, String> mergedConfig = buildCatalogConf(provider, properties);
    long uid = idGenerator.nextId();
    StringIdentifier stringId = StringIdentifier.fromId(uid);
    Instant now = Instant.now();
    String creator = PrincipalUtils.getCurrentPrincipal().getName();
    CatalogEntity e =
        CatalogEntity.builder()
            .withId(uid)
            .withName(ident.name())
            .withNamespace(ident.namespace())
            .withType(type)
            .withProvider(provider)
            .withComment(comment)
            .withProperties(StringIdentifier.newPropertiesWithId(stringId, mergedConfig))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(creator)
                    .withCreateTime(now)
                    .withLastModifier(creator)
                    .withLastModifiedTime(now)
                    .build())
            .build();

    return TreeLockUtils.doWithTreeLock(
        metalakeIdent,
        LockType.WRITE,
        () -> {
          checkMetalake(metalakeIdent, store);
          boolean needClean = true;
          try {
            store.put(e, false /* overwrite */);
            CatalogWrapper wrapper =
                catalogCache.get(ident, id -> createCatalogWrapper(e, mergedConfig));

            needClean = false;
            return wrapper.catalog;

          } catch (EntityAlreadyExistsException e1) {
            needClean = false;
            LOG.warn("Catalog {} already exists", ident, e1);
            throw new CatalogAlreadyExistsException("Catalog %s already exists", ident);

          } catch (IllegalArgumentException | NoSuchMetalakeException e2) {
            throw e2;

          } catch (Exception e3) {
            catalogCache.invalidate(ident);
            LOG.error("Failed to create catalog {}", ident, e3);
            if (e3 instanceof RuntimeException) {
              throw (RuntimeException) e3;
            }
            throw new RuntimeException(e3);

          } finally {
            if (needClean) {
              // since we put the catalog entity into the store but failed to create the catalog
              // instance,
              // we need to clean up the entity stored.
              try {
                store.delete(ident, EntityType.CATALOG, true);
              } catch (IOException e4) {
                LOG.error("Failed to clean up catalog {}", ident, e4);
              }
            }
          }
        });
  }

  /**
   * Test whether a catalog can be created with the specified parameters, without actually creating
   * it.
   *
   * @param ident The identifier of the catalog to be tested.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   */
  @Override
  public void testConnection(
      NameIdentifier ident,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties) {
    NameIdentifier metalakeIdent = NameIdentifier.of(ident.namespace().levels());

    checkMetalake(metalakeIdent, store);
    try {
      if (store.exists(ident, EntityType.CATALOG)) {
        throw new CatalogAlreadyExistsException("Catalog %s already exists", ident);
      }

      Map<String, String> mergedConfig = buildCatalogConf(provider, properties);
      Instant now = Instant.now();
      String creator = PrincipalUtils.getCurrentPrincipal().getName();
      CatalogEntity dummyEntity =
          CatalogEntity.builder()
              .withId(DUMMY_ID.id())
              .withName(ident.name())
              .withNamespace(ident.namespace())
              .withType(type)
              .withProvider(provider)
              .withComment(comment)
              .withProperties(StringIdentifier.newPropertiesWithId(DUMMY_ID, mergedConfig))
              .withAuditInfo(
                  AuditInfo.builder()
                      .withCreator(creator)
                      .withCreateTime(now)
                      .withLastModifier(creator)
                      .withLastModifiedTime(now)
                      .build())
              .build();

      CatalogWrapper wrapper = createCatalogWrapper(dummyEntity, mergedConfig);
      wrapper.doWithCatalogOps(
          c -> {
            c.testConnection(ident, type, provider, comment, mergedConfig);
            return null;
          });
    } catch (GravitinoRuntimeException e) {
      throw e;
    } catch (Exception e) {
      LOG.warn("Failed to test catalog creation {}", ident, e);
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public void enableCatalog(NameIdentifier ident)
      throws NoSuchCatalogException, CatalogNotInUseException {
    NameIdentifier metalakeIdent = NameIdentifier.of(ident.namespace().levels());

    TreeLockUtils.doWithTreeLock(
        metalakeIdent,
        LockType.WRITE,
        () -> {
          checkMetalake(metalakeIdent, store);

          try {
            if (catalogInUse(store, ident)) {
              return null;
            }

            store.update(
                ident,
                CatalogEntity.class,
                EntityType.CATALOG,
                catalog -> {
                  CatalogEntity.Builder newCatalogBuilder =
                      newCatalogBuilder(ident.namespace(), catalog);

                  Map<String, String> newProps =
                      catalog.getProperties() == null
                          ? new HashMap<>()
                          : new HashMap<>(catalog.getProperties());
                  newProps.put(PROPERTY_IN_USE, "true");
                  newCatalogBuilder.withProperties(newProps);

                  return newCatalogBuilder.build();
                });
            catalogCache.invalidate(ident);
            return null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Override
  public void disableCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    NameIdentifier metalakeIdent = NameIdentifier.of(ident.namespace().levels());

    TreeLockUtils.doWithTreeLock(
        metalakeIdent,
        LockType.WRITE,
        () -> {
          checkMetalake(metalakeIdent, store);

          try {
            if (!catalogInUse(store, ident)) {
              return null;
            }
            store.update(
                ident,
                CatalogEntity.class,
                EntityType.CATALOG,
                catalog -> {
                  CatalogEntity.Builder newCatalogBuilder =
                      newCatalogBuilder(ident.namespace(), catalog);

                  Map<String, String> newProps =
                      catalog.getProperties() == null
                          ? new HashMap<>()
                          : new HashMap<>(catalog.getProperties());
                  newProps.put(PROPERTY_IN_USE, "false");
                  newCatalogBuilder.withProperties(newProps);

                  return newCatalogBuilder.build();
                });
            catalogCache.invalidate(ident);
            return null;
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Alters an existing catalog with the specified changes.
   *
   * @param ident The identifier of the catalog to alter.
   * @param changes The changes to apply to the catalog.
   * @return The altered catalog.
   * @throws NoSuchCatalogException If the specified catalog does not exist.
   * @throws IllegalArgumentException If an unsupported catalog change is provided.
   */
  @Override
  public Catalog alterCatalog(NameIdentifier ident, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    TreeLockUtils.doWithTreeLock(
        ident,
        LockType.READ,
        () -> {
          checkCatalogInUse(store, ident);

          // There could be a race issue that someone is using the catalog from cache while we are
          // updating it.

          CatalogWrapper catalogWrapper = loadCatalogAndWrap(ident);
          if (catalogWrapper == null) {
            throw new NoSuchCatalogException(CATALOG_DOES_NOT_EXIST_MSG, ident);
          }

          try {
            catalogWrapper.doWithPropertiesMeta(
                f -> {
                  Pair<Map<String, String>, Map<String, String>> alterProperty =
                      getCatalogAlterProperty(changes);
                  validatePropertyForAlter(
                      f.catalogPropertiesMetadata(),
                      alterProperty.getLeft(),
                      alterProperty.getRight());
                  return null;
                });
          } catch (IllegalArgumentException e1) {
            throw e1;
          } catch (Exception e) {
            LOG.error("Failed to alter catalog {}", ident, e);
            throw new RuntimeException(e);
          }
          return null;
        });

    boolean containsRenameCatalog =
        Arrays.stream(changes).anyMatch(c -> c instanceof CatalogChange.RenameCatalog);
    NameIdentifier nameIdentifierForLock =
        containsRenameCatalog ? NameIdentifier.of(ident.namespace().level(0)) : ident;

    return TreeLockUtils.doWithTreeLock(
        nameIdentifierForLock,
        LockType.WRITE,
        () -> {
          catalogCache.invalidate(ident);
          try {
            CatalogEntity updatedCatalog =
                store.update(
                    ident,
                    CatalogEntity.class,
                    EntityType.CATALOG,
                    catalog -> {
                      CatalogEntity.Builder newCatalogBuilder =
                          newCatalogBuilder(ident.namespace(), catalog);

                      Map<String, String> newProps =
                          catalog.getProperties() == null
                              ? new HashMap<>()
                              : new HashMap<>(catalog.getProperties());
                      newCatalogBuilder = updateEntity(newCatalogBuilder, newProps, changes);

                      return newCatalogBuilder.build();
                    });
            // The old fileset catalog's provider is "hadoop", whereas the new fileset catalog's
            // provider is "fileset", still using "hadoop" will lead to catalog loading issue. So
            // after reading the catalog entity, we convert it to the new fileset catalog entity.
            CatalogEntity convertedCatalog = convertFilesetCatalogEntity(updatedCatalog);
            return Objects.requireNonNull(
                    catalogCache.get(
                        convertedCatalog.nameIdentifier(),
                        id -> createCatalogWrapper(convertedCatalog, null)))
                .catalog;

          } catch (NoSuchEntityException ne) {
            LOG.warn("Catalog {} does not exist", ident, ne);
            throw new NoSuchCatalogException(CATALOG_DOES_NOT_EXIST_MSG, ident);

          } catch (IllegalArgumentException iae) {
            LOG.warn("Failed to alter catalog {} with unknown change", ident, iae);
            throw iae;

          } catch (IOException ioe) {
            LOG.error("Failed to alter catalog {}", ident, ioe);
            throw new RuntimeException(ioe);
          }
        });
  }

  @Override
  public boolean dropCatalog(NameIdentifier ident, boolean force)
      throws NonEmptyEntityException, CatalogInUseException {
    NameIdentifier metalakeIdent = NameIdentifier.of(ident.namespace().levels());

    return TreeLockUtils.doWithTreeLock(
        metalakeIdent,
        LockType.WRITE,
        () -> {
          checkMetalake(metalakeIdent, store);
          try {
            boolean catalogInUse = catalogInUse(store, ident);
            if (catalogInUse && !force) {
              throw new CatalogInUseException(
                  "Catalog %s is in use, please disable it first or use force option", ident);
            }

            Namespace schemaNamespace = Namespace.of(ident.namespace().level(0), ident.name());
            CatalogWrapper catalogWrapper = loadCatalogAndWrap(ident);

            List<SchemaEntity> schemaEntities =
                store.list(schemaNamespace, SchemaEntity.class, EntityType.SCHEMA);
            CatalogEntity catalogEntity = store.get(ident, EntityType.CATALOG, CatalogEntity.class);

            if (!force
                && containsUserCreatedSchemas(schemaEntities, catalogEntity, catalogWrapper)) {
              throw new NonEmptyCatalogException(
                  "Catalog %s has schemas, please drop them first or use force option", ident);
            }

            if (includeManagedEntities(catalogEntity)) {
              // code reach here in two cases:
              // 1. the catalog does not have available schemas
              // 2. the catalog has available schemas, and force is true
              // for case 1, the forEach block can drop them without any side effect
              // for case 2, the forEach block will drop all managed sub-entities
              schemaEntities.forEach(
                  schema -> {
                    try {
                      catalogWrapper.doWithSchemaOps(
                          schemaOps -> schemaOps.dropSchema(schema.nameIdentifier(), true));
                    } catch (Exception e) {
                      LOG.warn("Failed to drop schema {}", schema.nameIdentifier());
                      throw new RuntimeException(
                          "Failed to drop schema " + schema.nameIdentifier(), e);
                    }
                  });
            }
            catalogCache.invalidate(ident);
            return store.delete(ident, EntityType.CATALOG, true);

          } catch (NoSuchMetalakeException | NoSuchCatalogException ignored) {
            return false;
          } catch (GravitinoRuntimeException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * Check if the given list of schema entities contains any currently existing user-created
   * schemas.
   *
   * <p>This method determines if there are valid user-created schemas by comparing the provided
   * schema entities with the actual schemas currently existing in the external data source. It
   * excludes:
   *
   * <ul>
   *   <li>1. Automatically generated schemas (such as Kafka catalog's "default" schema or
   *       JDBC-PostgreSQL catalog's "public" schema).
   *   <li>2. Schemas that have been dropped externally but still exist in the entity store.
   * </ul>
   *
   * @param schemaEntities The list of schema entities to check.
   * @param catalogEntity The catalog entity to which the schemas belong.
   * @param catalogWrapper The catalog wrapper for the catalog.
   * @return True if the list of schema entities contains any valid user-created schemas, false
   *     otherwise.
   * @throws Exception If an error occurs while checking the schemas.
   */
  private boolean containsUserCreatedSchemas(
      List<SchemaEntity> schemaEntities, CatalogEntity catalogEntity, CatalogWrapper catalogWrapper)
      throws Exception {
    if (schemaEntities.isEmpty()) {
      return false;
    }

    if (schemaEntities.size() == 1) {
      if ("kafka".equals(catalogEntity.getProvider())) {
        return false;

      } else if ("jdbc-postgresql".equals(catalogEntity.getProvider())) {
        // PostgreSQL catalog includes the "public" schema, see
        // https://github.com/apache/gravitino/issues/2314
        return !schemaEntities.get(0).name().equals("public");
      } else if ("hive".equals(catalogEntity.getProvider())) {
        return !schemaEntities.get(0).name().equals("default");
      }
    }

    NameIdentifier[] allSchemas =
        catalogWrapper.doWithSchemaOps(
            schemaOps ->
                schemaOps.listSchemas(
                    Namespace.of(catalogEntity.namespace().level(0), catalogEntity.name())));
    if (allSchemas.length == 0) {
      return false;
    }

    Set<String> availableSchemaNames =
        Arrays.stream(allSchemas).map(NameIdentifier::name).collect(Collectors.toSet());

    // some schemas are dropped externally, but still exist in the entity store, those schemas are
    // invalid
    return schemaEntities.stream().map(SchemaEntity::name).anyMatch(availableSchemaNames::contains);
  }

  private boolean includeManagedEntities(CatalogEntity catalogEntity) {
    return catalogEntity.getType().equals(FILESET);
  }

  /**
   * Loads the catalog with the specified identifier, wraps it in a CatalogWrapper, and caches the
   * wrapper for reuse.
   *
   * @param ident The identifier of the catalog to load.
   * @return The wrapped CatalogWrapper containing the loaded catalog.
   * @throws NoSuchCatalogException If the specified catalog does not exist.
   */
  public CatalogWrapper loadCatalogAndWrap(NameIdentifier ident) throws NoSuchCatalogException {
    return catalogCache.get(ident, this::loadCatalogInternal);
  }

  private boolean catalogInUse(EntityStore store, NameIdentifier ident)
      throws NoSuchMetalakeException, NoSuchCatalogException {
    NameIdentifier metalakeIdent = NameIdentifier.of(ident.namespace().levels());
    return metalakeInUse(store, metalakeIdent) && getCatalogInUseValue(store, ident);
  }

  private boolean getCatalogInUseValue(EntityStore store, NameIdentifier catalogIdent) {
    try {
      CatalogWrapper wrapper = catalogCache.getIfPresent(catalogIdent);
      CatalogEntity catalogEntity;
      if (wrapper != null) {
        catalogEntity = wrapper.catalog.entity();
      } else {
        catalogEntity = store.get(catalogIdent, EntityType.CATALOG, CatalogEntity.class);
      }
      return (boolean)
          BASIC_CATALOG_PROPERTIES_METADATA.getOrDefault(
              catalogEntity.getProperties(), PROPERTY_IN_USE);

    } catch (NoSuchEntityException e) {
      LOG.warn("Catalog {} does not exist", catalogIdent, e);
      throw new NoSuchCatalogException(CATALOG_DOES_NOT_EXIST_MSG, catalogIdent);

    } catch (IOException e) {
      LOG.error("Failed to do store operation", e);
      throw new RuntimeException(e);
    }
  }

  private CatalogEntity.Builder newCatalogBuilder(Namespace namespace, CatalogEntity catalog) {
    CatalogEntity.Builder builder =
        CatalogEntity.builder()
            .withId(catalog.id())
            .withName(catalog.name())
            .withNamespace(namespace)
            .withType(catalog.getType())
            .withProvider(catalog.getProvider())
            .withComment(catalog.getComment());

    AuditInfo newInfo =
        AuditInfo.builder()
            .withCreator(catalog.auditInfo().creator())
            .withCreateTime(catalog.auditInfo().createTime())
            .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
            .withLastModifiedTime(Instant.now())
            .build();
    return builder.withAuditInfo(newInfo);
  }

  private Map<String, String> buildCatalogConf(String provider, Map<String, String> properties) {
    Map<String, String> newProperties = Optional.ofNullable(properties).orElse(Maps.newHashMap());
    // load catalog-related configuration from catalog-specific configuration file
    Map<String, String> catalogSpecificConfig = loadCatalogSpecificConfig(newProperties, provider);
    return mergeConf(newProperties, catalogSpecificConfig);
  }

  private Pair<Map<String, String>, Map<String, String>> getCatalogAlterProperty(
      CatalogChange... catalogChanges) {
    Map<String, String> upserts = Maps.newHashMap();
    Map<String, String> deletes = Maps.newHashMap();

    Arrays.stream(catalogChanges)
        .forEach(
            catalogChange -> {
              if (catalogChange instanceof SetProperty) {
                SetProperty setProperty = (SetProperty) catalogChange;
                upserts.put(setProperty.getProperty(), setProperty.getValue());
              } else if (catalogChange instanceof RemoveProperty) {
                RemoveProperty removeProperty = (RemoveProperty) catalogChange;
                deletes.put(removeProperty.getProperty(), removeProperty.getProperty());
              }
            });

    return Pair.of(upserts, deletes);
  }

  private CatalogWrapper loadCatalogInternal(NameIdentifier ident) throws NoSuchCatalogException {
    try {
      CatalogEntity entity = store.get(ident, EntityType.CATALOG, CatalogEntity.class);
      // The old fileset catalog's provider is "hadoop", whereas the new fileset catalog's
      // provider is "fileset", still using "hadoop" will lead to catalog loading issue. So
      // after reading the catalog entity, we convert it to the new fileset catalog entity.
      CatalogEntity convertedEntity = convertFilesetCatalogEntity(entity);
      return createCatalogWrapper(convertedEntity, null);

    } catch (NoSuchEntityException ne) {
      LOG.warn("Catalog {} does not exist", ident, ne);
      throw new NoSuchCatalogException(CATALOG_DOES_NOT_EXIST_MSG, ident);

    } catch (IOException ioe) {
      LOG.error("Failed to load catalog {}", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Create a catalog wrapper from the catalog entity and validate the given properties for
   * creation. The properties can be null if it is not needed to validate.
   *
   * @param entity The catalog entity.
   * @param propsToValidate The properties to validate.
   * @return The created catalog wrapper.
   */
  private CatalogWrapper createCatalogWrapper(
      CatalogEntity entity, @Nullable Map<String, String> propsToValidate) {
    Map<String, String> conf = entity.getProperties();
    String provider = entity.getProvider();

    IsolatedClassLoader classLoader = createClassLoader(provider, conf);
    BaseCatalog<?> catalog = createBaseCatalog(classLoader, entity);

    CatalogWrapper wrapper = new CatalogWrapper(catalog, classLoader);
    // Validate catalog properties and initialize the config
    classLoader.withClassLoader(
        cl -> {
          validatePropertyForCreate(catalog.catalogPropertiesMetadata(), propsToValidate);

          // Call wrapper.catalog.properties() to make BaseCatalog#properties in IsolatedClassLoader
          // not null. Why do we do this? Because wrapper.catalog.properties() needs to be called in
          // the IsolatedClassLoader, as it needs to load the specific catalog class
          // such as HiveCatalog or similar. To simplify, we will preload the value of properties
          // so that AppClassLoader can get the value of properties.
          wrapper.catalog.properties();
          wrapper.catalog.capability();
          return null;
        },
        IllegalArgumentException.class);

    return wrapper;
  }

  /**
   * Get the resolved properties (filter out the hidden properties and add some required default
   * properties) of the catalog entity.
   *
   * @param entity The catalog entity.
   * @return The resolved properties.
   */
  private Map<String, String> getResolvedProperties(CatalogEntity entity) {
    Map<String, String> conf = entity.getProperties();
    String provider = entity.getProvider();

    try (IsolatedClassLoader classLoader = createClassLoader(provider, conf)) {
      BaseCatalog<?> catalog = createBaseCatalog(classLoader, entity);
      return classLoader.withClassLoader(cl -> catalog.properties(), RuntimeException.class);
    }
  }

  private Set<String> getHiddenPropertyNames(CatalogEntity entity) {
    Map<String, String> conf = entity.getProperties();
    String provider = entity.getProvider();

    try (IsolatedClassLoader classLoader = createClassLoader(provider, conf)) {
      BaseCatalog<?> catalog = createBaseCatalog(classLoader, entity);
      return classLoader.withClassLoader(
          cl ->
              catalog.catalogPropertiesMetadata().propertyEntries().values().stream()
                  .filter(PropertyEntry::isHidden)
                  .map(PropertyEntry::getName)
                  .collect(Collectors.toSet()),
          RuntimeException.class);
    }
  }

  private BaseCatalog<?> createBaseCatalog(IsolatedClassLoader classLoader, CatalogEntity entity) {
    // Load Catalog class instance
    BaseCatalog<?> catalog = createCatalogInstance(classLoader, entity.getProvider());
    catalog.withCatalogConf(entity.getProperties()).withCatalogEntity(entity);
    catalog.initAuthorizationPluginInstance(classLoader);
    return catalog;
  }

  private IsolatedClassLoader createClassLoader(String provider, Map<String, String> conf) {
    if (config.get(Configs.CATALOG_LOAD_ISOLATED)) {
      String catalogPkgPath = buildPkgPath(conf, provider);
      String catalogConfPath = buildConfPath(conf, provider);
      ArrayList<String> libAndResourcesPaths = Lists.newArrayList(catalogPkgPath, catalogConfPath);
      BaseAuthorization.buildAuthorizationPkgPath(conf).ifPresent(libAndResourcesPaths::add);
      return IsolatedClassLoader.buildClassLoader(libAndResourcesPaths);
    } else {
      // This will use the current class loader, it is mainly used for test.
      return new IsolatedClassLoader(
          Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }
  }

  private BaseCatalog<?> createCatalogInstance(IsolatedClassLoader classLoader, String provider) {
    BaseCatalog<?> catalog;
    try {
      catalog =
          classLoader.withClassLoader(
              cl -> {
                try {
                  Class<? extends CatalogProvider> providerClz =
                      lookupCatalogProvider(provider, cl);
                  return (BaseCatalog) providerClz.getDeclaredConstructor().newInstance();
                } catch (Exception e) {
                  LOG.error("Failed to load catalog with provider: {}", provider, e);
                  throw new RuntimeException(e);
                }
              });
    } catch (Exception e) {
      LOG.error("Failed to load catalog with class loader", e);
      throw new RuntimeException(e);
    }

    if (catalog == null) {
      throw new RuntimeException("Failed to load catalog with provider: " + provider);
    }
    return catalog;
  }

  private Map<String, String> loadCatalogSpecificConfig(
      Map<String, String> properties, String provider) {
    if ("test".equals(provider)) {
      return Maps.newHashMap();
    }

    String catalogSpecificConfigFile = provider + ".conf";
    Map<String, String> catalogSpecificConfig = Maps.newHashMap();

    String fullPath =
        buildConfPath(properties, provider) + File.separator + catalogSpecificConfigFile;
    try (InputStream inputStream = FileUtils.openInputStream(new File(fullPath))) {
      Properties loadProperties = new Properties();
      loadProperties.load(inputStream);
      loadProperties.forEach(
          (key, value) -> catalogSpecificConfig.put(key.toString(), value.toString()));
    } catch (Exception e) {
      LOG.warn(
          "Failed to load catalog specific configurations, file name: '{}'",
          catalogSpecificConfigFile,
          e);
    }
    return catalogSpecificConfig;
  }

  static Map<String, String> mergeConf(Map<String, String> properties, Map<String, String> conf) {
    Map<String, String> mergedConf = conf != null ? Maps.newHashMap(conf) : Maps.newHashMap();
    Optional.ofNullable(properties).ifPresent(mergedConf::putAll);
    return Collections.unmodifiableMap(mergedConf);
  }

  /**
   * Build the config path from the specific provider. Usually, the configuration file is under the
   * conf and conf and package are under the same directory.
   */
  private String buildConfPath(Map<String, String> properties, String provider) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Preconditions.checkArgument(gravitinoHome != null, "GRAVITINO_HOME not set");
    boolean testEnv = System.getenv("GRAVITINO_TEST") != null;

    String confPath;
    String pkg = properties.get(Catalog.PROPERTY_PACKAGE);
    if (pkg != null) {
      confPath = String.join(File.separator, pkg, "conf");
    } else if (testEnv) {
      confPath =
          String.join(
              File.separator,
              gravitinoHome,
              "catalogs",
              "catalog-" + provider,
              "build",
              "resources",
              "main");
    } else {
      confPath = String.join(File.separator, gravitinoHome, "catalogs", provider, "conf");
    }
    return confPath;
  }

  private String buildPkgPath(Map<String, String> conf, String provider) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    Preconditions.checkArgument(gravitinoHome != null, "GRAVITINO_HOME not set");
    boolean testEnv = System.getenv("GRAVITINO_TEST") != null;

    String pkg = conf.get(Catalog.PROPERTY_PACKAGE);
    String pkgPath;
    if (pkg != null) {
      pkgPath = String.join(File.separator, pkg, "libs");
    } else if (testEnv) {
      // In test, the catalog package is under the build directory.
      pkgPath =
          String.join(
              File.separator, gravitinoHome, "catalogs", "catalog-" + provider, "build", "libs");
    } else {
      // In real environment, the catalog package is under the catalog directory.
      pkgPath = String.join(File.separator, gravitinoHome, "catalogs", provider, "libs");
    }

    return pkgPath;
  }

  private Class<? extends CatalogProvider> lookupCatalogProvider(String provider, ClassLoader cl) {
    ServiceLoader<CatalogProvider> loader = ServiceLoader.load(CatalogProvider.class, cl);

    List<Class<? extends CatalogProvider>> providers =
        Streams.stream(loader.iterator())
            .filter(p -> p.shortName().equalsIgnoreCase(provider))
            .map(CatalogProvider::getClass)
            .collect(Collectors.toList());

    if (providers.isEmpty()) {
      throw new IllegalArgumentException("No catalog provider found for: " + provider);
    } else if (providers.size() > 1) {
      throw new IllegalArgumentException("Multiple catalog providers found for: " + provider);
    } else {
      return Iterables.getOnlyElement(providers);
    }
  }

  private CatalogEntity.Builder updateEntity(
      CatalogEntity.Builder builder, Map<String, String> newProps, CatalogChange... changes) {
    for (CatalogChange change : changes) {
      if (change instanceof CatalogChange.RenameCatalog) {
        CatalogChange.RenameCatalog rename = (CatalogChange.RenameCatalog) change;

        if (Entity.SYSTEM_CATALOG_RESERVED_NAME.equals(
            ((CatalogChange.RenameCatalog) change).getNewName())) {
          throw new IllegalArgumentException(
              "Can't rename a catalog with with reserved name `system`");
        }

        builder.withName(rename.getNewName());

      } else if (change instanceof CatalogChange.UpdateCatalogComment) {
        CatalogChange.UpdateCatalogComment updateComment =
            (CatalogChange.UpdateCatalogComment) change;
        builder.withComment(updateComment.getNewComment());

      } else if (change instanceof CatalogChange.SetProperty) {
        CatalogChange.SetProperty setProperty = (CatalogChange.SetProperty) change;
        newProps.put(setProperty.getProperty(), setProperty.getValue());

      } else if (change instanceof CatalogChange.RemoveProperty) {
        CatalogChange.RemoveProperty removeProperty = (CatalogChange.RemoveProperty) change;
        newProps.remove(removeProperty.getProperty());

      } else {
        throw new IllegalArgumentException(
            "Unsupported catalog change: " + change.getClass().getSimpleName());
      }
    }

    return builder.withProperties(newProps);
  }

  private CatalogEntity convertFilesetCatalogEntity(CatalogEntity entity) {
    if (entity.getType() != FILESET) {
      return entity;
    }

    if ("hadoop".equalsIgnoreCase(entity.getProvider())) {
      // If the provider is "hadoop", we need to convert it to a fileset catalog entity.
      // This is a special case to maintain compatibility.
      return CatalogEntity.builder()
          .withId(entity.id())
          .withName(entity.name())
          .withNamespace(entity.namespace())
          .withType(FILESET)
          .withProvider("fileset")
          .withComment(entity.getComment())
          .withProperties(entity.getProperties())
          .withAuditInfo(
              AuditInfo.builder()
                  .withCreator(entity.auditInfo().creator())
                  .withCreateTime(entity.auditInfo().createTime())
                  .withLastModifier(entity.auditInfo().lastModifier())
                  .withLastModifiedTime(entity.auditInfo().lastModifiedTime())
                  .build())
          .build();
    }

    // If the provider is not "hadoop", we assume it is already a fileset catalog entity.
    return entity;
  }
}
