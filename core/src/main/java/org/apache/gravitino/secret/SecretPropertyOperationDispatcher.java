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
package org.apache.gravitino.secret;

import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import javax.ws.rs.NotSupportedException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.OperationDispatcher;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchModelException;
import org.apache.gravitino.exceptions.NoSuchModelVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NoSuchTopicException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.metalake.MetalakePropertiesMetadata;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.ThrowableFunction;

/**
 * Dispatches secrets requests for metalake, catalog, schema, fileset, table, topic, view, model,
 * and model version metadata objects.
 *
 * <p>Loads raw entity properties from the entity store / catalog connector, then builds plaintext
 * secrets via {@link SecretPropertyUtils#buildSecrets} (secret-manager URNs plus sensitive-named
 * inline values). Declared {@code hidden} properties alone are not recovered; see {@link
 * SecretPropertyUtils#buildSecrets}.
 */
public class SecretPropertyOperationDispatcher extends OperationDispatcher {

  /**
   * Creates a new SecretPropertyOperationDispatcher.
   *
   * @param catalogManager The CatalogManager instance.
   * @param store The EntityStore instance.
   * @param idGenerator The IdGenerator instance.
   * @param secretManager The SecretManager instance.
   */
  public SecretPropertyOperationDispatcher(
      CatalogManager catalogManager,
      EntityStore store,
      IdGenerator idGenerator,
      SecretManager secretManager) {
    super(catalogManager, store, idGenerator, secretManager);
  }

  /**
   * Returns plaintext secret properties for the given metadata object.
   *
   * @param identifier The entity name identifier.
   * @param entityType The entity type.
   * @return secret plaintext properties; never null
   */
  public Map<String, String> getSecrets(NameIdentifier identifier, Entity.EntityType entityType) {
    RawPropertiesAndMetadata loaded = loadRawPropertiesAndMetadata(identifier, entityType);
    return SecretPropertyUtils.buildSecrets(secretManager, loaded.rawProperties, loaded.metadata);
  }

  /**
   * Loads raw properties and matching properties metadata in one catalog lease when possible.
   *
   * <p>If the catalog does not expose properties metadata for the entity type ({@link
   * UnsupportedOperationException}), uses {@link FallbackPropertiesMetadata} which still registers
   * shared base / credential / cloud entries so official non-hidden keys are not fuzzy-recovered,
   * while undeclared sensitive-named keys keep fuzzy recovery.
   */
  private RawPropertiesAndMetadata loadRawPropertiesAndMetadata(
      NameIdentifier identifier, Entity.EntityType entityType) {
    switch (entityType) {
      case METALAKE:
        return new RawPropertiesAndMetadata(
            loadMetalakeRawProperties(identifier), new MetalakePropertiesMetadata());
      case CATALOG:
        return doWithCatalog(
            identifier,
            wrapper -> {
              wrapper.catalog().checkMetalakeInUse();
              return new RawPropertiesAndMetadata(
                  wrapper.catalog().entity().getProperties(),
                  resolvePropertiesMetadata(
                      wrapper, HasPropertyMetadata::catalogPropertiesMetadata));
            },
            NoSuchCatalogException.class);
      case SCHEMA:
        return loadSchemaRawPropertiesAndMetadata(identifier);
      case FILESET:
        return loadFilesetRawPropertiesAndMetadata(identifier);
      case TABLE:
        return loadTableRawPropertiesAndMetadata(identifier);
      case TOPIC:
        return loadTopicRawPropertiesAndMetadata(identifier);
      case VIEW:
        return loadViewRawPropertiesAndMetadata(identifier);
      case MODEL:
        return loadModelRawPropertiesAndMetadata(identifier);
      case MODEL_VERSION:
        return loadModelVersionRawPropertiesAndMetadata(identifier);
      default:
        throw new NotSupportedException(
            "Doesn't support secret property operations for entity type: " + entityType);
    }
  }

  private RawPropertiesAndMetadata loadSchemaRawPropertiesAndMetadata(NameIdentifier identifier) {
    NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(identifier);
    PropertiesMetadata metadata =
        doWithCatalog(
            catalogIdent,
            wrapper -> {
              wrapper.catalog().checkMetalakeInUse();
              return resolvePropertiesMetadata(
                  wrapper, HasPropertyMetadata::schemaPropertiesMetadata);
            },
            NoSuchCatalogException.class);
    try {
      SchemaEntity entity = store.get(identifier, Entity.EntityType.SCHEMA, SchemaEntity.class);
      return new RawPropertiesAndMetadata(
          entity.properties() == null ? Map.of() : entity.properties(), metadata);
    } catch (NoSuchEntityException e) {
      throw new NoSuchSchemaException(e, "Schema %s does not exist", identifier);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load schema entity " + identifier, e);
    }
  }

  private RawPropertiesAndMetadata loadFilesetRawPropertiesAndMetadata(NameIdentifier identifier) {
    NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(identifier);
    PropertiesMetadata metadata =
        doWithCatalog(
            catalogIdent,
            wrapper -> {
              wrapper.catalog().checkMetalakeInUse();
              return resolvePropertiesMetadata(
                  wrapper, HasPropertyMetadata::filesetPropertiesMetadata);
            },
            NoSuchCatalogException.class);
    try {
      FilesetEntity entity = store.get(identifier, Entity.EntityType.FILESET, FilesetEntity.class);
      return new RawPropertiesAndMetadata(
          entity.properties() == null ? Map.of() : entity.properties(), metadata);
    } catch (NoSuchEntityException e) {
      throw new NoSuchFilesetException(e, "Fileset %s does not exist", identifier);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load fileset entity " + identifier, e);
    }
  }

  private RawPropertiesAndMetadata loadTableRawPropertiesAndMetadata(NameIdentifier identifier) {
    NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(identifier);
    return doWithCatalog(
        catalogIdent,
        wrapper -> {
          wrapper.catalog().checkMetalakeInUse();
          // Load from the connector so we get the same raw property map that EntityCombinedTable
          // masks for API responses (including Flink connector options like flink.password).
          Table table = wrapper.doWithTableOps(ops -> ops.loadTable(identifier));
          Map<String, String> raw = table.properties() == null ? Map.of() : table.properties();
          return new RawPropertiesAndMetadata(
              raw,
              resolvePropertiesMetadata(wrapper, HasPropertyMetadata::tablePropertiesMetadata));
        },
        NoSuchCatalogException.class,
        NoSuchTableException.class);
  }

  private RawPropertiesAndMetadata loadTopicRawPropertiesAndMetadata(NameIdentifier identifier) {
    NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(identifier);
    return doWithCatalog(
        catalogIdent,
        wrapper -> {
          wrapper.catalog().checkMetalakeInUse();
          Topic topic = wrapper.doWithTopicOps(ops -> ops.loadTopic(identifier));
          Map<String, String> raw = topic.properties() == null ? Map.of() : topic.properties();
          return new RawPropertiesAndMetadata(
              raw,
              resolvePropertiesMetadata(wrapper, HasPropertyMetadata::topicPropertiesMetadata));
        },
        NoSuchCatalogException.class,
        NoSuchTopicException.class);
  }

  private RawPropertiesAndMetadata loadViewRawPropertiesAndMetadata(NameIdentifier identifier) {
    NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(identifier);
    return doWithCatalog(
        catalogIdent,
        wrapper -> {
          wrapper.catalog().checkMetalakeInUse();
          View view = wrapper.doWithViewOps(ops -> ops.loadView(identifier));
          Map<String, String> raw = view.properties() == null ? Map.of() : view.properties();
          // View masking uses table properties metadata elsewhere in OperationDispatcher.
          return new RawPropertiesAndMetadata(
              raw,
              resolvePropertiesMetadata(wrapper, HasPropertyMetadata::tablePropertiesMetadata));
        },
        NoSuchCatalogException.class,
        NoSuchViewException.class);
  }

  private RawPropertiesAndMetadata loadModelRawPropertiesAndMetadata(NameIdentifier identifier) {
    NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(identifier);
    return doWithCatalog(
        catalogIdent,
        wrapper -> {
          wrapper.catalog().checkMetalakeInUse();
          Model model = wrapper.doWithModelOps(ops -> ops.getModel(identifier));
          Map<String, String> raw = model.properties() == null ? Map.of() : model.properties();
          return new RawPropertiesAndMetadata(
              raw,
              resolvePropertiesMetadata(wrapper, HasPropertyMetadata::modelPropertiesMetadata));
        },
        NoSuchCatalogException.class,
        NoSuchModelException.class);
  }

  private RawPropertiesAndMetadata loadModelVersionRawPropertiesAndMetadata(
      NameIdentifier identifier) {
    NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(identifier);
    NameIdentifier modelIdent = NameIdentifier.of(identifier.namespace().levels());
    String versionName = identifier.name();
    return doWithCatalog(
        catalogIdent,
        wrapper -> {
          wrapper.catalog().checkMetalakeInUse();
          ModelVersion modelVersion;
          try {
            int version = Integer.parseInt(versionName);
            modelVersion = wrapper.doWithModelOps(ops -> ops.getModelVersion(modelIdent, version));
          } catch (NumberFormatException e) {
            modelVersion =
                wrapper.doWithModelOps(ops -> ops.getModelVersion(modelIdent, versionName));
          }
          Map<String, String> raw =
              modelVersion.properties() == null ? Map.of() : modelVersion.properties();
          return new RawPropertiesAndMetadata(
              raw,
              resolvePropertiesMetadata(
                  wrapper, HasPropertyMetadata::modelVersionPropertiesMetadata));
        },
        NoSuchCatalogException.class,
        NoSuchModelVersionException.class);
  }

  private Map<String, String> loadMetalakeRawProperties(NameIdentifier identifier) {
    try {
      BaseMetalake entity = store.get(identifier, Entity.EntityType.METALAKE, BaseMetalake.class);
      return entity.properties() == null ? Map.of() : entity.properties();
    } catch (NoSuchEntityException e) {
      throw new NoSuchMetalakeException(e, "Metalake %s does not exist", identifier);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load metalake entity " + identifier, e);
    }
  }

  /**
   * Returns properties metadata under the catalog connector classloader, or {@link
   * FallbackPropertiesMetadata#INSTANCE} when the catalog does not support it for this entity type.
   */
  static PropertiesMetadata resolvePropertiesMetadata(
      CatalogManager.CatalogWrapper wrapper,
      ThrowableFunction<HasPropertyMetadata, PropertiesMetadata> getter) {
    try {
      return wrapper.doWithPropertiesMeta(getter);
    } catch (UnsupportedOperationException e) {
      return FallbackPropertiesMetadata.INSTANCE;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final class RawPropertiesAndMetadata {
    private final Map<String, String> rawProperties;
    private final PropertiesMetadata metadata;

    private RawPropertiesAndMetadata(
        @Nullable Map<String, String> rawProperties, PropertiesMetadata metadata) {
      this.rawProperties = rawProperties == null ? Map.of() : rawProperties;
      this.metadata = metadata;
    }
  }
}
