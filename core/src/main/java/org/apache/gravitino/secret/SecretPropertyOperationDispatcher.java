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
import javax.ws.rs.NotSupportedException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.OperationDispatcher;
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
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;

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
    Map<String, String> rawProperties = loadRawProperties(identifier, entityType);
    return SecretPropertyUtils.buildSecrets(secretManager, rawProperties);
  }

  private Map<String, String> loadRawProperties(
      NameIdentifier identifier, Entity.EntityType entityType) {
    switch (entityType) {
      case METALAKE:
        return loadMetalakeRawProperties(identifier);
      case CATALOG:
        return loadCatalogRawProperties(identifier);
      case SCHEMA:
        return loadSchemaRawProperties(identifier);
      case FILESET:
        return loadFilesetRawProperties(identifier);
      case TABLE:
        return loadTableRawProperties(identifier);
      case TOPIC:
        return loadTopicRawProperties(identifier);
      case VIEW:
        return loadViewRawProperties(identifier);
      case MODEL:
        return loadModelRawProperties(identifier);
      case MODEL_VERSION:
        return loadModelVersionRawProperties(identifier);
      default:
        throw new NotSupportedException(
            "Doesn't support secret property operations for entity type: " + entityType);
    }
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

  private Map<String, String> loadCatalogRawProperties(NameIdentifier identifier) {
    return doWithCatalog(
        identifier,
        wrapper -> {
          wrapper.catalog().checkMetalakeInUse();
          return wrapper.catalog().entity().getProperties();
        },
        NoSuchCatalogException.class);
  }

  private Map<String, String> loadSchemaRawProperties(NameIdentifier identifier) {
    NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(identifier);
    doWithCatalog(
        catalogIdent,
        wrapper -> {
          wrapper.catalog().checkMetalakeInUse();
          return null;
        },
        NoSuchCatalogException.class);
    try {
      SchemaEntity entity = store.get(identifier, Entity.EntityType.SCHEMA, SchemaEntity.class);
      return entity.properties() == null ? Map.of() : entity.properties();
    } catch (NoSuchEntityException e) {
      throw new NoSuchSchemaException(e, "Schema %s does not exist", identifier);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load schema entity " + identifier, e);
    }
  }

  private Map<String, String> loadFilesetRawProperties(NameIdentifier identifier) {
    NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(identifier);
    doWithCatalog(
        catalogIdent,
        wrapper -> {
          wrapper.catalog().checkMetalakeInUse();
          return null;
        },
        NoSuchCatalogException.class);
    try {
      FilesetEntity entity = store.get(identifier, Entity.EntityType.FILESET, FilesetEntity.class);
      return entity.properties() == null ? Map.of() : entity.properties();
    } catch (NoSuchEntityException e) {
      throw new NoSuchFilesetException(e, "Fileset %s does not exist", identifier);
    } catch (IOException e) {
      throw new RuntimeException("Failed to load fileset entity " + identifier, e);
    }
  }

  private Map<String, String> loadTableRawProperties(NameIdentifier identifier) {
    NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(identifier);
    return doWithCatalog(
        catalogIdent,
        wrapper -> {
          wrapper.catalog().checkMetalakeInUse();
          // Load from the connector so we get the same raw property map that EntityCombinedTable
          // masks for API responses (including Flink connector options like flink.password).
          Table table = wrapper.doWithTableOps(ops -> ops.loadTable(identifier));
          return table.properties() == null ? Map.of() : table.properties();
        },
        NoSuchCatalogException.class,
        NoSuchTableException.class);
  }

  private Map<String, String> loadTopicRawProperties(NameIdentifier identifier) {
    NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(identifier);
    return doWithCatalog(
        catalogIdent,
        wrapper -> {
          wrapper.catalog().checkMetalakeInUse();
          Topic topic = wrapper.doWithTopicOps(ops -> ops.loadTopic(identifier));
          return topic.properties() == null ? Map.of() : topic.properties();
        },
        NoSuchCatalogException.class,
        NoSuchTopicException.class);
  }

  private Map<String, String> loadViewRawProperties(NameIdentifier identifier) {
    NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(identifier);
    return doWithCatalog(
        catalogIdent,
        wrapper -> {
          wrapper.catalog().checkMetalakeInUse();
          View view = wrapper.doWithViewOps(ops -> ops.loadView(identifier));
          return view.properties() == null ? Map.of() : view.properties();
        },
        NoSuchCatalogException.class,
        NoSuchViewException.class);
  }

  private Map<String, String> loadModelRawProperties(NameIdentifier identifier) {
    NameIdentifier catalogIdent = NameIdentifierUtil.getCatalogIdentifier(identifier);
    return doWithCatalog(
        catalogIdent,
        wrapper -> {
          wrapper.catalog().checkMetalakeInUse();
          Model model = wrapper.doWithModelOps(ops -> ops.getModel(identifier));
          return model.properties() == null ? Map.of() : model.properties();
        },
        NoSuchCatalogException.class,
        NoSuchModelException.class);
  }

  private Map<String, String> loadModelVersionRawProperties(NameIdentifier identifier) {
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
          return modelVersion.properties() == null ? Map.of() : modelVersion.properties();
        },
        NoSuchCatalogException.class,
        NoSuchModelVersionException.class);
  }
}
