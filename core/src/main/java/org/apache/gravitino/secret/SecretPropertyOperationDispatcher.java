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
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;

/**
 * Dispatches secrets requests for catalog, schema, and fileset metadata objects.
 *
 * <p>Loads raw entity properties (including secret URNs) from the entity store / catalog entity,
 * then resolves only secret-manager URNs via {@link SecretPropertyUtils#buildSecrets}.
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
   * Returns resolved secret-manager plaintext properties for the given metadata object.
   *
   * @param identifier The entity name identifier.
   * @param entityType The entity type (CATALOG, SCHEMA, or FILESET).
   * @return secret plaintext properties; never null
   */
  public Map<String, String> getSecrets(NameIdentifier identifier, Entity.EntityType entityType) {
    Map<String, String> rawProperties = loadRawProperties(identifier, entityType);
    return SecretPropertyUtils.buildSecrets(secretManager, rawProperties);
  }

  private Map<String, String> loadRawProperties(
      NameIdentifier identifier, Entity.EntityType entityType) {
    switch (entityType) {
      case CATALOG:
        return loadCatalogRawProperties(identifier);
      case SCHEMA:
        return loadSchemaRawProperties(identifier);
      case FILESET:
        return loadFilesetRawProperties(identifier);
      default:
        throw new NotSupportedException(
            "Doesn't support secret property operations for entity type: " + entityType);
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
}
