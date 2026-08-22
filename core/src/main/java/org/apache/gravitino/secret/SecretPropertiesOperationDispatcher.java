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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.NotSupportedException;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.CatalogManager;
import org.apache.gravitino.catalog.OperationDispatcher;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.storage.IdGenerator;

/**
 * Loads entity properties from the store and resolves secret URN values to plaintext for remote
 * connectors.
 */
public class SecretPropertiesOperationDispatcher extends OperationDispatcher {

  /**
   * Creates a dispatcher for secret-properties operations.
   *
   * @param catalogManager catalog manager
   * @param store entity store
   * @param idGenerator id generator
   * @param secretManager secret manager
   */
  public SecretPropertiesOperationDispatcher(
      CatalogManager catalogManager,
      EntityStore store,
      IdGenerator idGenerator,
      SecretManager secretManager) {
    super(catalogManager, store, idGenerator, secretManager);
  }

  /**
   * Returns only secret-backed properties for the given metadata object, with URN values replaced
   * by plaintext.
   *
   * @param identifier entity name identifier
   * @param type entity type (CATALOG, SCHEMA, or FILESET)
   * @return secret key to plaintext map; never null
   * @throws NoSuchMetadataObjectException if the entity does not exist
   * @throws NotSupportedException if the entity type is not supported
   */
  public Map<String, String> getSecretProperties(
      NameIdentifier identifier, Entity.EntityType type) {
    Map<String, String> properties = loadEntityProperties(identifier, type);
    return resolveSecretProperties(properties);
  }

  private Map<String, String> loadEntityProperties(
      NameIdentifier identifier, Entity.EntityType type) {
    try {
      switch (type) {
        case CATALOG:
          CatalogEntity catalog = store.get(identifier, type, CatalogEntity.class);
          return catalog.getProperties() == null ? Collections.emptyMap() : catalog.getProperties();
        case SCHEMA:
          SchemaEntity schema = store.get(identifier, type, SchemaEntity.class);
          return schema.properties() == null ? Collections.emptyMap() : schema.properties();
        case FILESET:
          FilesetEntity fileset = store.get(identifier, type, FilesetEntity.class);
          return fileset.properties() == null ? Collections.emptyMap() : fileset.properties();
        default:
          throw new NotSupportedException(
              "Doesn't support secret-properties operations for entity type: " + type);
      }
    } catch (NoSuchEntityException e) {
      throw new NoSuchMetadataObjectException(
          "Metadata object %s of type %s does not exist", identifier, type);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to load properties for %s of type %s", identifier, type), e);
    }
  }

  private Map<String, String> resolveSecretProperties(Map<String, String> properties) {
    if (properties == null || properties.isEmpty()) {
      return Map.of();
    }
    Map<String, String> secretOnly = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (SecretPropertyUtils.isSecretProperty(entry.getKey(), entry.getValue())) {
        secretOnly.put(entry.getKey(), entry.getValue());
      }
    }
    if (secretOnly.isEmpty()) {
      return Map.of();
    }
    return secretManager.toPlaintextProperties(secretOnly);
  }
}
