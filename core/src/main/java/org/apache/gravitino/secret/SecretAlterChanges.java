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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.file.FilesetChange;

/**
 * Rewrites entity alter changes that involve secrets into plain setProperty / removeProperty.
 *
 * <p>Write-through plaintexts are written as new secret versions after the whole batch is applied,
 * and the returned {@link SecretVersionChanges} must be committed or rolled back with the entity
 * alter.
 */
public final class SecretAlterChanges {

  private SecretAlterChanges() {}

  /**
   * Prepares catalog alter changes that involve secrets.
   *
   * @param secretManager secret manager
   * @param currentProperties current catalog properties (may be null)
   * @param entityId catalog entity id, or null if the catalog has none yet; then the batch cannot
   *     bind, rewrite, replace or remove a write-through secret
   * @param changes catalog changes
   * @return effective changes and the prepared write-through secret versions
   * @throws IllegalArgumentException if {@code entityId} is null and the batch touches a
   *     write-through secret
   */
  public static Pair<CatalogChange[], SecretVersionChanges> prepareCatalogChanges(
      SecretManager secretManager,
      @Nullable Map<String, String> currentProperties,
      @Nullable Long entityId,
      CatalogChange... changes) {
    Map<String, String> properties =
        currentProperties == null ? new HashMap<>() : new HashMap<>(currentProperties);
    List<CatalogChange> out = new ArrayList<>(changes.length);
    Map<String, String> plaintexts = new LinkedHashMap<>();
    for (CatalogChange change : changes) {
      if (change instanceof CatalogChange.SetSecretBinding) {
        CatalogChange.SetSecretBinding c = (CatalogChange.SetSecretBinding) change;
        long id =
            SecretPropertyUtils.requireEntityIdForWriteThrough(
                entityId, "catalog", c.getProperty());
        String urn =
            secretManager.alterSetSecretBinding(
                properties, "catalog", id, c.getProperty(), c.getBinding(), plaintexts);
        out.add(CatalogChange.setProperty(c.getProperty(), urn));
      } else if (change instanceof CatalogChange.SetSecretReference) {
        CatalogChange.SetSecretReference c = (CatalogChange.SetSecretReference) change;
        SecretPropertyUtils.checkWriteThroughHasEntityId(
            entityId, "catalog", c.getProperty(), properties.get(c.getProperty()));
        String urn =
            secretManager.alterSetSecretReference(
                properties, c.getProperty(), c.getReference(), plaintexts);
        out.add(CatalogChange.setProperty(c.getProperty(), urn));
      } else if (change instanceof CatalogChange.SetProperty) {
        CatalogChange.SetProperty c = (CatalogChange.SetProperty) change;
        String value =
            secretManager.alterSetProperty(
                properties, "catalog", entityId, c.getProperty(), c.getValue(), plaintexts);
        out.add(CatalogChange.setProperty(c.getProperty(), value));
      } else if (change instanceof CatalogChange.RemoveProperty) {
        CatalogChange.RemoveProperty c = (CatalogChange.RemoveProperty) change;
        SecretPropertyUtils.checkWriteThroughHasEntityId(
            entityId, "catalog", c.getProperty(), properties.get(c.getProperty()));
        secretManager.alterRemoveProperty(properties, c.getProperty(), plaintexts);
        out.add(change);
      } else {
        out.add(change);
      }
    }
    // Without an id the checks above let no change write or release a write-through secret.
    SecretVersionChanges prepared =
        entityId == null
            ? SecretVersionChanges.EMPTY
            : secretManager.prepareSecretVersions(
                currentProperties, properties, "catalog", entityId, plaintexts);
    return Pair.of(out.toArray(new CatalogChange[0]), prepared);
  }

  /**
   * Prepares catalog changes for a connection test without writing or deleting secret material.
   *
   * <p>Write-through bindings are validated but represented by their plaintext only in the
   * temporary catalog configuration. External references are converted to reference URNs so the
   * temporary catalog resolves them through the configured provider.
   *
   * @param secretManager secret manager
   * @param entityId catalog entity id
   * @param changes proposed catalog changes
   * @return effective changes for a temporary catalog entity
   */
  public static CatalogChange[] prepareCatalogChangesForTest(
      SecretManager secretManager, long entityId, CatalogChange... changes) {
    Preconditions.checkArgument(secretManager != null, "secretManager must not be null");
    Preconditions.checkArgument(changes != null, "changes must not be null");

    List<CatalogChange> out = new ArrayList<>(changes.length);
    for (CatalogChange change : changes) {
      if (change instanceof CatalogChange.SetSecretBinding) {
        CatalogChange.SetSecretBinding c = (CatalogChange.SetSecretBinding) change;
        String property = c.getProperty();
        SecretBinding binding = c.getBinding();
        Preconditions.checkArgument(StringUtils.isNotBlank(property), "property must not be blank");
        Preconditions.checkArgument(binding != null, "binding must not be null");
        SecretPropertyUtils.validateAlterSecretBindingPlaintext(binding.plaintext());
        secretManager.validateSecretBindingUrns("catalog", entityId, Map.of(property, binding));
        out.add(CatalogChange.setProperty(property, binding.plaintext()));
      } else if (change instanceof CatalogChange.SetSecretReference) {
        CatalogChange.SetSecretReference c = (CatalogChange.SetSecretReference) change;
        String property = c.getProperty();
        SecretReference reference = c.getReference();
        Preconditions.checkArgument(StringUtils.isNotBlank(property), "property must not be blank");
        Preconditions.checkArgument(reference != null, "reference must not be null");
        SecretUrn urn = secretManager.buildSecretReferenceUrns(Map.of(property, reference)).get(0);
        out.add(CatalogChange.setProperty(property, urn.toString()));
      } else if (change instanceof CatalogChange.SetProperty) {
        CatalogChange.SetProperty c = (CatalogChange.SetProperty) change;
        SecretPropertyUtils.validateAlterSetPropertyValue(c.getProperty(), c.getValue());
        out.add(change);
      } else if (change instanceof CatalogChange.RemoveProperty) {
        CatalogChange.RemoveProperty c = (CatalogChange.RemoveProperty) change;
        Preconditions.checkArgument(
            StringUtils.isNotBlank(c.getProperty()), "property must not be blank");
        out.add(change);
      } else {
        out.add(change);
      }
    }
    return out.toArray(new CatalogChange[0]);
  }

  /**
   * Prepares schema alter changes that involve secrets.
   *
   * @param secretManager secret manager
   * @param currentProperties current schema properties (may be null)
   * @param entityId schema entity id, or null if the schema has none yet; then the batch cannot
   *     bind, rewrite, replace or remove a write-through secret
   * @param changes schema changes
   * @return effective changes and the prepared write-through secret versions
   * @throws IllegalArgumentException if {@code entityId} is null and the batch touches a
   *     write-through secret
   */
  public static Pair<SchemaChange[], SecretVersionChanges> prepareSchemaChanges(
      SecretManager secretManager,
      @Nullable Map<String, String> currentProperties,
      @Nullable Long entityId,
      SchemaChange... changes) {
    Map<String, String> properties =
        currentProperties == null ? new HashMap<>() : new HashMap<>(currentProperties);
    List<SchemaChange> out = new ArrayList<>(changes.length);
    Map<String, String> plaintexts = new LinkedHashMap<>();
    for (SchemaChange change : changes) {
      if (change instanceof SchemaChange.SetSecretBinding) {
        SchemaChange.SetSecretBinding c = (SchemaChange.SetSecretBinding) change;
        long id =
            SecretPropertyUtils.requireEntityIdForWriteThrough(entityId, "schema", c.getProperty());
        String urn =
            secretManager.alterSetSecretBinding(
                properties, "schema", id, c.getProperty(), c.getBinding(), plaintexts);
        out.add(SchemaChange.setProperty(c.getProperty(), urn));
      } else if (change instanceof SchemaChange.SetSecretReference) {
        SchemaChange.SetSecretReference c = (SchemaChange.SetSecretReference) change;
        SecretPropertyUtils.checkWriteThroughHasEntityId(
            entityId, "schema", c.getProperty(), properties.get(c.getProperty()));
        String urn =
            secretManager.alterSetSecretReference(
                properties, c.getProperty(), c.getReference(), plaintexts);
        out.add(SchemaChange.setProperty(c.getProperty(), urn));
      } else if (change instanceof SchemaChange.SetProperty) {
        SchemaChange.SetProperty c = (SchemaChange.SetProperty) change;
        String value =
            secretManager.alterSetProperty(
                properties, "schema", entityId, c.getProperty(), c.getValue(), plaintexts);
        out.add(SchemaChange.setProperty(c.getProperty(), value));
      } else if (change instanceof SchemaChange.RemoveProperty) {
        SchemaChange.RemoveProperty c = (SchemaChange.RemoveProperty) change;
        SecretPropertyUtils.checkWriteThroughHasEntityId(
            entityId, "schema", c.getProperty(), properties.get(c.getProperty()));
        secretManager.alterRemoveProperty(properties, c.getProperty(), plaintexts);
        out.add(change);
      } else {
        out.add(change);
      }
    }
    // Without an id the checks above let no change write or release a write-through secret.
    SecretVersionChanges prepared =
        entityId == null
            ? SecretVersionChanges.EMPTY
            : secretManager.prepareSecretVersions(
                currentProperties, properties, "schema", entityId, plaintexts);
    return Pair.of(out.toArray(new SchemaChange[0]), prepared);
  }

  /**
   * Prepares fileset alter changes that involve secrets.
   *
   * @param secretManager secret manager
   * @param currentProperties current fileset properties (may be null)
   * @param entityId fileset entity id, or null if the fileset has none yet; then the batch cannot
   *     bind, rewrite, replace or remove a write-through secret
   * @param changes fileset changes
   * @return effective changes and the prepared write-through secret versions
   * @throws IllegalArgumentException if {@code entityId} is null and the batch touches a
   *     write-through secret
   */
  public static Pair<FilesetChange[], SecretVersionChanges> prepareFilesetChanges(
      SecretManager secretManager,
      @Nullable Map<String, String> currentProperties,
      @Nullable Long entityId,
      FilesetChange... changes) {
    Map<String, String> properties =
        currentProperties == null ? new HashMap<>() : new HashMap<>(currentProperties);
    List<FilesetChange> out = new ArrayList<>(changes.length);
    Map<String, String> plaintexts = new LinkedHashMap<>();
    for (FilesetChange change : changes) {
      if (change instanceof FilesetChange.SetSecretBinding) {
        FilesetChange.SetSecretBinding c = (FilesetChange.SetSecretBinding) change;
        long id =
            SecretPropertyUtils.requireEntityIdForWriteThrough(
                entityId, "fileset", c.getProperty());
        String urn =
            secretManager.alterSetSecretBinding(
                properties, "fileset", id, c.getProperty(), c.getBinding(), plaintexts);
        out.add(FilesetChange.setProperty(c.getProperty(), urn));
      } else if (change instanceof FilesetChange.SetSecretReference) {
        FilesetChange.SetSecretReference c = (FilesetChange.SetSecretReference) change;
        SecretPropertyUtils.checkWriteThroughHasEntityId(
            entityId, "fileset", c.getProperty(), properties.get(c.getProperty()));
        String urn =
            secretManager.alterSetSecretReference(
                properties, c.getProperty(), c.getReference(), plaintexts);
        out.add(FilesetChange.setProperty(c.getProperty(), urn));
      } else if (change instanceof FilesetChange.SetProperty) {
        FilesetChange.SetProperty c = (FilesetChange.SetProperty) change;
        String value =
            secretManager.alterSetProperty(
                properties, "fileset", entityId, c.getProperty(), c.getValue(), plaintexts);
        out.add(FilesetChange.setProperty(c.getProperty(), value));
      } else if (change instanceof FilesetChange.RemoveProperty) {
        FilesetChange.RemoveProperty c = (FilesetChange.RemoveProperty) change;
        SecretPropertyUtils.checkWriteThroughHasEntityId(
            entityId, "fileset", c.getProperty(), properties.get(c.getProperty()));
        secretManager.alterRemoveProperty(properties, c.getProperty(), plaintexts);
        out.add(change);
      } else {
        out.add(change);
      }
    }
    // Without an id the checks above let no change write or release a write-through secret.
    SecretVersionChanges prepared =
        entityId == null
            ? SecretVersionChanges.EMPTY
            : secretManager.prepareSecretVersions(
                currentProperties, properties, "fileset", entityId, plaintexts);
    return Pair.of(out.toArray(new FilesetChange[0]), prepared);
  }
}
