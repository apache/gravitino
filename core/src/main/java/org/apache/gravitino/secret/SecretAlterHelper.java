/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.file.FilesetChange;

/**
 * Applies secret-related alter semantics and rewrites changes into plain property updates.
 *
 * <p>{@code setSecretBinding} / {@code setSecretReference} become {@code setProperty} with a URN
 * value. {@code setProperty} on an existing secret URN rewrites material in-place. {@code
 * removeProperty} best-effort deletes write-through secrets owned by this entity.
 */
public final class SecretAlterHelper {

  private SecretAlterHelper() {}

  /**
   * Result of applying secret alter operations: rewritten changes plus write-through URNs to roll
   * back if the subsequent metadata update fails.
   *
   * @param <C> change type
   */
  public static final class Result<C> {
    private final C[] changes;
    private final List<SecretUrn> writtenUrns;

    Result(C[] changes, List<SecretUrn> writtenUrns) {
      this.changes = changes;
      this.writtenUrns = writtenUrns;
    }

    /**
     * Returns rewritten changes (secret ops converted to setProperty / removeProperty /
     * passthrough).
     *
     * @return changes
     */
    public C[] changes() {
      return changes;
    }

    /**
     * Returns newly written write-through URNs for rollback on failure.
     *
     * @return write-through URNs
     */
    public List<SecretUrn> writtenUrns() {
      return writtenUrns;
    }
  }

  /**
   * Rewrites catalog changes, writing secrets as needed.
   *
   * @param secretManager the secret manager
   * @param currentProperties current entity properties (may be null)
   * @param entityId catalog entity id
   * @param changes catalog changes
   * @return rewritten changes and written URNs
   */
  public static Result<CatalogChange> applyCatalogChanges(
      SecretManager secretManager,
      @Nullable Map<String, String> currentProperties,
      long entityId,
      CatalogChange... changes) {
    Map<String, String> working =
        currentProperties == null ? new HashMap<>() : new HashMap<>(currentProperties);
    List<CatalogChange> out = new ArrayList<>(changes.length);
    List<SecretUrn> written = new ArrayList<>();
    try {
      for (CatalogChange change : changes) {
        if (change instanceof CatalogChange.SetSecretBinding) {
          CatalogChange.SetSecretBinding c = (CatalogChange.SetSecretBinding) change;
          String urn =
              applySecretBinding(
                  secretManager,
                  working,
                  "catalog",
                  entityId,
                  c.getProperty(),
                  c.getBinding(),
                  written);
          out.add(CatalogChange.setProperty(c.getProperty(), urn));
        } else if (change instanceof CatalogChange.SetSecretReference) {
          CatalogChange.SetSecretReference c = (CatalogChange.SetSecretReference) change;
          String urn =
              applySecretReference(
                  secretManager, working, "catalog", entityId, c.getProperty(), c.getReference());
          out.add(CatalogChange.setProperty(c.getProperty(), urn));
        } else if (change instanceof CatalogChange.SetProperty) {
          CatalogChange.SetProperty c = (CatalogChange.SetProperty) change;
          String urn =
              applySetProperty(
                  secretManager, working, "catalog", entityId, c.getProperty(), c.getValue());
          out.add(CatalogChange.setProperty(c.getProperty(), urn != null ? urn : c.getValue()));
        } else if (change instanceof CatalogChange.RemoveProperty) {
          CatalogChange.RemoveProperty c = (CatalogChange.RemoveProperty) change;
          applyRemoveProperty(secretManager, working, "catalog", entityId, c.getProperty());
          out.add(change);
        } else {
          out.add(change);
        }
      }
      return new Result<>(out.toArray(new CatalogChange[0]), List.copyOf(written));
    } catch (RuntimeException e) {
      secretManager.rollbackWritten(written);
      throw e;
    }
  }

  /**
   * Rewrites schema changes, writing secrets as needed.
   *
   * @param secretManager the secret manager
   * @param currentProperties current entity properties (may be null)
   * @param entityId schema entity id
   * @param changes schema changes
   * @return rewritten changes and written URNs
   */
  public static Result<SchemaChange> applySchemaChanges(
      SecretManager secretManager,
      @Nullable Map<String, String> currentProperties,
      long entityId,
      SchemaChange... changes) {
    Map<String, String> working =
        currentProperties == null ? new HashMap<>() : new HashMap<>(currentProperties);
    List<SchemaChange> out = new ArrayList<>(changes.length);
    List<SecretUrn> written = new ArrayList<>();
    try {
      for (SchemaChange change : changes) {
        if (change instanceof SchemaChange.SetSecretBinding) {
          SchemaChange.SetSecretBinding c = (SchemaChange.SetSecretBinding) change;
          String urn =
              applySecretBinding(
                  secretManager,
                  working,
                  "schema",
                  entityId,
                  c.getProperty(),
                  c.getBinding(),
                  written);
          out.add(SchemaChange.setProperty(c.getProperty(), urn));
        } else if (change instanceof SchemaChange.SetSecretReference) {
          SchemaChange.SetSecretReference c = (SchemaChange.SetSecretReference) change;
          String urn =
              applySecretReference(
                  secretManager, working, "schema", entityId, c.getProperty(), c.getReference());
          out.add(SchemaChange.setProperty(c.getProperty(), urn));
        } else if (change instanceof SchemaChange.SetProperty) {
          SchemaChange.SetProperty c = (SchemaChange.SetProperty) change;
          String urn =
              applySetProperty(
                  secretManager, working, "schema", entityId, c.getProperty(), c.getValue());
          out.add(SchemaChange.setProperty(c.getProperty(), urn != null ? urn : c.getValue()));
        } else if (change instanceof SchemaChange.RemoveProperty) {
          SchemaChange.RemoveProperty c = (SchemaChange.RemoveProperty) change;
          applyRemoveProperty(secretManager, working, "schema", entityId, c.getProperty());
          out.add(change);
        } else {
          out.add(change);
        }
      }
      return new Result<>(out.toArray(new SchemaChange[0]), List.copyOf(written));
    } catch (RuntimeException e) {
      secretManager.rollbackWritten(written);
      throw e;
    }
  }

  /**
   * Rewrites fileset changes, writing secrets as needed.
   *
   * @param secretManager the secret manager
   * @param currentProperties current entity properties (may be null)
   * @param entityId fileset entity id
   * @param changes fileset changes
   * @return rewritten changes and written URNs
   */
  public static Result<FilesetChange> applyFilesetChanges(
      SecretManager secretManager,
      @Nullable Map<String, String> currentProperties,
      long entityId,
      FilesetChange... changes) {
    Map<String, String> working =
        currentProperties == null ? new HashMap<>() : new HashMap<>(currentProperties);
    List<FilesetChange> out = new ArrayList<>(changes.length);
    List<SecretUrn> written = new ArrayList<>();
    try {
      for (FilesetChange change : changes) {
        if (change instanceof FilesetChange.SetSecretBinding) {
          FilesetChange.SetSecretBinding c = (FilesetChange.SetSecretBinding) change;
          String urn =
              applySecretBinding(
                  secretManager,
                  working,
                  "fileset",
                  entityId,
                  c.getProperty(),
                  c.getBinding(),
                  written);
          out.add(FilesetChange.setProperty(c.getProperty(), urn));
        } else if (change instanceof FilesetChange.SetSecretReference) {
          FilesetChange.SetSecretReference c = (FilesetChange.SetSecretReference) change;
          String urn =
              applySecretReference(
                  secretManager, working, "fileset", entityId, c.getProperty(), c.getReference());
          out.add(FilesetChange.setProperty(c.getProperty(), urn));
        } else if (change instanceof FilesetChange.SetProperty) {
          FilesetChange.SetProperty c = (FilesetChange.SetProperty) change;
          String urn =
              applySetProperty(
                  secretManager, working, "fileset", entityId, c.getProperty(), c.getValue());
          out.add(FilesetChange.setProperty(c.getProperty(), urn != null ? urn : c.getValue()));
        } else if (change instanceof FilesetChange.RemoveProperty) {
          FilesetChange.RemoveProperty c = (FilesetChange.RemoveProperty) change;
          applyRemoveProperty(secretManager, working, "fileset", entityId, c.getProperty());
          out.add(change);
        } else {
          out.add(change);
        }
      }
      return new Result<>(out.toArray(new FilesetChange[0]), List.copyOf(written));
    } catch (RuntimeException e) {
      secretManager.rollbackWritten(written);
      throw e;
    }
  }

  private static String applySecretBinding(
      SecretManager secretManager,
      Map<String, String> working,
      String entityType,
      long entityId,
      String property,
      SecretBinding binding,
      List<SecretUrn> written) {
    Preconditions.checkArgument(StringUtils.isNotBlank(property), "property must not be blank");
    Preconditions.checkArgument(binding != null, "binding must not be null");
    SecretPropertyUtils.validateAlterSecretBindingPlaintext(binding.plaintext());
    Map<String, SecretBinding> bindings = ImmutableMap.of(property, binding);
    List<SecretUrn> urns = secretManager.getSecretBindingUrns(entityType, entityId, bindings);
    String newUrn = urns.get(0).toString();
    String current = working.get(property);
    if (current != null
        && !current.equals(newUrn)
        && SecretPropertyUtils.isWriteThroughForEntity(property, current, entityType, entityId)) {
      secretManager.rollbackWritten(List.of(SecretUrn.parse(current)));
    }
    secretManager.writeSecrets(bindings, urns);
    written.addAll(urns);
    SecretPropertyUtils.applySecretUrns(working, urns);
    return working.get(property);
  }

  private static String applySecretReference(
      SecretManager secretManager,
      Map<String, String> working,
      String entityType,
      long entityId,
      String property,
      SecretReference reference) {
    Preconditions.checkArgument(StringUtils.isNotBlank(property), "property must not be blank");
    Preconditions.checkArgument(reference != null, "reference must not be null");
    String current = working.get(property);
    if (SecretPropertyUtils.isWriteThroughForEntity(property, current, entityType, entityId)) {
      secretManager.rollbackWritten(List.of(SecretUrn.parse(current)));
    }
    Map<String, SecretReference> refs = ImmutableMap.of(property, reference);
    List<SecretUrn> urns = secretManager.getSecretReferenceUrns(refs);
    SecretPropertyUtils.applySecretUrns(working, urns);
    return working.get(property);
  }

  /**
   * @return URN string when rewriting an existing secret property; {@code null} for plain
   *     setProperty
   */
  @Nullable
  private static String applySetProperty(
      SecretManager secretManager,
      Map<String, String> working,
      String entityType,
      long entityId,
      String property,
      String value) {
    SecretPropertyUtils.validateAlterSetPropertyValue(property, value);
    String current = working.get(property);
    if (SecretPropertyUtils.isSecretProperty(property, current)) {
      SecretUrn currentUrn = SecretUrn.parse(current);
      SecretBinding binding = new SecretBinding(currentUrn.providerName(), value);
      if (SecretPropertyUtils.isWriteThroughForEntity(property, current, entityType, entityId)) {
        Map<String, SecretBinding> bindings = ImmutableMap.of(property, binding);
        List<SecretUrn> urns = secretManager.getSecretBindingUrns(entityType, entityId, bindings);
        secretManager.writeSecrets(bindings, urns);
        SecretPropertyUtils.applySecretUrns(working, urns);
        return working.get(property);
      }
      List<String> segments = currentUrn.identifierSegments();
      Preconditions.checkArgument(
          !segments.isEmpty(), "Secret URN must contain identifier segments: %s", currentUrn);
      Map<String, String> attributes = new HashMap<>();
      if (segments.size() == 3) {
        attributes.put(SecretConstants.ATTR_ENTITY_TYPE, segments.get(0));
        attributes.put(SecretConstants.ATTR_ENTITY_ID, segments.get(1));
        attributes.put(SecretConstants.ATTR_PROPERTY_KEY, segments.get(2));
      } else {
        attributes.put(SecretConstants.ATTR_PROPERTY_KEY, property);
      }
      SecretUrn writtenUrn =
          secretManager
              .getRegistry()
              .getProvider(currentUrn.providerName())
              .writeSecret(value, attributes);
      working.put(property, writtenUrn.toString());
      return writtenUrn.toString();
    }
    working.put(property, value);
    return null;
  }

  private static void applyRemoveProperty(
      SecretManager secretManager,
      Map<String, String> working,
      String entityType,
      long entityId,
      String property) {
    Preconditions.checkArgument(StringUtils.isNotBlank(property), "property must not be blank");
    String current = working.get(property);
    if (SecretPropertyUtils.isWriteThroughForEntity(property, current, entityType, entityId)) {
      secretManager.rollbackWritten(List.of(SecretUrn.parse(current)));
    }
    working.remove(property);
  }
}
