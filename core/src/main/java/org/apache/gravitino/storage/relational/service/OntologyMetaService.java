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
package org.apache.gravitino.storage.relational.service;

import static org.apache.gravitino.ontology.OssieOntologyDocuments.DEFAULT_VERSION;
import static org.apache.gravitino.ontology.OssieOntologyDocuments.VERSION_PROPERTY;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityFieldLimits;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchOntologyException;
import org.apache.gravitino.exceptions.OntologyAlreadyExistsException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.OntologyEntity;
import org.apache.gravitino.ontology.OntologyChange;
import org.apache.gravitino.ontology.OntologyDefinition;
import org.apache.gravitino.ontology.OntologyReferences;
import org.apache.gravitino.ontology.OssieOntologyDocuments;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.EntityChangeLogWriter;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OntologyMapper;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.OntologySnapshotPO;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/** Stores versioned ontologies and validates their definitions. */
public final class OntologyMetaService {

  private static final OntologyMetaService INSTANCE = new OntologyMetaService();
  private static final ObjectMapper JSON =
      JsonUtils.anyFieldMapper().copy().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
  private static final TypeReference<Map<String, Object>> DEFINITION = new TypeReference<>() {};
  private static final TypeReference<Map<String, String>> PROPERTIES = new TypeReference<>() {};

  private OntologyMetaService() {}

  /**
   * @return The relational ontology service.
   */
  public static OntologyMetaService getInstance() {
    return INSTANCE;
  }

  /**
   * Loads a live ontology entity for the metadata store.
   *
   * @param ident The ontology identifier.
   * @return The current ontology entity.
   */
  public OntologyEntity getEntity(NameIdentifier ident) {
    String metalake = NameIdentifierUtil.getMetalake(ident);
    long metalakeId = metalakeId(metalake);
    OntologySnapshotPO row =
        SessionUtils.getWithoutCommit(
            OntologyMapper.class, mapper -> mapper.loadOntology(metalakeId, ident.name()));
    if (row == null) {
      throw new NoSuchEntityException("Ontology %s does not exist", ident);
    }
    return ontology(metalake, row);
  }

  /**
   * Looks up an ontology ID for authorization or metadata relations.
   *
   * @param metalakeId The containing metalake ID.
   * @param name The ontology name.
   * @return The stable ontology ID.
   */
  public long getId(long metalakeId, String name) {
    Long id =
        SessionUtils.getWithoutCommit(
            OntologyMapper.class, mapper -> mapper.getOntologyId(metalakeId, name));
    if (id == null) {
      throw new NoSuchEntityException("Ontology '%s' does not exist", name);
    }
    return id;
  }

  /**
   * Resolves an ontology ID without loading its definition, for authorization and existence checks.
   *
   * @param ident The ontology identifier.
   * @return The stable ontology ID.
   * @throws NoSuchEntityException If the metalake or the ontology does not exist.
   */
  public long getId(NameIdentifier ident) {
    return getId(metalakeId(NameIdentifierUtil.getMetalake(ident)), ident.name());
  }

  /**
   * Resolves live ontology names by stable IDs, for metadata relations.
   *
   * @param ids The requested ontology IDs.
   * @return The names of the live ontologies.
   */
  public Map<Long, String> namesById(List<Long> ids) {
    if (ids.isEmpty()) {
      return Map.of();
    }
    return SessionUtils.getWithoutCommit(
            OntologyMapper.class, mapper -> mapper.listOntologyNamesByIds(ids))
        .stream()
        .collect(Collectors.toMap(OntologySnapshotPO::getId, OntologySnapshotPO::getName));
  }

  /**
   * Lists ontology names before authorization filtering.
   *
   * @param metalake The containing metalake.
   * @return The live ontology names.
   */
  public List<String> listOntologies(String metalake) {
    long id = metalakeId(metalake);
    return SessionUtils.getWithoutCommit(
        OntologyMapper.class, mapper -> mapper.listOntologyNames(id));
  }

  /**
   * Creates an ontology and its initial snapshot.
   *
   * @param ids The server's ID generator.
   * @param metalake The containing metalake.
   * @param name The ontology name.
   * @param comment The optional comment.
   * @param definition The ontology definition.
   * @param properties The supplied properties.
   * @param ownerHook Assigns ownership inside the create transaction.
   * @return The created ontology.
   */
  public OntologyEntity createOntology(
      IdGenerator ids,
      String metalake,
      String name,
      @Nullable String comment,
      OntologyDefinition definition,
      Map<String, String> properties,
      Consumer<OntologyEntity> ownerHook) {
    checkName(name);
    Map<String, String> stored = withDefaultVersion(properties);
    OssieOntologyDocuments.validateOntology(name, comment, definition, stored);
    OntologyReferences.of(definition);
    long observedId;
    try {
      observedId = metalakeId(metalake);
    } catch (NoSuchEntityException e) {
      throw new NoSuchMetalakeException(e, "Metalake '%s' does not exist", metalake);
    }
    try {
      return SessionUtils.doWithCommitAndFetchResult(
          OntologyMapper.class,
          mapper -> {
            // A shared lock lets ontologies be created concurrently while still blocking a
            // metalake drop, which must update this row, until this transaction ends.
            MetalakePO parent =
                SessionUtils.getWithoutCommit(
                    MetalakeMetaMapper.class,
                    metalakes -> metalakes.selectMetalakeMetaByIdForShare(observedId));
            if (parent == null || !metalake.equals(parent.getMetalakeName())) {
              throw new NoSuchMetalakeException("Metalake '%s' does not exist", metalake);
            }
            OntologySnapshotPO row =
                newRow(ids.nextId(), observedId, name, comment, definition.fields(), stored);
            OntologyEntity entity = ontology(metalake, row);
            entity.validate();
            mapper.insertOntology(row);
            mapper.appendOntologyVersion(row);
            ownerHook.accept(entity);
            return entity;
          });
    } catch (RuntimeException e) {
      throw translateConflict(e, name);
    }
  }

  /**
   * Applies an ordered batch of ontology changes under the identity lock.
   *
   * @param metalake The containing metalake.
   * @param name The current ontology name.
   * @param changes Changes to apply as one transaction.
   * @return The updated ontology.
   */
  public OntologyEntity alterOntology(String metalake, String name, OntologyChange... changes) {
    long metalakeId = metalakeId(metalake);
    try {
      return SessionUtils.doWithCommitAndFetchResult(
          OntologyMapper.class,
          mapper -> {
            Long id = mapper.lockOntology(metalakeId, name);
            if (id == null) {
              throw missingOntology(name);
            }
            OntologySnapshotPO row = mapper.loadOntologyById(id);
            OntologyEntity previous = ontology(metalake, row);
            Content content = new Content(row);
            for (OntologyChange change : changes) {
              content.apply(change);
            }
            if (!content.changed()) {
              return previous;
            }
            OntologyDefinition definition = OntologyDefinition.of(content.fields);
            OssieOntologyDocuments.validateOntology(
                content.name, content.comment, definition, content.properties);
            if (content.replaced) {
              OntologyReferences.of(definition);
            }
            content.update(row);
            OntologyEntity replacement = ontology(metalake, row);
            replacement.validate();
            if (content.snapshot) {
              mapper.appendOntologyVersion(row);
            }
            Preconditions.checkState(
                mapper.updateOntology(row) == 1, "Ontology version changed while locked");
            EntityChangeLogWriter.append(
                NameIdentifierUtil.ofOntology(metalake, name),
                Entity.EntityType.ONTOLOGY,
                OperateType.ALTER);
            return replacement;
          });
    } catch (RuntimeException e) {
      throw translateConflict(e, targetName(name, changes));
    }
  }

  /**
   * Drops a live ontology.
   *
   * @param metalake The containing metalake.
   * @param name The ontology name.
   * @return Whether the ontology existed and was dropped.
   */
  public boolean dropOntology(String metalake, String name) {
    long metalakeId = metalakeId(metalake);
    try {
      return SessionUtils.doWithCommitAndFetchResult(
          OntologyMapper.class,
          mapper -> {
            Long id = mapper.lockOntology(metalakeId, name);
            if (id == null) {
              return false;
            }
            long now = System.currentTimeMillis();
            mapper.dropOntology(id, now);
            mapper.dropOntologyVersions(id, now);
            EntityChangeLogWriter.append(
                NameIdentifierUtil.ofOntology(metalake, name),
                Entity.EntityType.ONTOLOGY,
                OperateType.DROP);
            return true;
          });
    } catch (RuntimeException e) {
      throw translateConflict(e, name);
    }
  }

  /**
   * Drops ontologies in the enclosing metalake transaction.
   *
   * @param metalakeId The dropped metalake ID.
   */
  public void cascadeMetalake(long metalakeId) {
    long now = System.currentTimeMillis();
    SessionUtils.doWithoutCommit(
        OntologyMapper.class,
        mapper -> {
          mapper.cascadeOntologyByMetalake(metalakeId, now);
          mapper.cascadeOntologyVersionsByMetalake(metalakeId, now);
        });
  }

  /**
   * Retires snapshots older than the configured version retention count.
   *
   * @param count The number of versions to retain.
   * @param limit The batch size.
   * @return The number of retired snapshots.
   */
  public int retireVersions(long count, int limit) {
    long now = System.currentTimeMillis();
    return SessionUtils.doWithCommitAndFetchResult(
        OntologyMapper.class, mapper -> mapper.retireOntologyVersions(count, limit, now));
  }

  /**
   * Permanently removes batches of previously deleted rows.
   *
   * @param before The exclusive deletion cutoff.
   * @param limit The batch size per table.
   * @return The number of deleted rows.
   */
  public int purge(long before, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        OntologyMapper.class,
        mapper ->
            mapper.purgeOntologyVersions(before, limit)
                + mapper.purgeOntologyIdentities(before, limit));
  }

  private static long metalakeId(String metalake) {
    return MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
  }

  private static NoSuchOntologyException missingOntology(String name) {
    return new NoSuchOntologyException("Ontology '%s' does not exist", name);
  }

  /**
   * Reports a duplicate name as an already-exists error and a lock wait timeout as a retryable
   * conflict.
   *
   * @param failure The failure of the write transaction.
   * @param name The name the write tried to claim: the new name for a rename.
   * @return The exception to throw.
   */
  private static RuntimeException translateConflict(RuntimeException failure, String name) {
    if (ExceptionUtils.isDuplicateKey(failure)) {
      return new OntologyAlreadyExistsException(
          failure, "Ontology '%s' already exists or the requested name is taken", name);
    }
    return ExceptionUtils.lockWaitTimeoutAsConflict(failure, "The ontology " + name);
  }

  private static String targetName(String name, OntologyChange... changes) {
    String target = name;
    for (OntologyChange change : changes) {
      if (change instanceof OntologyChange.RenameOntology rename) {
        target = rename.getNewName();
      }
    }
    return target;
  }

  private static void checkName(String name) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(name)
            && !MetadataObjects.METADATA_OBJECT_RESERVED_NAME.equals(name)
            && !name.contains(".")
            && name.length() <= EntityFieldLimits.MAX_NAME_LENGTH,
        "Name must be non-blank and contain no dots. The maximum length is %s and the name '*' is reserved.",
        EntityFieldLimits.MAX_NAME_LENGTH);
  }

  private static Map<String, String> withDefaultVersion(Map<String, String> properties) {
    Map<String, String> stored = new LinkedHashMap<>(properties);
    stored.putIfAbsent(VERSION_PROPERTY, DEFAULT_VERSION);
    return ImmutableMap.copyOf(stored);
  }

  private static OntologySnapshotPO newRow(
      long id,
      long metalakeId,
      String name,
      @Nullable String comment,
      Map<String, Object> definition,
      Map<String, String> properties) {
    checkComment(comment);
    OntologySnapshotPO row = new OntologySnapshotPO();
    row.setId(id);
    row.setMetalakeId(metalakeId);
    row.setName(name);
    row.setComment(comment);
    row.setDefinition(write(definition));
    row.setProperties(write(properties));
    row.setAuditInfo(
        write(
            AuditInfo.builder()
                .withCreator(PrincipalUtils.getCurrentUserName())
                .withCreateTime(Instant.now())
                .build()));
    row.setCurrentVersion(1);
    row.setLastVersion(1);
    return row;
  }

  private static void checkComment(@Nullable String comment) {
    Preconditions.checkArgument(
        comment == null || comment.length() <= EntityFieldLimits.MAX_COMMENT_LENGTH,
        "Comment exceeds the maximum length of %s",
        EntityFieldLimits.MAX_COMMENT_LENGTH);
  }

  private static OntologyEntity ontology(String metalake, OntologySnapshotPO row) {
    return new OntologyEntity(
        row.getId(),
        row.getName(),
        NamespaceUtil.ofOntology(metalake),
        row.getComment(),
        OntologyDefinition.of(read(row.getDefinition(), DEFINITION)),
        read(row.getProperties(), PROPERTIES),
        audit(row.getAuditInfo()));
  }

  private static AuditInfo audit(String data) {
    try {
      return JSON.readValue(data, AuditInfo.class);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot read ontology audit information", e);
    }
  }

  private static <T> T read(String data, TypeReference<T> type) {
    try {
      return JSON.readValue(data, type);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot read stored ontology content", e);
    }
  }

  private static String write(Object value) {
    try {
      return JSON.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Cannot serialize ontology content", e);
    }
  }

  private static final class Content {
    private final String originalName;
    @Nullable private final String originalComment;
    private final Map<String, Object> originalFields;
    private final Map<String, String> originalProperties;
    private final Map<String, String> properties;
    private String name;
    @Nullable private String comment;
    private Map<String, Object> fields;
    private boolean snapshot;
    private boolean replaced;

    private Content(OntologySnapshotPO row) {
      originalName = row.getName();
      name = originalName;
      originalComment = row.getComment();
      comment = originalComment;
      originalFields = read(row.getDefinition(), DEFINITION);
      fields = originalFields;
      originalProperties = read(row.getProperties(), PROPERTIES);
      properties = new LinkedHashMap<>(originalProperties);
    }

    private void apply(OntologyChange change) {
      if (change instanceof OntologyChange.RenameOntology rename) {
        rename(rename.getNewName());
      } else if (change instanceof OntologyChange.UpdateComment update) {
        comment(update.getNewComment());
      } else if (change instanceof OntologyChange.SetProperty set) {
        property(set.getProperty(), set.getValue());
      } else if (change instanceof OntologyChange.RemoveProperty remove) {
        property(remove.getProperty(), null);
      } else if (change instanceof OntologyChange.ReplaceDefinition replace) {
        fields = replace.getDefinition().fields();
      } else {
        throw new IllegalArgumentException("Unsupported ontology change: " + change);
      }
    }

    private void rename(String newName) {
      checkName(newName);
      name = newName;
    }

    private void comment(@Nullable String value) {
      checkComment(value);
      comment = value;
    }

    private void property(String key, @Nullable String value) {
      if (value == null) {
        properties.remove(key);
      } else {
        properties.put(key, value);
      }
    }

    /**
     * Compares the result of the whole batch with the stored row, so a batch that restores the
     * original values, or a client retry that repeats them, creates no version.
     */
    private boolean changed() {
      replaced = !fields.equals(originalFields);
      snapshot =
          replaced
              || !Objects.equals(comment, originalComment)
              || !properties.equals(originalProperties);
      return snapshot || !originalName.equals(name);
    }

    private void update(OntologySnapshotPO row) {
      row.setName(name);
      row.setComment(comment);
      row.setDefinition(write(fields));
      row.setProperties(write(properties));
      AuditInfo previous = audit(row.getAuditInfo());
      row.setAuditInfo(
          write(
              AuditInfo.builder()
                  .withCreator(previous.creator())
                  .withCreateTime(previous.createTime())
                  .withLastModifier(PrincipalUtils.getCurrentUserName())
                  .withLastModifiedTime(Instant.now())
                  .build()));
      if (snapshot) {
        row.setLastVersion(row.getLastVersion() + 1);
      }
    }
  }
}
