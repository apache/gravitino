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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityFieldLimits;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.IllegalOntologyMappingException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchOntologyException;
import org.apache.gravitino.exceptions.NoSuchOntologyMappingException;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.apache.gravitino.exceptions.OntologyAlreadyExistsException;
import org.apache.gravitino.exceptions.OntologyInUseException;
import org.apache.gravitino.exceptions.OntologyMappingAlreadyExistsException;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.OntologyEntity;
import org.apache.gravitino.ontology.OntologyChange;
import org.apache.gravitino.ontology.OntologyDefinition;
import org.apache.gravitino.ontology.OntologyMapping;
import org.apache.gravitino.ontology.OntologyMappingChange;
import org.apache.gravitino.ontology.OntologyMappingDefinition;
import org.apache.gravitino.ontology.OntologyReferences;
import org.apache.gravitino.ontology.OssieOntologyDocuments;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.relational.EntityChangeLogWriter;
import org.apache.gravitino.storage.relational.mapper.MetalakeMetaMapper;
import org.apache.gravitino.storage.relational.mapper.OntologyMapper;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetaMapper;
import org.apache.gravitino.storage.relational.po.MetalakePO;
import org.apache.gravitino.storage.relational.po.OntologySnapshotPO;
import org.apache.gravitino.storage.relational.po.SemanticModelPO;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.storage.relational.utils.ExceptionUtils;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/** Stores ontologies and model mappings and enforces their version and reference invariants. */
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
      throw translateConflict(e, name, false);
    }
  }

  /**
   * Applies an ordered batch of ontology changes under the identity lock.
   *
   * @param metalake The containing metalake.
   * @param name The current ontology name.
   * @param visibleMapping Checks access given the mapping identifier and whether its model has a
   *     published revision.
   * @param changes Changes to apply as one transaction.
   * @return The updated ontology.
   */
  public OntologyEntity alterOntology(
      String metalake,
      String name,
      BiPredicate<NameIdentifier, Boolean> visibleMapping,
      OntologyChange... changes) {
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
              OntologyReferences references = OntologyReferences.of(definition);
              checkMappings(metalake, name, mapper.listOntologyMappings(id), references);
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
    } catch (MappingsInUse e) {
      throw e.toException(visibleMapping);
    } catch (RuntimeException e) {
      throw translateConflict(e, targetName(name, changes), false);
    }
  }

  /**
   * Drops an ontology only if no live mapping references it.
   *
   * @param metalake The containing metalake.
   * @param name The ontology name.
   * @param visibleMapping Checks access given the mapping identifier and whether its model has a
   *     published revision.
   * @return Whether the ontology existed and was dropped.
   */
  public boolean dropOntology(
      String metalake, String name, BiPredicate<NameIdentifier, Boolean> visibleMapping) {
    long metalakeId = metalakeId(metalake);
    try {
      return SessionUtils.doWithCommitAndFetchResult(
          OntologyMapper.class,
          mapper -> {
            Long id = mapper.lockOntology(metalakeId, name);
            if (id == null) {
              return false;
            }
            checkMappings(metalake, name, mapper.listOntologyMappingIdentities(id), null);
            long now = System.currentTimeMillis();
            mapper.dropOntology(id, now);
            mapper.dropOntologyVersions(id, now);
            EntityChangeLogWriter.append(
                NameIdentifierUtil.ofOntology(metalake, name),
                Entity.EntityType.ONTOLOGY,
                OperateType.DROP);
            return true;
          });
    } catch (MappingsInUse e) {
      throw e.toException(visibleMapping);
    } catch (RuntimeException e) {
      throw translateConflict(e, name, false);
    }
  }

  /**
   * Lists every mapping of an ontology, using current model names.
   *
   * @param metalake The containing metalake.
   * @param name The ontology name.
   * @return Mapping identifiers including the metalake.
   */
  public List<NameIdentifier> listOntologyMappings(String metalake, String name) {
    long metalakeId = metalakeId(metalake);
    return SessionUtils.getWithoutCommit(
        OntologyMapper.class,
        mapper -> {
          Long id = mapper.getOntologyId(metalakeId, name);
          if (id == null) {
            throw missingOntology(name);
          }
          return mapper.listOntologyMappingIdentities(id).stream()
              .map(mapping -> mappingIdentifier(metalake, mapping))
              .toList();
        });
  }

  /**
   * Lists mappings under a semantic model.
   *
   * @param model The parent model identifier.
   * @return The live names in that model.
   */
  public List<String> listModelMappings(NameIdentifier model) {
    long id = requireModel(model).getSemanticModelId();
    return SessionUtils.getWithoutCommit(
        OntologyMapper.class, mapper -> mapper.listModelMappings(id));
  }

  /**
   * Loads a mapping using its current ontology name.
   *
   * @param model The parent model identifier.
   * @param name The mapping name.
   * @return The current mapping.
   */
  public OntologyMapping loadMapping(NameIdentifier model, String name) {
    long id = requireModel(model).getSemanticModelId();
    return SessionUtils.getWithoutCommit(
        OntologyMapper.class, mapper -> mapping(requireMapping(mapper, id, name)));
  }

  /**
   * Creates a mapping under its model and validates references while the ontology is locked.
   *
   * @param ids The server's ID generator.
   * @param model The parent model identifier.
   * @param name The mapping name.
   * @param comment The optional comment.
   * @param ontology The ontology name.
   * @param definition The mapping definition.
   * @param properties The supplied properties.
   * @param checkOntologyLoad Enforces the caller's load authority for the referenced ontology.
   * @return The created mapping.
   */
  public OntologyMapping createMapping(
      IdGenerator ids,
      NameIdentifier model,
      String name,
      @Nullable String comment,
      String ontology,
      OntologyMappingDefinition definition,
      Map<String, String> properties,
      Consumer<String> checkOntologyLoad) {
    checkName(name);
    Map<String, String> stored = withDefaultVersion(properties);
    OssieOntologyDocuments.validateMapping(
        name, comment, ontology, model.name(), definition, stored);
    try {
      return SessionUtils.doWithCommitAndFetchResult(
          OntologyMapper.class,
          mapper -> {
            SemanticModelPO parent = lockModel(model);
            Long ontologyId = mapper.lockOntology(parent.getMetalakeId(), ontology);
            // Authorize first: reporting a missing ontology to a caller who cannot load the
            // ontology anyway would reveal which ontology names exist.
            checkOntologyLoad.accept(ontology);
            if (ontologyId == null) {
              throw new IllegalOntologyMappingException(
                  "$.ontology: ontology '%s' does not exist in the model's metalake", ontology);
            }
            validateMappingReferences(mapper, ontologyId, definition);
            OntologySnapshotPO row =
                newRow(
                    ids.nextId(),
                    parent.getMetalakeId(),
                    name,
                    comment,
                    definition.fields(),
                    stored);
            row.setCatalogId(parent.getCatalogId());
            row.setSchemaId(parent.getSchemaId());
            row.setSemanticModelId(parent.getSemanticModelId());
            row.setOntologyId(ontologyId);
            row.setOntologyName(ontology);
            mapper.insertMapping(row);
            mapper.appendMappingVersion(row);
            return mapping(row);
          });
    } catch (RuntimeException e) {
      if (ExceptionUtils.isDuplicateKey(e)) {
        throw new OntologyMappingAlreadyExistsException(
            e,
            "Mapping name '%s' is taken or the semantic model already has a mapping for ontology '%s'",
            name,
            ontology);
      }
      throw ExceptionUtils.lockWaitTimeoutAsConflict(e, "The mapping " + name);
    }
  }

  /**
   * Applies an ordered batch of mapping changes.
   *
   * @param model The parent model.
   * @param name The existing mapping name.
   * @param checkOntologyLoad Enforces load authority for a definition replacement request.
   * @param changes The changes to apply in one transaction.
   * @return The updated mapping.
   */
  public OntologyMapping alterMapping(
      NameIdentifier model,
      String name,
      Consumer<String> checkOntologyLoad,
      OntologyMappingChange... changes) {
    try {
      return SessionUtils.doWithCommitAndFetchResult(
          OntologyMapper.class,
          mapper -> {
            SemanticModelPO parent = lockModel(model);
            OntologySnapshotPO row = requireMapping(mapper, parent.getSemanticModelId(), name);
            Content content = new Content(row);
            for (OntologyMappingChange change : changes) {
              content.apply(change);
            }
            if (content.mappingReplacementRequested) {
              Long ontologyId = mapper.lockOntologyById(row.getOntologyId());
              Preconditions.checkState(
                  ontologyId != null, "Missing ontology of live mapping %s", name);
              // Reload the ontology name after taking the lock, as it may have been renamed.
              OntologySnapshotPO referenced = mapper.loadOntologyById(ontologyId);
              row.setOntologyName(referenced.getName());
              checkOntologyLoad.accept(referenced.getName());
            }
            if (!content.changed()) {
              return mapping(row);
            }
            OntologyMappingDefinition definition = OntologyMappingDefinition.of(content.fields);
            OssieOntologyDocuments.validateMapping(
                content.name,
                content.comment,
                row.getOntologyName(),
                model.name(),
                definition,
                content.properties);
            if (content.replaced) {
              validateMappingReferences(mapper, row.getOntologyId(), definition);
            }
            content.update(row);
            if (content.snapshot) {
              mapper.appendMappingVersion(row);
            }
            Preconditions.checkState(
                mapper.updateMapping(row) == 1, "Mapping version changed while locked");
            return mapping(row);
          });
    } catch (RuntimeException e) {
      throw translateConflict(e, targetName(name, changes), true);
    }
  }

  /**
   * Drops a mapping and its snapshots in one transaction.
   *
   * @param model The parent model.
   * @param name The mapping name.
   * @return Whether the mapping existed and was dropped.
   */
  public boolean dropMapping(NameIdentifier model, String name) {
    try {
      return SessionUtils.doWithCommitAndFetchResult(
          OntologyMapper.class,
          mapper -> {
            SemanticModelPO parent = lockModel(model);
            Long id = mapper.getMappingId(parent.getSemanticModelId(), name);
            if (id == null) {
              return false;
            }
            long now = System.currentTimeMillis();
            mapper.dropMappingVersions(id, now);
            mapper.dropMapping(id, now);
            return true;
          });
    } catch (RuntimeException e) {
      throw translateConflict(e, name, true);
    }
  }

  /**
   * Drops ontologies and mappings in the enclosing metalake transaction.
   *
   * @param metalakeId The dropped metalake ID.
   */
  public void cascadeMetalake(long metalakeId) {
    long now = System.currentTimeMillis();
    SessionUtils.doWithoutCommit(
        OntologyMapper.class,
        mapper -> {
          mapper.cascadeMappingVersionsByMetalake(metalakeId, now);
          mapper.cascadeMappingByMetalake(metalakeId, now);
          mapper.cascadeOntologyByMetalake(metalakeId, now);
          mapper.cascadeOntologyVersionsByMetalake(metalakeId, now);
        });
  }

  /**
   * Drops mappings in the enclosing catalog transaction.
   *
   * @param catalogId The dropped catalog ID.
   */
  public void cascadeCatalog(long catalogId) {
    long now = System.currentTimeMillis();
    SessionUtils.doWithoutCommit(
        OntologyMapper.class,
        mapper -> {
          mapper.cascadeMappingVersionsByCatalog(catalogId, now);
          mapper.cascadeMappingByCatalog(catalogId, now);
        });
  }

  /**
   * Drops mappings in the enclosing schema cascade.
   *
   * @param schemaIds The dropped schemas, including descendants.
   */
  public void cascadeSchemas(List<Long> schemaIds) {
    long now = System.currentTimeMillis();
    SessionUtils.doWithoutCommit(
        OntologyMapper.class,
        mapper -> {
          mapper.cascadeMappingVersionsBySchemas(schemaIds, now);
          mapper.cascadeMappingBySchemas(schemaIds, now);
        });
  }

  /**
   * Drops mappings in the enclosing semantic model transaction.
   *
   * @param modelId The dropped semantic model ID.
   */
  public void cascadeModel(long modelId) {
    long now = System.currentTimeMillis();
    SessionUtils.doWithoutCommit(
        OntologyMapper.class,
        mapper -> {
          mapper.cascadeMappingVersionsByModel(modelId, now);
          mapper.cascadeMappingByModel(modelId, now);
        });
  }

  /**
   * Retires snapshots older than the configured version retention count.
   *
   * @param mapping Whether to process mapping snapshots.
   * @param count The number of versions to retain.
   * @param limit The batch size.
   * @return The number of retired snapshots.
   */
  public int retireVersions(boolean mapping, long count, int limit) {
    long now = System.currentTimeMillis();
    return SessionUtils.doWithCommitAndFetchResult(
        OntologyMapper.class,
        mapper ->
            mapping
                ? mapper.retireMappingVersions(count, limit, now)
                : mapper.retireOntologyVersions(count, limit, now));
  }

  /**
   * Permanently removes batches of previously deleted rows.
   *
   * @param mapping Whether to purge mappings.
   * @param before The exclusive deletion cutoff.
   * @param limit The batch size per table.
   * @return The number of deleted rows.
   */
  public int purge(boolean mapping, long before, int limit) {
    return SessionUtils.doWithCommitAndFetchResult(
        OntologyMapper.class,
        mapper ->
            mapping
                ? mapper.purgeMappingVersions(before, limit)
                    + mapper.purgeMappingIdentities(before, limit)
                : mapper.purgeOntologyVersions(before, limit)
                    + mapper.purgeOntologyIdentities(before, limit));
  }

  private static long metalakeId(String metalake) {
    return MetalakeMetaService.getInstance().getMetalakeIdByName(metalake);
  }

  private static SemanticModelPO requireModel(NameIdentifier model) {
    try {
      return SemanticModelMetaService.getInstance().getSemanticModelPOByIdentifier(model);
    } catch (NoSuchEntityException e) {
      throw new NoSuchSemanticModelException(e, "Semantic model '%s' does not exist", model);
    }
  }

  private static SemanticModelPO lockModel(NameIdentifier model) {
    SemanticModelPO observed = requireModel(model);
    SemanticModelPO locked =
        SessionUtils.getWithoutCommit(
            SemanticModelMetaMapper.class,
            mapper -> mapper.selectSemanticModelMetaByIdForUpdate(observed.getSemanticModelId()));
    if (locked == null
        || !Objects.equals(locked.getSemanticModelName(), observed.getSemanticModelName())
        || !Objects.equals(locked.getSchemaId(), observed.getSchemaId())) {
      throw new NoSuchSemanticModelException("Semantic model '%s' does not exist", model);
    }
    return locked;
  }

  private static OntologySnapshotPO requireMapping(
      OntologyMapper mapper, long modelId, String name) {
    OntologySnapshotPO row = mapper.loadMapping(modelId, name);
    if (row == null) {
      throw new NoSuchOntologyMappingException("Mapping '%s' does not exist", name);
    }
    return row;
  }

  private static NoSuchOntologyException missingOntology(String name) {
    return new NoSuchOntologyException("Ontology '%s' does not exist", name);
  }

  private static void validateMappingReferences(
      OntologyMapper mapper, long ontologyId, OntologyMappingDefinition definition) {
    OntologyReferences.of(
            OntologyDefinition.of(
                read(mapper.loadOntologyById(ontologyId).getDefinition(), DEFINITION)))
        .validateMapping(definition);
  }

  /**
   * Rejects the change if live mappings would break. Authorization decides how much of each
   * conflict is reported, so it runs after the transaction rolls back and releases its row locks.
   */
  private static void checkMappings(
      String metalake,
      String name,
      List<OntologySnapshotPO> mappings,
      @Nullable OntologyReferences references) {
    List<MappingConflict> conflicts = new ArrayList<>();
    for (OntologySnapshotPO row : mappings) {
      String detail = "";
      if (references != null) {
        try {
          references.validateMapping(
              OntologyMappingDefinition.of(read(row.getDefinition(), DEFINITION)));
          continue;
        } catch (IllegalOntologyMappingException e) {
          detail = " at " + e.getMessage();
        }
      }
      conflicts.add(
          new MappingConflict(
              mappingIdentifier(metalake, row), row.getSemanticModelActiveVersion() > 0, detail));
    }
    if (!conflicts.isEmpty()) {
      throw new MappingsInUse(name, conflicts);
    }
  }

  private static NameIdentifier mappingIdentifier(String metalake, OntologySnapshotPO row) {
    return NameIdentifier.of(
        metalake,
        row.getCatalogName(),
        HierarchicalSchemaUtil.physicalToLogical(
            row.getSchemaName(), HierarchicalSchemaUtil.schemaSeparator()),
        row.getSemanticModelName(),
        row.getName());
  }

  /**
   * Reports a duplicate name as an already-exists error and a lock wait timeout as a retryable
   * conflict.
   *
   * @param failure The failure of the write transaction.
   * @param name The name the write tried to claim: the new name for a rename.
   * @param mapping Whether the write targeted a mapping.
   * @return The exception to throw.
   */
  private static RuntimeException translateConflict(
      RuntimeException failure, String name, boolean mapping) {
    if (ExceptionUtils.isDuplicateKey(failure)) {
      return mapping
          ? new OntologyMappingAlreadyExistsException(
              failure, "Mapping '%s' already exists or the requested name is taken", name)
          : new OntologyAlreadyExistsException(
              failure, "Ontology '%s' already exists or the requested name is taken", name);
    }
    return ExceptionUtils.lockWaitTimeoutAsConflict(
        failure, (mapping ? "The mapping " : "The ontology ") + name);
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

  private static String targetName(String name, OntologyMappingChange... changes) {
    String target = name;
    for (OntologyMappingChange change : changes) {
      if (change instanceof OntologyMappingChange.RenameOntologyMapping rename) {
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

  private static OntologyMapping mapping(OntologySnapshotPO row) {
    return new CurrentMapping(
        row.getName(),
        row.getComment(),
        row.getOntologyName(),
        OntologyMappingDefinition.of(read(row.getDefinition(), DEFINITION)),
        ImmutableMap.copyOf(read(row.getProperties(), PROPERTIES)),
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

  @SuppressWarnings("UnusedVariable") // Read through the record accessors.
  private record MappingConflict(
      NameIdentifier identifier, boolean modelHasPublishedRevision, String detail) {}

  /** Carries mapping conflicts out of the transaction so authorization runs without row locks. */
  private static final class MappingsInUse extends RuntimeException {
    private final String ontology;
    private final List<MappingConflict> conflicts;

    private MappingsInUse(String ontology, List<MappingConflict> conflicts) {
      super(null, null, false, false);
      this.ontology = ontology;
      this.conflicts = conflicts;
    }

    private OntologyInUseException toException(
        BiPredicate<NameIdentifier, Boolean> visibleMapping) {
      List<String> visible = new ArrayList<>();
      int hidden = 0;
      for (MappingConflict conflict : conflicts) {
        NameIdentifier identifier = conflict.identifier();
        if (visibleMapping.test(identifier, conflict.modelHasPublishedRevision())) {
          // Include the failing JSON path and reason only for a mapping the caller can load.
          visible.add(
              NameIdentifier.of(
                      identifier.namespace().level(1),
                      identifier.namespace().level(2),
                      identifier.namespace().level(3),
                      identifier.name())
                  + conflict.detail());
        } else {
          hidden++;
        }
      }
      if (hidden > 0) {
        visible.add(hidden + (hidden == 1 ? " mapping" : " mappings") + " the caller cannot load");
      }
      return new OntologyInUseException(
          "Ontology %s is used by %s", ontology, String.join(", ", visible));
    }
  }

  @SuppressWarnings("UnusedVariable") // Accessed through the OntologyMapping interface.
  private record CurrentMapping(
      String name,
      @Nullable String comment,
      String ontology,
      OntologyMappingDefinition definition,
      Map<String, String> properties,
      AuditInfo auditInfo)
      implements OntologyMapping {}

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
    private boolean mappingReplacementRequested;

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

    private void apply(OntologyMappingChange change) {
      if (change instanceof OntologyMappingChange.RenameOntologyMapping rename) {
        rename(rename.getNewName());
      } else if (change instanceof OntologyMappingChange.UpdateComment update) {
        comment(update.getNewComment());
      } else if (change instanceof OntologyMappingChange.SetProperty set) {
        property(set.getProperty(), set.getValue());
      } else if (change instanceof OntologyMappingChange.RemoveProperty remove) {
        property(remove.getProperty(), null);
      } else if (change instanceof OntologyMappingChange.ReplaceDefinition replace) {
        fields = replace.getDefinition().fields();
        mappingReplacementRequested = true;
      } else {
        throw new IllegalArgumentException("Unsupported ontology mapping change: " + change);
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
