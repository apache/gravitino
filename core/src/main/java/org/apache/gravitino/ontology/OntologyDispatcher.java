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
package org.apache.gravitino.ontology;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CapabilityHelpers;
import org.apache.gravitino.connector.capability.Capability;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchOntologyException;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.event.ontology.OntologyEvents;
import org.apache.gravitino.meta.OntologyEntity;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.gravitino.storage.relational.service.OntologyMetaService;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/** Normalizes model scopes, applies metadata lifecycle hooks, and audits ontology operations. */
public final class OntologyDispatcher {

  private final GravitinoEnv env;
  private final OntologyMetaService storage;

  /**
   * Creates the server's ontology and mapping dispatcher.
   *
   * @param env The server environment.
   */
  public OntologyDispatcher(GravitinoEnv env) {
    this.env = env;
    this.storage = OntologyMetaService.getInstance();
  }

  /**
   * Lists ontology names.
   *
   * @param metalake The containing metalake.
   * @return The names before authorization filtering.
   */
  public List<String> listOntologies(String metalake) {
    return operate(
        OperationType.LIST_ONTOLOGY,
        NameIdentifier.of(metalake),
        () -> storage.listOntologies(metalake));
  }

  /**
   * Loads the current ontology.
   *
   * @param metalake The containing metalake.
   * @param name The ontology name.
   * @return The current ontology.
   */
  public Ontology loadOntology(String metalake, String name) {
    return operate(
        OperationType.LOAD_ONTOLOGY,
        NameIdentifierUtil.ofOntology(metalake, name),
        () -> getOntology(metalake, name));
  }

  /**
   * Creates an ontology and assigns its creator as owner.
   *
   * @param metalake The containing metalake.
   * @param name The ontology name.
   * @param comment The optional description.
   * @param definition The ontology definition.
   * @param properties The Gravitino properties.
   * @return The created ontology.
   */
  public Ontology createOntology(
      String metalake,
      String name,
      @Nullable String comment,
      OntologyDefinition definition,
      Map<String, String> properties) {
    return operate(
        OperationType.CREATE_ONTOLOGY,
        NameIdentifierUtil.ofOntology(metalake, name),
        () -> create(metalake, name, comment, definition, properties));
  }

  /**
   * Creates an ontology from a standalone Ossie document.
   *
   * @param metalake The containing metalake.
   * @param document The input document.
   * @return The imported ontology.
   */
  public Ontology importOntology(String metalake, OssieDocument document) {
    AtomicReference<NameIdentifier> target = new AtomicReference<>(NameIdentifier.of(metalake));
    return operate(
        OperationType.IMPORT_ONTOLOGY,
        target,
        () -> {
          OssieOntologyDocuments.ParsedOntology value =
              OssieOntologyDocuments.importOntology(document);
          target.set(NameIdentifierUtil.ofOntology(metalake, value.name()));
          return create(
              metalake, value.name(), value.comment(), value.definition(), value.properties());
        },
        created -> NameIdentifierUtil.ofOntology(metalake, created.name()));
  }

  /**
   * Exports the current ontology.
   *
   * @param metalake The containing metalake.
   * @param name The ontology name.
   * @param format The output format.
   * @return The Ossie document.
   */
  public OssieDocument exportOntology(String metalake, String name, OssieFormat format) {
    return operate(
        OperationType.EXPORT_ONTOLOGY,
        NameIdentifierUtil.ofOntology(metalake, name),
        () -> OssieOntologyDocuments.exportOntology(getOntology(metalake, name), format));
  }

  /**
   * Applies ontology changes, preserving compatibility with its live mappings.
   *
   * @param metalake The containing metalake.
   * @param name The current ontology name.
   * @param visible Checks access given the mapping identifier and whether its model has a published
   *     revision.
   * @param changes The changes applied atomically.
   * @return The updated ontology.
   */
  public Ontology alterOntology(
      String metalake,
      String name,
      BiPredicate<NameIdentifier, Boolean> visible,
      OntologyChange... changes) {
    NameIdentifier ident = NameIdentifierUtil.ofOntology(metalake, name);
    return operate(
        OperationType.ALTER_ONTOLOGY,
        ident,
        () -> {
          Ontology result = storage.alterOntology(metalake, name, visible, changes);
          if (!name.equals(result.name())) {
            AuthorizationUtils.notifyEntityNameIdMappingChange(ident, Entity.EntityType.ONTOLOGY);
          }
          return result;
        });
  }

  /**
   * Drops an unused ontology.
   *
   * @param metalake The containing metalake.
   * @param name The ontology name.
   * @param visible Checks access given the mapping identifier and whether its model has a published
   *     revision.
   * @return Whether the ontology was dropped.
   */
  public boolean dropOntology(
      String metalake, String name, BiPredicate<NameIdentifier, Boolean> visible) {
    NameIdentifier ident = NameIdentifierUtil.ofOntology(metalake, name);
    return operate(
        OperationType.DROP_ONTOLOGY,
        ident,
        () -> {
          boolean dropped = storage.dropOntology(metalake, name, visible);
          if (dropped) {
            AuthorizationUtils.notifyEntityNameIdMappingChange(ident, Entity.EntityType.ONTOLOGY);
          }
          return dropped;
        });
  }

  /**
   * Finds mappings of an ontology.
   *
   * @param metalake The containing metalake.
   * @param name The ontology name.
   * @param visible Checks access to each mapping given its model's publication state.
   * @return The visible mapping identifiers.
   */
  public List<NameIdentifier> listOntologyMappings(
      String metalake, String name, BiPredicate<NameIdentifier, Boolean> visible) {
    return operate(
        OperationType.LIST_MAPPINGS_BY_ONTOLOGY,
        NameIdentifierUtil.ofOntology(metalake, name),
        () -> storage.listOntologyMappings(metalake, name, visible));
  }

  /**
   * Lists mappings under a model.
   *
   * @param model The parent model.
   * @param canModify Checks whether the caller can read a never-published model.
   * @return The mapping identifiers.
   */
  public List<NameIdentifier> listModelMappings(
      NameIdentifier model, Predicate<NameIdentifier> canModify) {
    return operate(
        OperationType.LIST_ONTOLOGY_MAPPING,
        model,
        () -> {
          NameIdentifier parent = normalizeModel(model);
          return storage.listModelMappings(parent, canModify).stream()
              .map(name -> mappingIdent(parent, name))
              .toList();
        });
  }

  /**
   * Loads a mapping.
   *
   * @param model The parent model.
   * @param name The mapping name.
   * @param canModify Checks whether the caller can read a never-published model.
   * @return The current mapping.
   */
  public OntologyMapping loadMapping(
      NameIdentifier model, String name, Predicate<NameIdentifier> canModify) {
    return operate(
        OperationType.LOAD_ONTOLOGY_MAPPING,
        mappingIdent(model, name),
        () -> storage.loadMapping(normalizeModel(model), name, canModify));
  }

  /**
   * Creates a mapping under a model.
   *
   * @param model The parent model.
   * @param name The mapping name.
   * @param comment The optional description.
   * @param ontology The referenced ontology name.
   * @param definition The mapping definition.
   * @param properties The Gravitino properties.
   * @param checkOntology Checks load authority on the referenced ontology.
   * @return The created mapping.
   */
  public OntologyMapping createMapping(
      NameIdentifier model,
      String name,
      @Nullable String comment,
      String ontology,
      OntologyMappingDefinition definition,
      Map<String, String> properties,
      Consumer<String> checkOntology) {
    return operate(
        OperationType.CREATE_ONTOLOGY_MAPPING,
        mappingIdent(model, name),
        () ->
            storage.createMapping(
                env.idGenerator(),
                normalizeModel(model),
                name,
                comment,
                ontology,
                definition,
                properties,
                checkOntology));
  }

  /**
   * Imports an Ossie mapping under a model.
   *
   * @param model The parent model.
   * @param document The input document.
   * @param checkOntology Checks load authority on the referenced ontology.
   * @return The imported mapping.
   */
  public OntologyMapping importMapping(
      NameIdentifier model, OssieDocument document, Consumer<String> checkOntology) {
    AtomicReference<NameIdentifier> target = new AtomicReference<>(model);
    return operate(
        OperationType.IMPORT_ONTOLOGY_MAPPING,
        target,
        () -> {
          NameIdentifier parent = normalizeModel(model);
          OssieOntologyDocuments.ParsedMapping value =
              OssieOntologyDocuments.importMapping(document, parent.name());
          target.set(mappingIdent(model, value.name()));
          return storage.createMapping(
              env.idGenerator(),
              parent,
              value.name(),
              value.comment(),
              value.ontology(),
              value.definition(),
              value.properties(),
              checkOntology);
        },
        created -> mappingIdent(model, created.name()));
  }

  /**
   * Exports the current mapping with its current parent and ontology names.
   *
   * @param model The parent model.
   * @param name The mapping name.
   * @param format The output format.
   * @param canModify Checks whether the caller can read a never-published model.
   * @return The Ossie document.
   */
  public OssieDocument exportMapping(
      NameIdentifier model, String name, OssieFormat format, Predicate<NameIdentifier> canModify) {
    return operate(
        OperationType.EXPORT_ONTOLOGY_MAPPING,
        mappingIdent(model, name),
        () -> {
          NameIdentifier parent = normalizeModel(model);
          return OssieOntologyDocuments.exportMapping(
              storage.loadMapping(parent, name, canModify), parent.name(), format);
        });
  }

  /**
   * Applies a batch of mapping changes.
   *
   * @param model The parent model.
   * @param name The current mapping name.
   * @param checkOntology Checks load authority if the definition is replaced.
   * @param changes The changes applied atomically.
   * @return The updated mapping.
   */
  public OntologyMapping alterMapping(
      NameIdentifier model,
      String name,
      Consumer<String> checkOntology,
      OntologyMappingChange... changes) {
    return operate(
        OperationType.ALTER_ONTOLOGY_MAPPING,
        mappingIdent(model, name),
        () -> storage.alterMapping(normalizeModel(model), name, checkOntology, changes));
  }

  /**
   * Drops a mapping.
   *
   * @param model The parent model.
   * @param name The mapping name.
   * @return Whether it was dropped.
   */
  public boolean dropMapping(NameIdentifier model, String name) {
    return operate(
        OperationType.DROP_ONTOLOGY_MAPPING,
        mappingIdent(model, name),
        () -> storage.dropMapping(normalizeModel(model), name));
  }

  private OntologyEntity create(
      String metalake,
      String name,
      @Nullable String comment,
      OntologyDefinition definition,
      Map<String, String> properties) {
    return storage.createOntology(
        env.idGenerator(),
        metalake,
        name,
        comment,
        definition,
        properties,
        entity -> {
          OwnerDispatcher owner = env.internalOwnerDispatcher();
          if (owner != null) {
            owner.setOwner(
                metalake,
                MetadataObjects.of(null, name, MetadataObject.Type.ONTOLOGY),
                PrincipalUtils.getCurrentUserName(),
                Owner.Type.USER);
          }
        });
  }

  private OntologyEntity getOntology(String metalake, String name) {
    try {
      return storage.getEntity(NameIdentifierUtil.ofOntology(metalake, name));
    } catch (NoSuchEntityException e) {
      throw new NoSuchOntologyException(e, "Ontology '%s' does not exist", name);
    }
  }

  private NameIdentifier normalizeModel(NameIdentifier model) {
    return env.catalogManager()
        .doWithCatalog(
            NameIdentifierUtil.getCatalogIdentifier(model),
            catalog -> {
              catalog.checkMetalakeAndCatalogInUse();
              return NameIdentifier.of(
                  CapabilityHelpers.applyCaseSensitive(
                      model.namespace(), Capability.Scope.SEMANTIC_MODEL, catalog.capability()),
                  model.name());
            });
  }

  private static NameIdentifier mappingIdent(NameIdentifier model, String mapping) {
    return NameIdentifier.of(
        model.namespace().level(0),
        model.namespace().level(1),
        model.namespace().level(2),
        model.name(),
        mapping);
  }

  private <T> T operate(OperationType operation, NameIdentifier ident, Supplier<T> action) {
    return operate(operation, new AtomicReference<>(ident), action, result -> ident);
  }

  /**
   * Runs an audited operation. An import learns its object's name only after parsing, so the action
   * may narrow {@code target} from the parent to that object for the failure event.
   */
  private <T> T operate(
      OperationType operation,
      AtomicReference<NameIdentifier> target,
      Supplier<T> action,
      Function<T, NameIdentifier> successIdentifier) {
    String user = PrincipalUtils.getCurrentUserName();
    NameIdentifier ident = target.get();
    T result;
    try {
      env.eventBus().dispatchEvent(new OntologyEvents.Before(user, ident, operation));
      MetalakeManager.checkMetalake(
          NameIdentifierUtil.ofMetalake(NameIdentifierUtil.getMetalake(ident)), env.entityStore());
      result = action.get();
    } catch (RuntimeException failure) {
      env.eventBus()
          .dispatchEvent(new OntologyEvents.Failure(user, target.get(), operation, failure));
      throw failure;
    }
    env.eventBus()
        .dispatchEvent(
            new OntologyEvents.Success(user, successIdentifier.apply(result), operation));
    return result;
  }
}
