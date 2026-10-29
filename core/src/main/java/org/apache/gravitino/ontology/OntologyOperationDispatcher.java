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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.IllegalOntologyException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchOntologyException;
import org.apache.gravitino.exceptions.NonEmptyOntologyException;
import org.apache.gravitino.exceptions.OntologyAlreadyExistsException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.OntologyEntity;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The {@link OntologyDispatcher} that reads and writes ontologies through the entity store. */
public class OntologyOperationDispatcher implements OntologyDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(OntologyOperationDispatcher.class);

  private final EntityStore store;
  private final IdGenerator idGenerator;

  /**
   * Creates a new ontology operation dispatcher.
   *
   * @param store The entity store that persists ontologies.
   * @param idGenerator The id generator for new ontology entities.
   */
  public OntologyOperationDispatcher(EntityStore store, IdGenerator idGenerator) {
    this.store = store;
    this.idGenerator = idGenerator;
  }

  @Override
  public String[] listOntologies(String metalake) throws NoSuchMetalakeException {
    MetalakeManager.checkMetalake(NameIdentifier.of(metalake), store);
    try {
      List<OntologyEntity> ontologies = listOntologyEntities(metalake);
      return ontologies.stream().map(OntologyEntity::name).toArray(String[]::new);
    } catch (IOException ioe) {
      LOG.error("Failed to list ontologies under metalake {}", metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public Ontology[] listOntologiesInfo(String metalake) throws NoSuchMetalakeException {
    MetalakeManager.checkMetalake(NameIdentifier.of(metalake), store);
    try {
      List<OntologyEntity> ontologies = listOntologyEntities(metalake);
      return ontologies.toArray(new Ontology[0]);
    } catch (IOException ioe) {
      LOG.error("Failed to list ontologies under metalake {}", metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public Ontology getOntology(String metalake, String name) throws NoSuchOntologyException {
    MetalakeManager.checkMetalake(NameIdentifier.of(metalake), store);
    try {
      return getOntologyEntity(NameIdentifier.of(metalake, name));
    } catch (NoSuchEntityException e) {
      throw new NoSuchOntologyException(
          "Ontology with name %s under metalake %s does not exist", name, metalake);
    }
  }

  @Override
  public Ontology createOntology(
      String metalake,
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      OntologyDefinition definition)
      throws NoSuchMetalakeException, OntologyAlreadyExistsException, IllegalOntologyException {
    MetalakeManager.checkMetalake(NameIdentifier.of(metalake), store);
    OntologyEntity ontologyEntity;
    try {
      ontologyEntity = newOntologyEntity(metalake, name, comment, properties, definition);
    } catch (IllegalArgumentException iae) {
      throw new IllegalOntologyException(iae, "Invalid ontology %s: %s", name, iae.getMessage());
    }

    try {
      store.put(ontologyEntity, false /* overwritten */);
      return ontologyEntity;
    } catch (EntityAlreadyExistsException e) {
      throw new OntologyAlreadyExistsException(
          "Ontology with name %s under metalake %s already exists", name, metalake);
    } catch (IOException ioe) {
      LOG.error("Failed to create ontology {} under metalake {}", name, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public Ontology alterOntology(String metalake, String name, OntologyChange... changes)
      throws NoSuchOntologyException, OntologyAlreadyExistsException, IllegalOntologyException {
    MetalakeManager.checkMetalake(NameIdentifier.of(metalake), store);
    try {
      return store.update(
          NameIdentifier.of(metalake, name),
          OntologyEntity.class,
          Entity.EntityType.ONTOLOGY,
          entity -> updateOntologyEntity(entity, changes));
    } catch (NoSuchEntityException e) {
      throw new NoSuchOntologyException(
          "Ontology with name %s under metalake %s does not exist", name, metalake);
    } catch (EntityAlreadyExistsException e) {
      throw new OntologyAlreadyExistsException(
          "Ontology with name %s under metalake %s already exists", name, metalake);
    } catch (OptimisticLockException ole) {
      LOG.warn(
          "Failed to alter ontology {} under metalake {} because of a concurrent operation",
          name,
          metalake,
          ole);
      throw ole;
    } catch (IOException ioe) {
      LOG.error("Failed to alter ontology {} under metalake {}", name, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public boolean deleteOntology(String metalake, String name) throws NonEmptyOntologyException {
    MetalakeManager.checkMetalake(NameIdentifier.of(metalake), store);
    try {
      return store.delete(NameIdentifier.of(metalake, name), Entity.EntityType.ONTOLOGY);
    } catch (OptimisticLockException ole) {
      LOG.warn(
          "Failed to delete ontology {} under metalake {} because of a concurrent operation",
          name,
          metalake,
          ole);
      throw ole;
    } catch (IOException ioe) {
      LOG.error("Failed to delete ontology {} under metalake {}", name, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public Ontology importOssieDocument(String metalake, OssieDocument document)
      throws NoSuchMetalakeException, OntologyAlreadyExistsException, IllegalOntologyException {
    OssieOntologyConverter.ParsedOntology parsed;
    try {
      parsed = OssieOntologyConverter.importDocument(document);
    } catch (IllegalArgumentException iae) {
      throw new IllegalOntologyException(
          iae, "Invalid Ossie ontology document: %s", iae.getMessage());
    }
    return createOntology(
        metalake, parsed.name(), parsed.comment(), parsed.properties(), parsed.definition());
  }

  @Override
  public OssieDocument exportOssieDocument(String metalake, String name, OssieFormat format)
      throws NoSuchOntologyException {
    return OssieOntologyConverter.exportDocument(getOntology(metalake, name), format);
  }

  @Override
  public OntologyEntity getOntologyEntity(NameIdentifier ident) throws NoSuchEntityException {
    try {
      return store.get(ident, Entity.EntityType.ONTOLOGY, OntologyEntity.class);
    } catch (IOException ioe) {
      LOG.error("Failed to get ontology {}", ident, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private List<OntologyEntity> listOntologyEntities(String metalake) throws IOException {
    List<OntologyEntity> ontologies =
        store.list(Namespace.of(metalake), OntologyEntity.class, Entity.EntityType.ONTOLOGY);
    return ontologies == null ? Collections.emptyList() : ontologies;
  }

  private OntologyEntity newOntologyEntity(
      String metalake,
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      OntologyDefinition definition) {
    return OntologyEntity.builder()
        .withId(idGenerator.nextId())
        .withName(name)
        .withNamespace(Namespace.of(metalake))
        .withComment(comment)
        .withStatus(Ontology.Status.DRAFT)
        .withDefinition(OssieOntologyConverter.assignIds(definition, null))
        .withProperties(properties == null ? Collections.emptyMap() : properties)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                .withCreateTime(Instant.now())
                .build())
        .build();
  }

  private static OntologyEntity updateOntologyEntity(
      OntologyEntity entity, OntologyChange... changes) {
    String newName = entity.name();
    String newComment = entity.comment();
    Ontology.Status newStatus = entity.status();
    OntologyDefinition newDefinition = entity.definition();
    Map<String, String> newProperties = new HashMap<>(entity.properties());

    for (OntologyChange change : changes) {
      if (change instanceof OntologyChange.RenameOntology) {
        newName = ((OntologyChange.RenameOntology) change).getNewName();
      } else if (change instanceof OntologyChange.UpdateComment) {
        newComment = ((OntologyChange.UpdateComment) change).getNewComment();
      } else if (change instanceof OntologyChange.SetProperty) {
        OntologyChange.SetProperty setProperty = (OntologyChange.SetProperty) change;
        newProperties.put(setProperty.getProperty(), setProperty.getValue());
      } else if (change instanceof OntologyChange.RemoveProperty) {
        newProperties.remove(((OntologyChange.RemoveProperty) change).getProperty());
      } else if (change instanceof OntologyChange.ReplaceDefinition) {
        newDefinition = ((OntologyChange.ReplaceDefinition) change).getDefinition();
      } else if (change instanceof OntologyChange.UpdateStatus) {
        newStatus = ((OntologyChange.UpdateStatus) change).getStatus();
      } else {
        throw new IllegalArgumentException(
            "Unsupported ontology change: " + change.getClass().getSimpleName());
      }
    }

    return OntologyEntity.builder()
        .withId(entity.id())
        .withName(newName)
        .withNamespace(entity.namespace())
        .withComment(newComment)
        .withStatus(newStatus)
        .withDefinition(newDefinition)
        .withProperties(
            newProperties.isEmpty() ? Collections.emptyMap() : ImmutableMap.copyOf(newProperties))
        .withAuditInfo(entity.auditInfo())
        .build();
  }
}
