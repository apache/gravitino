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
package org.apache.gravitino.domain;

import static org.apache.gravitino.metalake.MetalakeManager.checkMetalake;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.SupportsDomainOperations;
import org.apache.gravitino.SupportsDomainOperations.LockedDomainState;
import org.apache.gravitino.exceptions.DomainAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyDomainException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.gravitino.utils.RenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The domain manager. It keeps the domain hierarchy one level deep and the status of each domain
 * consistent with its parent and sub-domains (see {@link DomainDispatcher}).
 *
 * <p>The rules that involve other domains are checked by the entity store's domain operations,
 * against the domains as they are under the row locks of the change, so that a concurrent change
 * cannot break them. Name, comment and property changes involve no other domain and use the store's
 * update, as other entities do.
 */
public class DomainManager implements DomainDispatcher {

  private static final Logger LOG = LoggerFactory.getLogger(DomainManager.class);

  private final IdGenerator idGenerator;
  private final EntityStore entityStore;

  /**
   * Creates a domain manager.
   *
   * @param idGenerator The ID generator of new domains.
   * @param entityStore The entity store, which must support the domain operations.
   */
  public DomainManager(IdGenerator idGenerator, EntityStore entityStore) {
    this.idGenerator = idGenerator;
    this.entityStore = entityStore;
  }

  @Override
  public DomainEntity[] listDomainInfos(String metalake) {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);
    try {
      return entityStore
          .list(NamespaceUtil.ofDomain(metalake), DomainEntity.class, Entity.EntityType.DOMAIN)
          .toArray(new DomainEntity[0]);
    } catch (IOException ioe) {
      LOG.error("Failed to list domains under metalake {}", metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public DomainEntity getDomain(String metalake, String name) throws NoSuchDomainException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);
    try {
      return entityStore.get(
          NameIdentifierUtil.ofDomain(metalake, name),
          Entity.EntityType.DOMAIN,
          DomainEntity.class);
    } catch (NoSuchEntityException e) {
      throw noSuchDomain(e, metalake, name);
    } catch (IOException ioe) {
      LOG.error("Failed to get domain {} under metalake {}", name, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public DomainEntity createDomain(
      String metalake,
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties)
      throws DomainAlreadyExistsException {
    return insertDomain(metalake, null, name, comment, properties);
  }

  @Override
  public DomainEntity createSubdomain(
      String metalake,
      String parent,
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties)
      throws NoSuchDomainException, DomainAlreadyExistsException, IllegalArgumentException {
    checkParentName(name, parent);
    return insertDomain(metalake, parent, name, comment, properties);
  }

  @Override
  public DomainEntity alterDomain(String metalake, String name, DomainChange... changes)
      throws NoSuchDomainException, DomainAlreadyExistsException, IllegalArgumentException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);
    try {
      return entityStore.update(
          NameIdentifierUtil.ofDomain(metalake, name),
          DomainEntity.class,
          Entity.EntityType.DOMAIN,
          domain -> applyChanges(domain, changes));
    } catch (NoSuchEntityException e) {
      throw noSuchDomain(e, metalake, name);
    } catch (EntityAlreadyExistsException e) {
      String newName =
          RenameUtils.getLastRenameName(
              name,
              changes,
              DomainChange.RenameDomain.class,
              DomainChange.RenameDomain::getNewName);
      throw new DomainAlreadyExistsException(
          e, "Domain %s under metalake %s already exists", newName, metalake);
    } catch (IOException ioe) {
      LOG.error("Failed to alter domain {} under metalake {}", name, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public boolean deleteDomain(String metalake, String name) throws NonEmptyDomainException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);
    try {
      // In one transaction, the store refuses a domain with sub-domains, and removes the domain
      // with its own parent edge, its owner and the privileges on it. Its backing tag stays.
      return entityStore.delete(
          NameIdentifierUtil.ofDomain(metalake, name), Entity.EntityType.DOMAIN);
    } catch (NonEmptyEntityException e) {
      throw new NonEmptyDomainException(
          e, "Domain %s under metalake %s has sub-domains, remove them first", name, metalake);
    } catch (IOException ioe) {
      LOG.error("Failed to delete domain {} under metalake {}", name, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public DomainEntity publishDomain(String metalake, String name)
      throws NoSuchDomainException, IllegalArgumentException {
    return updateStatus(metalake, name, Domain.Status.PUBLISHED);
  }

  @Override
  public DomainEntity unpublishDomain(String metalake, String name)
      throws NoSuchDomainException, IllegalArgumentException {
    return updateStatus(metalake, name, Domain.Status.DRAFT);
  }

  @Override
  public DomainEntity[] listSubdomainInfos(String metalake, String name)
      throws NoSuchDomainException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);
    try {
      return domainOperations()
          .listSubDomains(NameIdentifierUtil.ofDomain(metalake, name))
          .toArray(new DomainEntity[0]);
    } catch (NoSuchEntityException e) {
      throw noSuchDomain(e, metalake, name);
    } catch (IOException ioe) {
      LOG.error("Failed to list sub-domains of domain {} under metalake {}", name, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public DomainEntity attachDomain(String metalake, String name, String parent)
      throws NoSuchDomainException, IllegalArgumentException {
    checkParentName(name, parent);
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);
    try {
      return domainOperations()
          .setDomainParentAndGet(
              NameIdentifierUtil.ofDomain(metalake, name),
              NameIdentifierUtil.ofDomain(metalake, parent),
              newAuditInfo(),
              DomainManager::checkAttach);
    } catch (NoSuchEntityException e) {
      throw new NoSuchDomainException(
          e,
          "Domain %s or parent %s under metalake %s does not exist: %s",
          name,
          parent,
          metalake,
          e.getMessage());
    } catch (IOException ioe) {
      LOG.error("Failed to attach domain {} to {} under metalake {}", name, parent, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public DomainEntity detachDomain(String metalake, String name) throws NoSuchDomainException {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);
    try {
      // Detaching breaks no rule of the hierarchy or the status, so nothing is checked.
      return domainOperations()
          .removeDomainParentAndGet(NameIdentifierUtil.ofDomain(metalake, name), state -> {});
    } catch (NoSuchEntityException e) {
      throw noSuchDomain(e, metalake, name);
    } catch (IOException ioe) {
      LOG.error("Failed to detach domain {} under metalake {}", name, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  /**
   * Checks a new sub-domain against its locked parent: the parent must be top-level, so that a
   * sub-domain has no sub-domains. A new domain has no sub-domains of its own and is a draft, so
   * the other rules of an attach hold for it.
   *
   * @param state The locked state of the insert.
   */
  @VisibleForTesting
  static void checkNewDomain(LockedDomainState state) {
    state.parent().ifPresent(parent -> checkParentIsTopLevel(state.domain(), parent));
  }

  /**
   * Checks an attach or a move against the locked domains. Attaching a domain to its current parent
   * changes nothing and is not checked.
   *
   * @param state The locked state of the attach.
   */
  @VisibleForTesting
  static void checkAttach(LockedDomainState state) {
    DomainEntity domain = state.domain();
    DomainEntity parent =
        state.parent().orElseThrow(() -> new IllegalStateException("An attach needs a parent"));
    // Two different names can find the same row, such as names that differ only in trailing
    // spaces on MySQL. The store refuses that before it locks; this checks the locked rows again,
    // so the rule does not depend on one store implementation.
    Preconditions.checkArgument(
        !Objects.equals(domain.id(), parent.id()),
        "Domain %s cannot be its own parent",
        domain.name());
    if (domain.parent().filter(parent.name()::equals).isPresent()) {
      return;
    }

    // A sub-domain cannot be a parent, or the hierarchy would be two levels deep.
    checkParentIsTopLevel(domain, parent);
    // A domain with sub-domains cannot become a sub-domain, for the same reason.
    Preconditions.checkArgument(
        state.subDomains().isEmpty(),
        "Domain %s has sub-domains %s and cannot be attached to another domain",
        domain.name(),
        names(state.subDomains()));
    // A published sub-domain needs a published parent, as when it is published.
    Preconditions.checkArgument(
        domain.status() != Domain.Status.PUBLISHED || parent.status() == Domain.Status.PUBLISHED,
        "Published domain %s cannot be attached to draft domain %s, publish %s first",
        domain.name(),
        parent.name(),
        parent.name());
  }

  /**
   * Checks a publish against the locked domains: a sub-domain needs a published parent.
   *
   * @param state The locked state of the status change.
   */
  @VisibleForTesting
  static void checkPublish(LockedDomainState state) {
    if (state.domain().status() == Domain.Status.PUBLISHED) {
      return;
    }
    state
        .parent()
        .ifPresent(
            parent ->
                Preconditions.checkArgument(
                    parent.status() == Domain.Status.PUBLISHED,
                    "Domain %s cannot be published while its parent %s is a draft",
                    state.domain().name(),
                    parent.name()));
  }

  /**
   * Checks an unpublish against the locked domains: no sub-domain may be published.
   *
   * @param state The locked state of the status change.
   */
  @VisibleForTesting
  static void checkUnpublish(LockedDomainState state) {
    if (state.domain().status() == Domain.Status.DRAFT) {
      return;
    }
    List<DomainEntity> publishedSubDomains =
        state.subDomains().stream()
            .filter(subDomain -> subDomain.status() == Domain.Status.PUBLISHED)
            .collect(Collectors.toList());
    Preconditions.checkArgument(
        publishedSubDomains.isEmpty(),
        "Domain %s cannot be unpublished while its sub-domains %s are published",
        state.domain().name(),
        names(publishedSubDomains));
  }

  private DomainEntity insertDomain(
      String metalake,
      @Nullable String parent,
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties) {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);
    // TODO: The next PR of #13884 sets the backing tag here: it creates a new tag or binds an
    // existing (tag, value), in the same transaction as the domain, under the binding rules.
    DomainEntity domain =
        DomainEntity.builder()
            .withId(idGenerator.nextId())
            .withName(name)
            .withNamespace(NamespaceUtil.ofDomain(metalake))
            .withComment(comment)
            .withProperties(properties == null ? Collections.emptyMap() : properties)
            .withStatus(Domain.Status.DRAFT)
            .withParent(parent)
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentPrincipal().getName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    try {
      // Return the parent's name as stored, read under its lock, rather than the requested one:
      // on MySQL, a name with trailing spaces finds the same parent row.
      String[] storedParent = new String[] {parent};
      domainOperations()
          .insertDomain(
              domain,
              state -> {
                checkNewDomain(state);
                state.parent().ifPresent(lockedParent -> storedParent[0] = lockedParent.name());
              });
      return Objects.equals(storedParent[0], parent)
          ? domain
          : domain.toBuilder().withParent(storedParent[0]).build();
    } catch (EntityAlreadyExistsException e) {
      throw new DomainAlreadyExistsException(
          e, "Domain %s under metalake %s already exists", name, metalake);
    } catch (NoSuchEntityException e) {
      if (parent == null) {
        throw new NoSuchMetalakeException(e, "Metalake %s does not exist", metalake);
      }
      // The metalake was checked, so the missing entity is most likely the parent.
      throw new NoSuchDomainException(
          e,
          "Parent domain %s under metalake %s does not exist: %s",
          parent,
          metalake,
          e.getMessage());
    } catch (IOException ioe) {
      LOG.error("Failed to create domain {} under metalake {}", name, metalake, ioe);
      throw new RuntimeException(ioe);
    }
  }

  private DomainEntity updateStatus(String metalake, String name, Domain.Status status) {
    checkMetalake(NameIdentifierUtil.ofMetalake(metalake), entityStore);
    try {
      return domainOperations()
          .updateDomainStatusAndGet(
              NameIdentifierUtil.ofDomain(metalake, name),
              status,
              newAuditInfo(),
              status == Domain.Status.PUBLISHED
                  ? DomainManager::checkPublish
                  : DomainManager::checkUnpublish);
    } catch (NoSuchEntityException e) {
      throw noSuchDomain(e, metalake, name);
    } catch (IOException ioe) {
      LOG.error(
          "Failed to change the status of domain {} under metalake {} to {}",
          name,
          metalake,
          status,
          ioe);
      throw new RuntimeException(ioe);
    }
  }

  private SupportsDomainOperations domainOperations() {
    return entityStore.domainOperations();
  }

  private static DomainEntity applyChanges(DomainEntity domain, DomainChange... changes) {
    String newName = domain.name();
    String newComment = domain.comment();
    Map<String, String> newProperties = new HashMap<>(domain.properties());

    for (DomainChange change : changes) {
      if (change instanceof DomainChange.RenameDomain) {
        newName = ((DomainChange.RenameDomain) change).getNewName();
      } else if (change instanceof DomainChange.UpdateDomainComment) {
        newComment = ((DomainChange.UpdateDomainComment) change).getNewComment();
      } else if (change instanceof DomainChange.SetProperty) {
        DomainChange.SetProperty setProperty = (DomainChange.SetProperty) change;
        newProperties.put(setProperty.getProperty(), setProperty.getValue());
      } else if (change instanceof DomainChange.RemoveProperty) {
        newProperties.remove(((DomainChange.RemoveProperty) change).getProperty());
      } else {
        throw new IllegalArgumentException("Unsupported domain change: " + change);
      }
    }

    Audit audit = domain.auditInfo();
    return domain.toBuilder()
        .withName(newName)
        .withComment(newComment)
        .withProperties(newProperties)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(audit.creator())
                .withCreateTime(audit.createTime())
                .withLastModifier(PrincipalUtils.getCurrentPrincipal().getName())
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }

  // The audit info of a change: a new parent edge takes its creator, a changed row its modifier.
  private static AuditInfo newAuditInfo() {
    String user = PrincipalUtils.getCurrentPrincipal().getName();
    Instant now = Instant.now();
    return AuditInfo.builder()
        .withCreator(user)
        .withCreateTime(now)
        .withLastModifier(user)
        .withLastModifiedTime(now)
        .build();
  }

  // A domain cannot be its own parent. Parent and child are always in the same metalake, since
  // both are looked up in the given one.
  private static void checkParentName(String name, @Nullable String parent) {
    Preconditions.checkArgument(parent != null, "The parent domain name must not be null");
    Preconditions.checkArgument(
        !Objects.equals(name, parent), "Domain %s cannot be its own parent", name);
  }

  private static void checkParentIsTopLevel(DomainEntity domain, DomainEntity parent) {
    Preconditions.checkArgument(
        !parent.parent().isPresent(),
        "Domain %s cannot be a sub-domain of %s, which is itself a sub-domain of %s",
        domain.name(),
        parent.name(),
        parent.parent().orElse(null));
  }

  private static List<String> names(List<DomainEntity> domains) {
    return domains.stream().map(DomainEntity::name).collect(Collectors.toList());
  }

  private static NoSuchDomainException noSuchDomain(
      NoSuchEntityException e, String metalake, String name) {
    return new NoSuchDomainException(
        e, "Domain %s under metalake %s does not exist", name, metalake);
  }
}
