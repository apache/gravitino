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
package org.apache.gravitino.hook;

import java.util.Map;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.SupportsDomainOperations.LockedDomainState;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.domain.DomainChange;
import org.apache.gravitino.domain.DomainDispatcher;
import org.apache.gravitino.domain.DomainMember;
import org.apache.gravitino.domain.DomainMemberFilter;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/** Creator ownership and authorization cache lifecycle hooks for domains. */
public final class DomainHookDispatcher implements DomainDispatcher {

  private final DomainDispatcher dispatcher;

  /**
   * Creates the domain hooks.
   *
   * @param dispatcher The internal dispatcher.
   */
  public DomainHookDispatcher(DomainDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public DomainDispatcher withMutationCheck(Consumer<LockedDomainState> check) {
    return new DomainHookDispatcher(dispatcher.withMutationCheck(check));
  }

  @Override
  public DomainEntity[] listDomainInfos(String metalake) {
    return dispatcher.listDomainInfos(metalake);
  }

  @Override
  public DomainEntity getDomain(String metalake, String name) {
    return dispatcher.getDomain(metalake, name);
  }

  @Override
  public DomainEntity createDomain(
      String metalake,
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      TagValue backingTag,
      boolean createBackingTag) {
    DomainEntity created =
        dispatcher.createDomain(metalake, name, comment, properties, backingTag, createBackingTag);
    setOwners(metalake, created, backingTag, createBackingTag);
    return created;
  }

  @Override
  public DomainEntity createSubdomain(
      String metalake,
      String parent,
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      TagValue backingTag,
      boolean createBackingTag) {
    DomainEntity created =
        dispatcher.createSubdomain(
            metalake, parent, name, comment, properties, backingTag, createBackingTag);
    setOwners(metalake, created, backingTag, createBackingTag);
    return created;
  }

  @Override
  public DomainEntity alterDomain(String metalake, String name, DomainChange... changes) {
    DomainEntity domain = dispatcher.alterDomain(metalake, name, changes);
    if (!name.equals(domain.name())) {
      AuthorizationUtils.notifyEntityNameIdMappingChange(
          NameIdentifierUtil.ofDomain(metalake, name), Entity.EntityType.DOMAIN);
    }
    return domain;
  }

  @Override
  public boolean deleteDomain(String metalake, String name) {
    boolean deleted = dispatcher.deleteDomain(metalake, name);
    if (deleted) {
      AuthorizationUtils.notifyEntityNameIdMappingChange(
          NameIdentifierUtil.ofDomain(metalake, name), Entity.EntityType.DOMAIN);
    }
    return deleted;
  }

  @Override
  public DomainEntity publishDomain(String metalake, String name) {
    return dispatcher.publishDomain(metalake, name);
  }

  @Override
  public DomainEntity unpublishDomain(String metalake, String name) {
    return dispatcher.unpublishDomain(metalake, name);
  }

  @Override
  public DomainEntity updateBackingTag(
      String metalake, String name, @Nullable TagValue backingTag) {
    return dispatcher.updateBackingTag(metalake, name, backingTag);
  }

  @Override
  public DomainEntity[] listSubdomainInfos(String metalake, String name) {
    return dispatcher.listSubdomainInfos(metalake, name);
  }

  @Override
  public DomainMember[] listMembers(String metalake, String name, DomainMemberFilter filter) {
    return dispatcher.listMembers(metalake, name, filter);
  }

  @Override
  public DomainMember[] listMembers(
      String metalake, DomainEntity domain, DomainEntity[] subdomains, DomainMemberFilter filter) {
    return dispatcher.listMembers(metalake, domain, subdomains, filter);
  }

  @Override
  public DomainEntity[] listDomainsForMetadataObject(String metalake, MetadataObject object) {
    return dispatcher.listDomainsForMetadataObject(metalake, object);
  }

  @Override
  public DomainEntity attachDomain(String metalake, String name, String parent) {
    return dispatcher.attachDomain(metalake, name, parent);
  }

  @Override
  public DomainEntity detachDomain(String metalake, String name) {
    return dispatcher.detachDomain(metalake, name);
  }

  private void setOwners(String metalake, DomainEntity domain, TagValue tag, boolean createdTag) {
    OwnerDispatcher owners = GravitinoEnv.getInstance().internalOwnerDispatcher();
    if (owners != null) {
      owners.setOwner(
          metalake,
          MetadataObjects.of(null, domain.name(), MetadataObject.Type.DOMAIN),
          PrincipalUtils.getCurrentUserName(),
          Owner.Type.USER);
      if (createdTag) {
        owners.setOwner(
            metalake,
            MetadataObjects.of(null, tag.name(), MetadataObject.Type.TAG),
            PrincipalUtils.getCurrentUserName(),
            Owner.Type.USER);
      }
    }
  }
}
