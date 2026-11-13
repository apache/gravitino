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

import java.util.Map;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.SupportsDomainOperations.LockedDomainState;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.NameValidationUtils;

/** Validates domain names before dispatching domain operations. */
public final class DomainNormalizeDispatcher implements DomainDispatcher {

  private final DomainDispatcher dispatcher;

  /**
   * Creates the domain name validation dispatcher.
   *
   * @param dispatcher The internal dispatcher.
   */
  public DomainNormalizeDispatcher(DomainDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public DomainDispatcher withMutationCheck(Consumer<LockedDomainState> check) {
    return new DomainNormalizeDispatcher(dispatcher.withMutationCheck(check));
  }

  @Override
  public DomainEntity[] listDomainInfos(String metalake) {
    return dispatcher.listDomainInfos(metalake);
  }

  @Override
  public DomainEntity getDomain(String metalake, String name) {
    NameValidationUtils.validateName("domain", name);
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
    NameValidationUtils.validateName("domain", name);
    return dispatcher.createDomain(
        metalake, name, comment, properties, backingTag, createBackingTag);
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
    NameValidationUtils.validateName("domain", name);
    NameValidationUtils.validateName("domain", parent);
    return dispatcher.createSubdomain(
        metalake, parent, name, comment, properties, backingTag, createBackingTag);
  }

  @Override
  public DomainEntity alterDomain(String metalake, String name, DomainChange... changes) {
    NameValidationUtils.validateName("domain", name);
    for (DomainChange change : changes) {
      if (change instanceof DomainChange.RenameDomain) {
        NameValidationUtils.validateName(
            "domain", ((DomainChange.RenameDomain) change).getNewName());
      }
    }
    return dispatcher.alterDomain(metalake, name, changes);
  }

  @Override
  public boolean deleteDomain(String metalake, String name) {
    NameValidationUtils.validateName("domain", name);
    return dispatcher.deleteDomain(metalake, name);
  }

  @Override
  public DomainEntity publishDomain(String metalake, String name) {
    NameValidationUtils.validateName("domain", name);
    return dispatcher.publishDomain(metalake, name);
  }

  @Override
  public DomainEntity unpublishDomain(String metalake, String name) {
    NameValidationUtils.validateName("domain", name);
    return dispatcher.unpublishDomain(metalake, name);
  }

  @Override
  public DomainEntity updateBackingTag(
      String metalake, String name, @Nullable TagValue backingTag) {
    NameValidationUtils.validateName("domain", name);
    return dispatcher.updateBackingTag(metalake, name, backingTag);
  }

  @Override
  public DomainEntity[] listSubdomainInfos(String metalake, String name) {
    NameValidationUtils.validateName("domain", name);
    return dispatcher.listSubdomainInfos(metalake, name);
  }

  @Override
  public DomainMember[] listMembers(String metalake, String name, DomainMemberFilter filter) {
    NameValidationUtils.validateName("domain", name);
    NameValidationUtils.validateDomainNames(filter.domains());
    return dispatcher.listMembers(metalake, name, filter);
  }

  @Override
  public DomainMember[] listMembers(
      String metalake, DomainEntity domain, DomainEntity[] subdomains, DomainMemberFilter filter) {
    NameValidationUtils.validateDomainNames(filter.domains());
    return dispatcher.listMembers(metalake, domain, subdomains, filter);
  }

  @Override
  public DomainEntity[] listDomainsForMetadataObject(String metalake, MetadataObject object) {
    return dispatcher.listDomainsForMetadataObject(metalake, object);
  }

  @Override
  public DomainEntity attachDomain(String metalake, String name, String parent) {
    NameValidationUtils.validateName("domain", name);
    NameValidationUtils.validateName("domain", parent);
    return dispatcher.attachDomain(metalake, name, parent);
  }

  @Override
  public DomainEntity detachDomain(String metalake, String name) {
    NameValidationUtils.validateName("domain", name);
    return dispatcher.detachDomain(metalake, name);
  }
}
