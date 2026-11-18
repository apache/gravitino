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
package org.apache.gravitino.listener;

import java.util.Map;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.SupportsDomainOperations.LockedDomainState;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainChange;
import org.apache.gravitino.domain.DomainDispatcher;
import org.apache.gravitino.domain.DomainMember;
import org.apache.gravitino.domain.DomainMemberFilter;
import org.apache.gravitino.listener.api.event.domain.AlterDomainEvent;
import org.apache.gravitino.listener.api.event.domain.AlterDomainFailureEvent;
import org.apache.gravitino.listener.api.event.domain.AlterDomainPreEvent;
import org.apache.gravitino.listener.api.event.domain.AttachDomainEvent;
import org.apache.gravitino.listener.api.event.domain.AttachDomainFailureEvent;
import org.apache.gravitino.listener.api.event.domain.AttachDomainPreEvent;
import org.apache.gravitino.listener.api.event.domain.CreateDomainEvent;
import org.apache.gravitino.listener.api.event.domain.CreateDomainFailureEvent;
import org.apache.gravitino.listener.api.event.domain.CreateDomainPreEvent;
import org.apache.gravitino.listener.api.event.domain.DeleteDomainEvent;
import org.apache.gravitino.listener.api.event.domain.DeleteDomainFailureEvent;
import org.apache.gravitino.listener.api.event.domain.DeleteDomainPreEvent;
import org.apache.gravitino.listener.api.event.domain.DetachDomainEvent;
import org.apache.gravitino.listener.api.event.domain.DetachDomainFailureEvent;
import org.apache.gravitino.listener.api.event.domain.DetachDomainPreEvent;
import org.apache.gravitino.listener.api.event.domain.GetDomainEvent;
import org.apache.gravitino.listener.api.event.domain.GetDomainFailureEvent;
import org.apache.gravitino.listener.api.event.domain.GetDomainPreEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainMembersEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainMembersFailureEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainMembersPreEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsFailureEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsForMetadataObjectEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsForMetadataObjectFailureEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsForMetadataObjectPreEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsInfoEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsInfoFailureEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsInfoPreEvent;
import org.apache.gravitino.listener.api.event.domain.ListDomainsPreEvent;
import org.apache.gravitino.listener.api.event.domain.ListSubdomainsEvent;
import org.apache.gravitino.listener.api.event.domain.ListSubdomainsFailureEvent;
import org.apache.gravitino.listener.api.event.domain.ListSubdomainsInfoEvent;
import org.apache.gravitino.listener.api.event.domain.ListSubdomainsInfoFailureEvent;
import org.apache.gravitino.listener.api.event.domain.ListSubdomainsInfoPreEvent;
import org.apache.gravitino.listener.api.event.domain.ListSubdomainsPreEvent;
import org.apache.gravitino.listener.api.event.domain.PublishDomainEvent;
import org.apache.gravitino.listener.api.event.domain.PublishDomainFailureEvent;
import org.apache.gravitino.listener.api.event.domain.PublishDomainPreEvent;
import org.apache.gravitino.listener.api.event.domain.UnpublishDomainEvent;
import org.apache.gravitino.listener.api.event.domain.UnpublishDomainFailureEvent;
import org.apache.gravitino.listener.api.event.domain.UnpublishDomainPreEvent;
import org.apache.gravitino.listener.api.event.domain.UpdateDomainBackingTagEvent;
import org.apache.gravitino.listener.api.event.domain.UpdateDomainBackingTagFailureEvent;
import org.apache.gravitino.listener.api.event.domain.UpdateDomainBackingTagPreEvent;
import org.apache.gravitino.listener.api.info.DomainInfo;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.tag.TagValue;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * Dispatches domain events around the existing hook/manager chain.
 *
 * <p>Events describe dispatcher operations. REST visibility and member-access filtering remain at
 * the server boundary, so list counts describe dispatcher results, not necessarily HTTP results.
 * Internal storage and authorization lookups use the undecorated internal dispatcher.
 */
public final class DomainEventDispatcher implements DomainDispatcher {
  private final EventBus eventBus;
  private final DomainDispatcher dispatcher;

  /**
   * Creates the domain event decorator.
   *
   * @param eventBus The existing event bus.
   * @param dispatcher The domain hook/manager chain.
   */
  public DomainEventDispatcher(EventBus eventBus, DomainDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  /** {@inheritDoc} */
  @Override
  public DomainDispatcher withMutationCheck(Consumer<LockedDomainState> check) {
    return new DomainEventDispatcher(eventBus, dispatcher.withMutationCheck(check));
  }

  /** {@inheritDoc} */
  @Override
  public String[] listDomains(String metalake) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new ListDomainsPreEvent(user, metalake));
    try {
      String[] result = dispatcher.listDomains(metalake);
      eventBus.dispatchEvent(
          new ListDomainsEvent(user, metalake, result == null ? -1 : result.length));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListDomainsFailureEvent(user, metalake, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public DomainEntity[] listDomainInfos(String metalake) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new ListDomainsInfoPreEvent(user, metalake));
    try {
      DomainEntity[] result = dispatcher.listDomainInfos(metalake);
      eventBus.dispatchEvent(
          new ListDomainsInfoEvent(user, metalake, result == null ? -1 : result.length));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListDomainsInfoFailureEvent(user, metalake, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public DomainEntity getDomain(String metalake, String name) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new GetDomainPreEvent(user, metalake, name));
    try {
      DomainEntity result = dispatcher.getDomain(metalake, name);
      eventBus.dispatchEvent(new GetDomainEvent(user, metalake, name, domainInfo(result)));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(new GetDomainFailureEvent(user, metalake, name, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public DomainEntity createDomain(
      String metalake,
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      TagValue backingTag,
      boolean createBackingTag) {
    String user = PrincipalUtils.getCurrentUserName();
    DomainInfo requested =
        new DomainInfo(name, comment, properties, Domain.Status.DRAFT, null, backingTag, null);
    eventBus.dispatchEvent(
        new CreateDomainPreEvent(user, metalake, name, requested, createBackingTag));
    try {
      DomainEntity result =
          dispatcher.createDomain(
              metalake, name, comment, properties, backingTag, createBackingTag);
      eventBus.dispatchEvent(
          new CreateDomainEvent(user, metalake, name, domainInfo(result), createBackingTag));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new CreateDomainFailureEvent(user, metalake, name, requested, createBackingTag, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public DomainEntity createSubdomain(
      String metalake,
      String parent,
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      TagValue backingTag,
      boolean createBackingTag) {
    String user = PrincipalUtils.getCurrentUserName();
    DomainInfo requested =
        new DomainInfo(name, comment, properties, Domain.Status.DRAFT, parent, backingTag, null);
    eventBus.dispatchEvent(
        new CreateDomainPreEvent(user, metalake, name, requested, createBackingTag));
    try {
      DomainEntity result =
          dispatcher.createSubdomain(
              metalake, parent, name, comment, properties, backingTag, createBackingTag);
      eventBus.dispatchEvent(
          new CreateDomainEvent(user, metalake, name, domainInfo(result), createBackingTag));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new CreateDomainFailureEvent(user, metalake, name, requested, createBackingTag, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public DomainEntity alterDomain(String metalake, String name, DomainChange... changes) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new AlterDomainPreEvent(user, metalake, name, changes));
    try {
      DomainEntity result = dispatcher.alterDomain(metalake, name, changes);
      eventBus.dispatchEvent(
          new AlterDomainEvent(user, metalake, name, changes, domainInfo(result)));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(new AlterDomainFailureEvent(user, metalake, name, changes, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteDomain(String metalake, String name) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new DeleteDomainPreEvent(user, metalake, name));
    try {
      boolean result = dispatcher.deleteDomain(metalake, name);
      eventBus.dispatchEvent(new DeleteDomainEvent(user, metalake, name, result));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(new DeleteDomainFailureEvent(user, metalake, name, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public DomainEntity publishDomain(String metalake, String name) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new PublishDomainPreEvent(user, metalake, name));
    try {
      DomainEntity result = dispatcher.publishDomain(metalake, name);
      eventBus.dispatchEvent(new PublishDomainEvent(user, metalake, name, domainInfo(result)));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(new PublishDomainFailureEvent(user, metalake, name, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public DomainEntity unpublishDomain(String metalake, String name) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new UnpublishDomainPreEvent(user, metalake, name));
    try {
      DomainEntity result = dispatcher.unpublishDomain(metalake, name);
      eventBus.dispatchEvent(new UnpublishDomainEvent(user, metalake, name, domainInfo(result)));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(new UnpublishDomainFailureEvent(user, metalake, name, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public DomainEntity detachDomain(String metalake, String name) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new DetachDomainPreEvent(user, metalake, name));
    try {
      DomainEntity result = dispatcher.detachDomain(metalake, name);
      eventBus.dispatchEvent(new DetachDomainEvent(user, metalake, name, domainInfo(result)));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(new DetachDomainFailureEvent(user, metalake, name, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public DomainEntity attachDomain(String metalake, String name, String parent) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new AttachDomainPreEvent(user, metalake, name, parent));
    try {
      DomainEntity result = dispatcher.attachDomain(metalake, name, parent);
      eventBus.dispatchEvent(
          new AttachDomainEvent(user, metalake, name, parent, domainInfo(result)));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(new AttachDomainFailureEvent(user, metalake, name, parent, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public DomainEntity updateBackingTag(
      String metalake, String name, @Nullable TagValue backingTag) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new UpdateDomainBackingTagPreEvent(user, metalake, name, backingTag));
    try {
      DomainEntity result = dispatcher.updateBackingTag(metalake, name, backingTag);
      eventBus.dispatchEvent(
          new UpdateDomainBackingTagEvent(user, metalake, name, backingTag, domainInfo(result)));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new UpdateDomainBackingTagFailureEvent(user, metalake, name, backingTag, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public String[] listSubdomains(String metalake, String name) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new ListSubdomainsPreEvent(user, metalake, name));
    try {
      String[] result = dispatcher.listSubdomains(metalake, name);
      eventBus.dispatchEvent(
          new ListSubdomainsEvent(user, metalake, name, result == null ? -1 : result.length));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListSubdomainsFailureEvent(user, metalake, name, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public DomainEntity[] listSubdomainInfos(String metalake, String name) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new ListSubdomainsInfoPreEvent(user, metalake, name));
    try {
      DomainEntity[] result = dispatcher.listSubdomainInfos(metalake, name);
      eventBus.dispatchEvent(
          new ListSubdomainsInfoEvent(user, metalake, name, result == null ? -1 : result.length));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListSubdomainsInfoFailureEvent(user, metalake, name, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public DomainMember[] listMembers(String metalake, String name, DomainMemberFilter filter) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new ListDomainMembersPreEvent(user, metalake, name, filter));
    try {
      DomainMember[] result = dispatcher.listMembers(metalake, name, filter);
      eventBus.dispatchEvent(
          new ListDomainMembersEvent(
              user, metalake, name, filter, result == null ? -1 : result.length));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListDomainMembersFailureEvent(user, metalake, name, filter, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public DomainMember[] listMembers(
      String metalake, DomainEntity domain, DomainEntity[] subdomains, DomainMemberFilter filter) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new ListDomainMembersPreEvent(user, metalake, domain.name(), filter));
    try {
      DomainMember[] result = dispatcher.listMembers(metalake, domain, subdomains, filter);
      eventBus.dispatchEvent(
          new ListDomainMembersEvent(
              user, metalake, domain.name(), filter, result == null ? -1 : result.length));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListDomainMembersFailureEvent(user, metalake, domain.name(), filter, e));
      throw e;
    }
  }

  /** {@inheritDoc} */
  @Override
  public DomainEntity[] listDomainsForMetadataObject(String metalake, MetadataObject object) {
    String user = PrincipalUtils.getCurrentUserName();
    eventBus.dispatchEvent(new ListDomainsForMetadataObjectPreEvent(user, metalake, object));
    try {
      DomainEntity[] result = dispatcher.listDomainsForMetadataObject(metalake, object);
      eventBus.dispatchEvent(
          new ListDomainsForMetadataObjectEvent(
              user, metalake, object, result == null ? -1 : result.length));
      return result;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListDomainsForMetadataObjectFailureEvent(user, metalake, object, e));
      throw e;
    }
  }

  private static DomainInfo domainInfo(DomainEntity domain) {
    return new DomainInfo(
        domain.name(),
        domain.comment(),
        domain.properties(),
        domain.status(),
        domain.parent().orElse(null),
        domain.backingTag().orElse(null),
        domain.auditInfo());
  }
}
