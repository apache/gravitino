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
package org.apache.gravitino.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainMember;
import org.apache.gravitino.domain.DomainMemberFilter;
import org.apache.gravitino.domain.DomainRelation;
import org.apache.gravitino.dto.domain.DomainBackingTagDTO;
import org.apache.gravitino.dto.domain.DomainDTO;
import org.apache.gravitino.dto.domain.DomainMemberDTO;
import org.apache.gravitino.dto.domain.DomainRelationDTO;
import org.apache.gravitino.dto.requests.DomainBackingTagSetRequest;
import org.apache.gravitino.dto.requests.DomainCreateRequest;
import org.apache.gravitino.dto.requests.DomainParentSetRequest;
import org.apache.gravitino.dto.requests.DomainSetRequest;
import org.apache.gravitino.dto.responses.DomainListResponse;
import org.apache.gravitino.dto.responses.DomainMemberListResponse;
import org.apache.gravitino.dto.responses.DomainResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.tag.TagValue;

/** A domain snapshot with operations addressed by its name. */
class GenericDomain implements Domain {
  private final DomainDTO domain;
  private final RESTClient restClient;
  private final String metalake;

  GenericDomain(DomainDTO domain, RESTClient restClient, String metalake) {
    this.domain = domain;
    this.restClient = restClient;
    this.metalake = metalake;
  }

  /** {@inheritDoc} */
  @Override
  public String name() {
    return domain.getName();
  }

  /** {@inheritDoc} */
  @Override
  @Nullable
  public String comment() {
    return domain.getComment();
  }

  /** {@inheritDoc} */
  @Override
  public Map<String, String> properties() {
    return Collections.unmodifiableMap(domain.getProperties());
  }

  /** {@inheritDoc} */
  @Override
  public Status status() {
    return domain.getStatus();
  }

  /** {@inheritDoc} */
  @Override
  public Optional<String> parent() {
    return Optional.ofNullable(domain.getParent());
  }

  /** {@inheritDoc} */
  @Override
  public Optional<TagValue> backingTag() {
    return Optional.ofNullable(domain.getBackingTag()).map(DomainBackingTagDTO::tagValue);
  }

  /** {@inheritDoc} */
  @Override
  public Audit auditInfo() {
    return domain.getAudit();
  }

  /** {@inheritDoc} */
  @Override
  public DomainRelation[] relations() {
    return relations(domain.getRelations());
  }

  /** {@inheritDoc} */
  @Override
  public Domain publish() {
    return setStatus(Status.PUBLISHED);
  }

  /** {@inheritDoc} */
  @Override
  public Domain unpublish() {
    return setStatus(Status.DRAFT);
  }

  /** {@inheritDoc} */
  @Override
  public Domain attachToParent(String parent) {
    DomainParentSetRequest request = new DomainParentSetRequest(parent);
    request.validate();
    return snapshot(
        restClient.put(
            domainPath(metalake, name()) + "/parent",
            request,
            DomainResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.domainErrorHandler()));
  }

  /** {@inheritDoc} */
  @Override
  public Domain detachFromParent() {
    return snapshot(
        restClient.delete(
            domainPath(metalake, name()) + "/parent",
            DomainResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.domainErrorHandler()));
  }

  /** {@inheritDoc} */
  @Override
  public Domain updateBackingTag(@Nullable TagValue backingTag) {
    DomainBackingTagSetRequest request =
        new DomainBackingTagSetRequest(
            backingTag == null
                ? null
                : new DomainBackingTagDTO(backingTag.name(), backingTag.value().orElse(null)));
    request.validate();
    return snapshot(
        restClient.put(
            domainPath(metalake, name()) + "/backingTag",
            request,
            DomainResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.domainErrorHandler()));
  }

  /** {@inheritDoc} */
  @Override
  public Domain createSubdomain(
      String name,
      @Nullable String comment,
      @Nullable Map<String, String> properties,
      TagValue backingTag,
      boolean createBackingTag) {
    Preconditions.checkArgument(backingTag != null, "backingTag must not be null");
    DomainCreateRequest request =
        new DomainCreateRequest(
            name,
            comment,
            properties,
            name(),
            new DomainCreateRequest.BackingTag(
                backingTag.name(), backingTag.value().orElse(null), createBackingTag));
    request.validate();
    DomainResponse response =
        restClient.post(
            "api/metalakes/" + RESTUtils.encodeString(metalake) + "/domains",
            request,
            DomainResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.domainErrorHandler());
    response.validate();
    return new GenericDomain(response.getDomain(), restClient, metalake);
  }

  /** {@inheritDoc} */
  @Override
  public String[] listSubdomains() {
    NameListResponse response =
        restClient.get(
            domainPath(metalake, name()) + "/subdomains",
            NameListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.domainErrorHandler());
    response.validate();
    return response.getNames();
  }

  /** {@inheritDoc} */
  @Override
  public Domain[] listSubdomainsInfo() {
    DomainListResponse response =
        restClient.get(
            domainPath(metalake, name()) + "/subdomains",
            ImmutableMap.of("details", "true"),
            DomainListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.domainErrorHandler());
    response.validate();
    return Arrays.stream(response.getDomains())
        .map(dto -> new GenericDomain(dto, restClient, metalake))
        .toArray(Domain[]::new);
  }

  /** {@inheritDoc} */
  @Override
  public DomainMember[] listMembers(DomainMemberFilter filter) {
    Preconditions.checkArgument(filter != null, "filter must not be null");
    Map<String, String> params = new HashMap<>();
    if (!filter.types().isEmpty()) {
      params.put("types", filter.types().stream().map(Enum::name).collect(Collectors.joining(",")));
    }
    filter.nameContains().ifPresent(value -> params.put("nameContains", value));
    // Keep each scope as a separate parameter: commas are valid in domain names.
    String scopes =
        filter.domains().stream()
            .map(value -> "domains=" + RESTUtils.encodeString(value))
            .collect(Collectors.joining("&"));
    String requestPath =
        domainPath(metalake, name()) + "/objects" + (scopes.isEmpty() ? "" : "?" + scopes);
    DomainMemberListResponse response =
        restClient.get(
            requestPath,
            params,
            DomainMemberListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.domainErrorHandler());
    response.validate();
    return Arrays.stream(response.getMembers())
        .map(dto -> new GenericDomainMember(metalake, dto))
        .toArray(DomainMember[]::new);
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof GenericDomain)) {
      return false;
    }
    GenericDomain that = (GenericDomain) other;
    return metalake.equals(that.metalake)
        && name().equals(that.name())
        && Objects.equals(comment(), that.comment())
        && properties().equals(that.properties())
        && status() == that.status()
        && parent().equals(that.parent())
        && backingTag().equals(that.backingTag())
        && Objects.equals(auditInfo(), that.auditInfo())
        && Arrays.equals(relations(), that.relations());
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return Objects.hash(
        metalake,
        name(),
        comment(),
        properties(),
        status(),
        parent(),
        backingTag(),
        auditInfo(),
        Arrays.hashCode(relations()));
  }

  static String domainPath(String metalake, String name) {
    return "api/metalakes/"
        + RESTUtils.encodeString(metalake)
        + "/domains/"
        + RESTUtils.encodeDomainPathSegment(name);
  }

  private static DomainRelation[] relations(DomainRelationDTO[] relations) {
    return Arrays.stream(relations)
        .map(
            dto -> {
              switch (dto.getRelation()) {
                case DIRECT:
                  return DomainRelation.direct();
                case INHERITED:
                  return DomainRelation.inherited(dto.getSource());
                case VIA_SUBDOMAIN:
                  return DomainRelation.viaSubdomain(dto.getSource().name());
                default:
                  throw new IllegalArgumentException(
                      "Unknown domain relation: " + dto.getRelation());
              }
            })
        .toArray(DomainRelation[]::new);
  }

  private Domain setStatus(Status status) {
    return snapshot(
        restClient.patch(
            domainPath(metalake, name()),
            new DomainSetRequest(status),
            DomainResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.domainErrorHandler()));
  }

  private Domain snapshot(DomainResponse response) {
    response.validate();
    return new GenericDomain(response.getDomain(), restClient, metalake);
  }

  /** A member and its relations in the queried metalake. */
  @EqualsAndHashCode
  private static final class GenericDomainMember implements DomainMember {
    private final String metalake;
    private final MetadataObject object;
    private final DomainRelation[] relations;

    private GenericDomainMember(String metalake, DomainMemberDTO dto) {
      this.metalake = metalake;
      this.object = MetadataObjects.parse(dto.getObject().fullName(), dto.getObject().type());
      this.relations = GenericDomain.relations(dto.getRelations());
    }

    /** {@inheritDoc} */
    @Override
    public MetadataObject object() {
      return object;
    }

    /** {@inheritDoc} */
    @Override
    public DomainRelation[] relations() {
      return relations.clone();
    }
  }
}
