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

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainChange;
import org.apache.gravitino.domain.DomainMember;
import org.apache.gravitino.domain.DomainMemberFilter;
import org.apache.gravitino.domain.DomainRelation;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.domain.DomainBackingTagDTO;
import org.apache.gravitino.dto.domain.DomainDTO;
import org.apache.gravitino.dto.domain.DomainMemberDTO;
import org.apache.gravitino.dto.requests.DomainBackingTagSetRequest;
import org.apache.gravitino.dto.requests.DomainCreateRequest;
import org.apache.gravitino.dto.requests.DomainParentSetRequest;
import org.apache.gravitino.dto.requests.DomainSetRequest;
import org.apache.gravitino.dto.requests.DomainUpdateRequest;
import org.apache.gravitino.dto.requests.DomainUpdatesRequest;
import org.apache.gravitino.dto.responses.DomainListResponse;
import org.apache.gravitino.dto.responses.DomainMemberListResponse;
import org.apache.gravitino.dto.responses.DomainResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.exceptions.BackingTagAlreadyBoundException;
import org.apache.gravitino.exceptions.DomainAlreadyExistsException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.NonEmptyDomainException;
import org.apache.gravitino.exceptions.TagAlreadyExistsException;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.tag.TagValue;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.Parameter;

/** Tests the domain Java client against HTTP responses. */
public class TestDomainOperations extends TestBase {
  private static final String NAME = "Finance/Trading. APAC";
  private static final String ROOT = "/api/metalakes/metalake/domains";
  private static final String PATH = ROOT + "/" + RESTUtils.encodeString(NAME);
  private static final AuditDTO AUDIT =
      AuditDTO.builder()
          .withCreator("test")
          .withCreateTime(Instant.parse("2026-01-01T00:00:00Z"))
          .build();

  /**
   * Verifies domain client behavior.
   *
   * @throws Exception if HTTP mock setup fails
   */
  @Test
  public void testMetalakeOperations() throws Exception {
    GravitinoMetalake metalake = metalake();
    DomainDTO dto = dto(NAME, Domain.Status.DRAFT, null, TagValue.of("dept", "finance"));
    buildMockResource(
        Method.POST,
        ROOT,
        new DomainCreateRequest(
            NAME,
            "comment",
            Collections.emptyMap(),
            null,
            new DomainCreateRequest.BackingTag("dept", "finance", false)),
        new DomainResponse(dto),
        200);
    Domain created =
        metalake.createDomain(
            NAME, "comment", Collections.emptyMap(), TagValue.of("dept", "finance"), false);
    assertSnapshot(created, dto);
    buildMockResource(Method.GET, ROOT, null, new NameListResponse(new String[] {NAME}), 200);
    Assertions.assertArrayEquals(new String[] {NAME}, metalake.listDomains());
    buildMockResource(
        Method.GET,
        ROOT,
        Collections.singletonMap("details", "true"),
        null,
        new DomainListResponse(new DomainDTO[] {dto}),
        200);
    assertSnapshot(metalake.listDomainsInfo()[0], dto);
    buildMockResource(Method.GET, PATH, null, new DomainResponse(dto), 200);
    Domain loaded = metalake.getDomain(NAME);
    assertSnapshot(loaded, dto);
    Assertions.assertEquals(created, loaded);
    Assertions.assertEquals(created.hashCode(), loaded.hashCode());
    Assertions.assertNotEquals(
        loaded, new GenericDomain(dto, client.restClient(), "another-metalake"));
    Assertions.assertNotEquals(
        loaded,
        new GenericDomain(
            dto(NAME, Domain.Status.PUBLISHED, null, TagValue.of("dept", "finance")),
            client.restClient(),
            "metalake"));
    DomainUpdatesRequest request =
        new DomainUpdatesRequest(
            Arrays.asList(
                new DomainUpdateRequest.RenameDomainRequest("renamed"),
                new DomainUpdateRequest.UpdateDomainCommentRequest(null),
                new DomainUpdateRequest.SetDomainPropertyRequest("key", "value"),
                new DomainUpdateRequest.RemoveDomainPropertyRequest("old")));
    DomainDTO renamed = dto("renamed", Domain.Status.DRAFT, null, null);
    buildMockResource(Method.PUT, PATH, request, new DomainResponse(renamed), 200);
    Assertions.assertEquals(
        "renamed",
        metalake
            .alterDomain(
                NAME,
                DomainChange.rename("renamed"),
                DomainChange.updateComment(null),
                DomainChange.setProperty("key", "value"),
                DomainChange.removeProperty("old"))
            .name());
    buildMockResource(Method.DELETE, PATH, null, new DropResponse(true), 200);
    Assertions.assertTrue(metalake.deleteDomain(NAME));
    buildMockResource(Method.DELETE, PATH, null, new DropResponse(false), 200);
    Assertions.assertFalse(metalake.deleteDomain(NAME));
    Assertions.assertEquals(NAME, created.name());
  }

  /**
   * Verifies domain client behavior.
   *
   * @throws Exception if HTTP mock setup fails
   */
  @Test
  public void testSnapshotMutations() throws Exception {
    Domain original = domain();
    DomainDTO published = dto(NAME, Domain.Status.PUBLISHED, null, TagValue.noValue("dept"));
    buildMockResource(
        Method.PATCH,
        PATH,
        new DomainSetRequest(Domain.Status.PUBLISHED),
        new DomainResponse(published),
        200);
    Assertions.assertEquals(Domain.Status.PUBLISHED, original.publish().status());
    Assertions.assertEquals(Domain.Status.DRAFT, original.status());
    buildMockResource(
        Method.PATCH,
        PATH,
        new DomainSetRequest(Domain.Status.DRAFT),
        new DomainResponse(dto(NAME, Domain.Status.DRAFT, null, null)),
        200);
    Assertions.assertEquals(Domain.Status.DRAFT, original.unpublish().status());
    for (String parent : new String[] {"Parent/One. Asia", "Parent Two"}) {
      buildMockResource(
          Method.PUT,
          PATH + "/parent",
          new DomainParentSetRequest(parent),
          new DomainResponse(dto(NAME, Domain.Status.DRAFT, parent, null)),
          200);
      Assertions.assertEquals(parent, original.attachToParent(parent).parent().get());
      Assertions.assertFalse(original.parent().isPresent());
    }
    // Detach uses the endpoint even though this snapshot already shows no parent.
    buildMockResource(
        Method.DELETE,
        PATH + "/parent",
        null,
        new DomainResponse(dto(NAME, Domain.Status.DRAFT, null, null)),
        200);
    Assertions.assertFalse(original.detachFromParent().parent().isPresent());
    buildMockResource(
        Method.PUT,
        PATH + "/backingTag",
        new DomainBackingTagSetRequest(new DomainBackingTagDTO("new/tag", "value")),
        new DomainResponse(dto(NAME, Domain.Status.DRAFT, null, TagValue.of("new/tag", "value"))),
        200);
    Assertions.assertEquals(
        TagValue.of("new/tag", "value"),
        original.updateBackingTag(TagValue.of("new/tag", "value")).backingTag().get());
    buildMockResource(
        Method.PUT,
        PATH + "/backingTag",
        new DomainBackingTagSetRequest(null),
        new DomainResponse(dto(NAME, Domain.Status.DRAFT, null, null)),
        200);
    Assertions.assertFalse(original.updateBackingTag(null).backingTag().isPresent());
    Assertions.assertEquals(TagValue.noValue("dept"), original.backingTag().get());
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> original.properties().put("x", "y"));
  }

  /**
   * Verifies domain client behavior.
   *
   * @throws Exception if HTTP mock setup fails
   */
  @Test
  public void testSubdomains() throws Exception {
    Domain domain = domain();
    DomainDTO child = dto("Tax/Asia", Domain.Status.DRAFT, NAME, TagValue.noValue("tax"));
    buildMockResource(
        Method.POST,
        ROOT,
        new DomainCreateRequest(
            "Tax/Asia", null, null, NAME, new DomainCreateRequest.BackingTag("tax", null, true)),
        new DomainResponse(child),
        200);
    assertSnapshot(
        domain.createSubdomain("Tax/Asia", null, null, TagValue.noValue("tax"), true), child);
    buildMockResource(
        Method.GET,
        PATH + "/subdomains",
        null,
        new NameListResponse(new String[] {"Tax/Asia"}),
        200);
    Assertions.assertArrayEquals(new String[] {"Tax/Asia"}, domain.listSubdomains());
    buildMockResource(
        Method.GET,
        PATH + "/subdomains",
        Collections.singletonMap("details", "true"),
        null,
        new DomainListResponse(new DomainDTO[] {child}),
        200);
    assertSnapshot(domain.listSubdomainsInfo()[0], child);
  }

  /**
   * Verifies domain client behavior.
   *
   * @throws Exception if HTTP mock setup fails
   */
  @Test
  public void testMembersAndRepeatedScopes() throws Exception {
    Domain domain = domain();
    MetadataObject object = MetadataObjects.parse("cat.db.table", MetadataObject.Type.TABLE);
    DomainRelation[] relations = {
      DomainRelation.direct(),
      DomainRelation.inherited(MetadataObjects.parse("cat.db", MetadataObject.Type.SCHEMA)),
      DomainRelation.viaSubdomain("Tax/Asia. South")
    };
    DomainMember member =
        new DomainMember() {
          @Override
          public MetadataObject object() {
            return object;
          }

          @Override
          public DomainRelation[] relations() {
            return relations;
          }
        };
    DomainMemberListResponse response =
        new DomainMemberListResponse(new DomainMemberDTO[] {DomainMemberDTO.from(member)});
    mockServer
        .when(
            HttpRequest.request(PATH + "/objects")
                .withMethod("GET")
                .withQueryStringParameters(
                    new Parameter("domains", "Tax,Asia", "Trading/EMEA. West"),
                    new Parameter("types", "TABLE,VIEW"),
                    new Parameter("nameContains", "a/b +&.")),
            Times.exactly(1))
        .respond(
            HttpResponse.response()
                .withStatusCode(200)
                .withBody(MAPPER.writeValueAsString(response)));
    DomainMember[] members =
        domain.listMembers(
            DomainMemberFilter.builder()
                .withDomains("Tax,Asia", "Trading/EMEA. West")
                .withTypes(MetadataObject.Type.TABLE, MetadataObject.Type.VIEW)
                .withNameContains("a/b +&.")
                .build());
    buildMockResource(Method.GET, PATH + "/objects", null, response, 200);
    DomainMember reloaded = domain.listMembers(DomainMemberFilter.builder().build())[0];
    Assertions.assertEquals(members[0], reloaded);
    Assertions.assertEquals(members[0].hashCode(), reloaded.hashCode());
    Assertions.assertEquals(object, members[0].object());
    Assertions.assertTrue(Collections.singleton(object).contains(members[0].object()));
    Assertions.assertArrayEquals(relations, members[0].relations());
    members[0].relations()[0] = null;
    Assertions.assertArrayEquals(relations, members[0].relations());
    buildMockResource(
        Method.GET,
        PATH + "/objects",
        null,
        new DomainMemberListResponse(new DomainMemberDTO[0]),
        200);
    Assertions.assertEquals(0, domain.listMembers(DomainMemberFilter.builder().build()).length);
    Assertions.assertThrows(IllegalArgumentException.class, () -> domain.listMembers(null));
  }

  /**
   * Verifies domain client behavior.
   *
   * @throws Exception if HTTP mock setup fails
   */
  @Test
  public void testErrors() throws Exception {
    GravitinoMetalake metalake = metalake();
    buildMockResource(
        Method.GET,
        PATH,
        null,
        ErrorResponse.notFound("NoSuchDomainException", "draft hidden"),
        404);
    Assertions.assertThrows(NoSuchDomainException.class, () -> metalake.getDomain(NAME));
    buildMockResource(
        Method.GET, ROOT, null, ErrorResponse.notFound("NoSuchMetalakeException", "missing"), 404);
    Assertions.assertThrows(NoSuchMetalakeException.class, metalake::listDomains);
    buildMockResource(Method.PATCH, PATH, null, ErrorResponse.forbidden("denied", null), 403);
    Assertions.assertThrows(ForbiddenException.class, () -> domain().publish());
    buildMockResource(
        Method.DELETE,
        PATH,
        null,
        ErrorResponse.nonEmpty(NonEmptyDomainException.class.getSimpleName(), "children"),
        409);
    Assertions.assertThrows(NonEmptyDomainException.class, () -> metalake.deleteDomain(NAME));
    Class<?>[] conflicts = {
      DomainAlreadyExistsException.class,
      TagAlreadyExistsException.class,
      BackingTagAlreadyBoundException.class
    };
    for (Class<?> type : conflicts) {
      buildMockResource(
          Method.POST,
          ROOT,
          null,
          ErrorResponse.alreadyExists(type.getSimpleName(), "conflict"),
          409);
      Assertions.assertThrows(
          type.asSubclass(RuntimeException.class),
          () -> metalake.createDomain(NAME, null, null, TagValue.noValue("dept"), true));
    }
    buildMockResource(
        Method.PUT,
        PATH + "/backingTag",
        null,
        ErrorResponse.notFound("NoSuchTagException", "missing"),
        404);
    Assertions.assertThrows(
        NoSuchTagException.class, () -> domain().updateBackingTag(TagValue.noValue("missing")));
    buildMockResource(
        Method.PUT, PATH + "/parent", null, ErrorResponse.illegalArguments("invalid parent"), 400);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> domain().attachToParent("parent"));
  }

  /** Verifies that invalid requests are rejected before HTTP calls. */
  @Test
  public void testServerRejectsInvalidDomainName() throws Exception {
    buildMockResource(
        Method.GET, ROOT + "/SyStEm", null, ErrorResponse.illegalArguments("reserved domain"), 400);
    IllegalArgumentException error =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> metalake().getDomain("SyStEm"));
    Assertions.assertTrue(error.getMessage().contains("reserved domain"));
  }

  @Test
  public void testInvalidRequests() {
    GravitinoMetalake metalake = metalake();
    for (String segment : new String[] {".", ".."}) {
      MetadataObject object =
          new MetadataObjects.MetadataObjectImpl(null, segment, MetadataObject.Type.DOMAIN);
      Assertions.assertThrows(IllegalArgumentException.class, () -> metalake.getOwner(object));
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> new MetadataObjectTagOperations("metalake", object, client.restClient()));
      Assertions.assertThrows(IllegalArgumentException.class, () -> metalake.deleteDomain(segment));
      Domain snapshot =
          new GenericDomain(
              dto(segment, Domain.Status.DRAFT, null, null), client.restClient(), "metalake");
      Assertions.assertEquals(segment, snapshot.name());
      Assertions.assertThrows(IllegalArgumentException.class, snapshot::publish);
    }
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> metalake.alterDomain(NAME, (DomainChange[]) null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> metalake.alterDomain(NAME, (DomainChange) null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> metalake.getDomain(" "));
    Assertions.assertThrows(IllegalArgumentException.class, () -> metalake.deleteDomain(null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> metalake.createDomain(NAME, null, null, null, false));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> metalake.createDomain(NAME, null, null, TagValue.of("tag", "value"), true));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> domain().createSubdomain("child", null, null, null, true));
    Assertions.assertThrows(IllegalArgumentException.class, () -> domain().attachToParent(null));
  }

  private static GravitinoMetalake metalake() {
    return new GravitinoMetalake(
        "metalake", null, Collections.emptyMap(), AUDIT, null, client.restClient());
  }

  private static Domain domain() {
    return new GenericDomain(
        dto(NAME, Domain.Status.DRAFT, null, TagValue.noValue("dept")),
        client.restClient(),
        "metalake");
  }

  private static DomainDTO dto(
      String name, Domain.Status status, String parent, TagValue backingTag) {
    return new DomainDTO(
        name,
        "comment",
        Collections.singletonMap("key", "value"),
        status,
        parent,
        backingTag,
        AUDIT,
        new DomainRelation[0]);
  }

  private static void assertSnapshot(Domain actual, DomainDTO expected) {
    Assertions.assertEquals(expected.getName(), actual.name());
    Assertions.assertEquals(expected.getComment(), actual.comment());
    Assertions.assertEquals(expected.getProperties(), actual.properties());
    Assertions.assertEquals(expected.getStatus(), actual.status());
    Assertions.assertEquals(expected.getParent(), actual.parent().orElse(null));
    Assertions.assertEquals(
        expected.getBackingTag() == null ? null : expected.getBackingTag().tagValue(),
        actual.backingTag().orElse(null));
    Assertions.assertEquals(expected.getAudit(), actual.auditInfo());
    Assertions.assertEquals(0, actual.relations().length);
  }
}
