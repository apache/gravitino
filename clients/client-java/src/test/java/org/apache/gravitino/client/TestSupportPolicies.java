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

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;
import static org.apache.hc.core5.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.SchemaDTO;
import org.apache.gravitino.dto.file.FilesetDTO;
import org.apache.gravitino.dto.messaging.TopicDTO;
import org.apache.gravitino.dto.policy.PolicyContentDTO;
import org.apache.gravitino.dto.policy.PolicyDTO;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.dto.requests.PoliciesAssociateRequest;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.PolicyListResponse;
import org.apache.gravitino.dto.responses.PolicyResponse;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.types.Types;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSupportPolicies extends TestBase {

  private static final String METALAKE_NAME = "metalake";

  private static Catalog relationalCatalog;

  private static Catalog filesetCatalog;

  private static Catalog messagingCatalog;

  private static Schema genericSchema;

  private static Table relationalTable;

  private static Fileset genericFileset;

  private static Topic genericTopic;

  private final Set<MetadataObject.Type> supportedObjectTypes =
      Collections.singleton(MetadataObject.Type.FILESET);
  private final PolicyContentDTO.CustomContentDTO contentDTO =
      PolicyContentDTO.CustomContentDTO.builder()
          .withSupportedObjectTypes(supportedObjectTypes)
          .build();

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();
    TestGravitinoMetalake.createMetalake(client, METALAKE_NAME);

    relationalCatalog =
        new RelationalCatalog(
            Namespace.of(METALAKE_NAME),
            "catalog1",
            Catalog.Type.RELATIONAL,
            "test",
            "comment",
            Collections.emptyMap(),
            AuditDTO.builder().build(),
            client.restClient());

    filesetCatalog =
        new FilesetCatalog(
            Namespace.of(METALAKE_NAME),
            "catalog2",
            Catalog.Type.FILESET,
            "test",
            "comment",
            Collections.emptyMap(),
            AuditDTO.builder().build(),
            client.restClient());

    messagingCatalog =
        new MessagingCatalog(
            Namespace.of(METALAKE_NAME),
            "catalog3",
            Catalog.Type.MESSAGING,
            "test",
            "comment",
            Collections.emptyMap(),
            AuditDTO.builder().build(),
            client.restClient());

    genericSchema =
        new GenericSchema(
            SchemaDTO.builder()
                .withName("schema1")
                .withComment("comment1")
                .withProperties(Collections.emptyMap())
                .withAudit(AuditDTO.builder().withCreator("test").build())
                .build(),
            client.restClient(),
            METALAKE_NAME,
            "catalog1");

    relationalTable =
        RelationalTable.from(
            Namespace.of(METALAKE_NAME, "catalog1", "schema1"),
            TableDTO.builder()
                .withName("table1")
                .withComment("comment1")
                .withColumns(
                    new ColumnDTO[] {
                      ColumnDTO.builder()
                          .withName("col1")
                          .withDataType(Types.IntegerType.get())
                          .build()
                    })
                .withProperties(Collections.emptyMap())
                .withAudit(AuditDTO.builder().withCreator("test").build())
                .build(),
            client.restClient());

    genericFileset =
        new GenericFileset(
            FilesetDTO.builder()
                .name("fileset1")
                .comment("comment1")
                .type(Fileset.Type.EXTERNAL)
                .storageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, "s3://bucket/path"))
                .properties(Collections.emptyMap())
                .audit(AuditDTO.builder().withCreator("test").build())
                .build(),
            client.restClient(),
            Namespace.of(METALAKE_NAME, "catalog1", "schema1"));

    genericTopic =
        new GenericTopic(
            TopicDTO.builder()
                .withName("topic1")
                .withComment("comment1")
                .withProperties(Collections.emptyMap())
                .withAudit(AuditDTO.builder().withCreator("test").build())
                .build(),
            client.restClient(),
            Namespace.of(METALAKE_NAME, "catalog1", "schema1"));
  }

  @Test
  public void testListPoliciesForCatalog() throws JsonProcessingException {
    testListPolicies(
        relationalCatalog.supportsPolicies(),
        MetadataObjects.of(null, relationalCatalog.name(), MetadataObject.Type.CATALOG));

    testListPolicies(
        filesetCatalog.supportsPolicies(),
        MetadataObjects.of(null, filesetCatalog.name(), MetadataObject.Type.CATALOG));

    testListPolicies(
        messagingCatalog.supportsPolicies(),
        MetadataObjects.of(null, messagingCatalog.name(), MetadataObject.Type.CATALOG));
  }

  @Test
  public void testListPoliciesForSchema() throws JsonProcessingException {
    testListPolicies(
        genericSchema.supportsPolicies(),
        MetadataObjects.of("catalog1", genericSchema.name(), MetadataObject.Type.SCHEMA));
  }

  @Test
  public void testListPoliciesForTable() throws JsonProcessingException {
    testListPolicies(
        relationalTable.supportsPolicies(),
        MetadataObjects.of("catalog1.schema1", relationalTable.name(), MetadataObject.Type.TABLE));
  }

  @Test
  public void testListPoliciesForFileset() throws JsonProcessingException {
    testListPolicies(
        genericFileset.supportsPolicies(),
        MetadataObjects.of("catalog1.schema1", genericFileset.name(), MetadataObject.Type.FILESET));
  }

  @Test
  public void testListPoliciesForTopic() throws JsonProcessingException {
    testListPolicies(
        genericTopic.supportsPolicies(),
        MetadataObjects.of("catalog1.schema1", genericTopic.name(), MetadataObject.Type.TOPIC));
  }

  @Test
  public void testListPoliciesInfoForCatalog() throws JsonProcessingException {
    testListPoliciesInfo(
        relationalCatalog.supportsPolicies(),
        MetadataObjects.of(null, relationalCatalog.name(), MetadataObject.Type.CATALOG));

    testListPoliciesInfo(
        filesetCatalog.supportsPolicies(),
        MetadataObjects.of(null, filesetCatalog.name(), MetadataObject.Type.CATALOG));

    testListPoliciesInfo(
        messagingCatalog.supportsPolicies(),
        MetadataObjects.of(null, messagingCatalog.name(), MetadataObject.Type.CATALOG));
  }

  @Test
  public void testListPoliciesInfoForSchema() throws JsonProcessingException {
    testListPoliciesInfo(
        genericSchema.supportsPolicies(),
        MetadataObjects.of("catalog1", genericSchema.name(), MetadataObject.Type.SCHEMA));
  }

  @Test
  public void testListPoliciesInfoForTable() throws JsonProcessingException {
    testListPoliciesInfo(
        relationalTable.supportsPolicies(),
        MetadataObjects.of("catalog1.schema1", relationalTable.name(), MetadataObject.Type.TABLE));
  }

  @Test
  public void testListPoliciesInfoForFileset() throws JsonProcessingException {
    testListPoliciesInfo(
        genericFileset.supportsPolicies(),
        MetadataObjects.of("catalog1.schema1", genericFileset.name(), MetadataObject.Type.FILESET));
  }

  @Test
  public void testListPoliciesInfoForTopic() throws JsonProcessingException {
    testListPoliciesInfo(
        genericTopic.supportsPolicies(),
        MetadataObjects.of("catalog1.schema1", genericTopic.name(), MetadataObject.Type.TOPIC));
  }

  @Test
  public void testGetPolicyForCatalog() throws JsonProcessingException {
    testGetPolicy(
        relationalCatalog.supportsPolicies(),
        MetadataObjects.of(null, relationalCatalog.name(), MetadataObject.Type.CATALOG));

    testGetPolicy(
        filesetCatalog.supportsPolicies(),
        MetadataObjects.of(null, filesetCatalog.name(), MetadataObject.Type.CATALOG));

    testGetPolicy(
        messagingCatalog.supportsPolicies(),
        MetadataObjects.of(null, messagingCatalog.name(), MetadataObject.Type.CATALOG));
  }

  @Test
  public void testGetPolicyForSchema() throws JsonProcessingException {
    testGetPolicy(
        genericSchema.supportsPolicies(),
        MetadataObjects.of("catalog1", genericSchema.name(), MetadataObject.Type.SCHEMA));
  }

  @Test
  public void testGetPolicyForTable() throws JsonProcessingException {
    testGetPolicy(
        relationalTable.supportsPolicies(),
        MetadataObjects.of("catalog1.schema1", relationalTable.name(), MetadataObject.Type.TABLE));
  }

  @Test
  public void testGetPolicyForFileset() throws JsonProcessingException {
    testGetPolicy(
        genericFileset.supportsPolicies(),
        MetadataObjects.of("catalog1.schema1", genericFileset.name(), MetadataObject.Type.FILESET));
  }

  @Test
  public void testGetPolicyForTopic() throws JsonProcessingException {
    testGetPolicy(
        genericTopic.supportsPolicies(),
        MetadataObjects.of("catalog1.schema1", genericTopic.name(), MetadataObject.Type.TOPIC));
  }

  @Test
  public void testAssociatePoliciesForCatalog() throws JsonProcessingException {
    testAssociatePolicies(
        relationalCatalog.supportsPolicies(),
        MetadataObjects.of(null, relationalCatalog.name(), MetadataObject.Type.CATALOG));

    testAssociatePolicies(
        filesetCatalog.supportsPolicies(),
        MetadataObjects.of(null, filesetCatalog.name(), MetadataObject.Type.CATALOG));

    testAssociatePolicies(
        messagingCatalog.supportsPolicies(),
        MetadataObjects.of(null, messagingCatalog.name(), MetadataObject.Type.CATALOG));
  }

  @Test
  public void testAssociatePoliciesForSchema() throws JsonProcessingException {
    testAssociatePolicies(
        genericSchema.supportsPolicies(),
        MetadataObjects.of("catalog1", genericSchema.name(), MetadataObject.Type.SCHEMA));
  }

  @Test
  public void testAssociatePoliciesForTable() throws JsonProcessingException {
    testAssociatePolicies(
        relationalTable.supportsPolicies(),
        MetadataObjects.of("catalog1.schema1", relationalTable.name(), MetadataObject.Type.TABLE));
  }

  @Test
  public void testAssociatePoliciesForFileset() throws JsonProcessingException {
    testAssociatePolicies(
        genericFileset.supportsPolicies(),
        MetadataObjects.of("catalog1.schema1", genericFileset.name(), MetadataObject.Type.FILESET));
  }

  @Test
  public void testAssociatePoliciesForTopic() throws JsonProcessingException {
    testAssociatePolicies(
        genericTopic.supportsPolicies(),
        MetadataObjects.of("catalog1.schema1", genericTopic.name(), MetadataObject.Type.TOPIC));
  }

  private void testListPolicies(SupportsPolicies supportsPolicies, MetadataObject metadataObject)
      throws JsonProcessingException {
    String path =
        "/api/metalakes/"
            + METALAKE_NAME
            + "/objects/"
            + metadataObject.type().name().toLowerCase(Locale.ROOT)
            + "/"
            + metadataObject.fullName()
            + "/policies";

    String[] policies = new String[] {"policy1", "policy2"};
    NameListResponse resp = new NameListResponse(policies);
    buildMockResource(Method.GET, path, null, resp, SC_OK);

    String[] actualPolicies = supportsPolicies.listPolicies();
    Assertions.assertArrayEquals(policies, actualPolicies);

    // Return empty list
    NameListResponse resp1 = new NameListResponse(new String[0]);
    buildMockResource(Method.GET, path, null, resp1, SC_OK);

    String[] actualPolicies1 = supportsPolicies.listPolicies();
    Assertions.assertArrayEquals(new String[0], actualPolicies1);

    // Test throw NotFoundException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NotFoundException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, errorResp, SC_NOT_FOUND);

    Throwable ex = Assertions.assertThrows(NotFoundException.class, supportsPolicies::listPolicies);
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse errorResp1 = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, path, null, errorResp1, SC_INTERNAL_SERVER_ERROR);

    Throwable ex1 = Assertions.assertThrows(RuntimeException.class, supportsPolicies::listPolicies);
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));
  }

  private void testListPoliciesInfo(
      SupportsPolicies supportsPolicies, MetadataObject metadataObject)
      throws JsonProcessingException {
    String path =
        "/api/metalakes/"
            + METALAKE_NAME
            + "/objects/"
            + metadataObject.type().name().toLowerCase(Locale.ROOT)
            + "/"
            + metadataObject.fullName()
            + "/policies";

    PolicyDTO policy1 =
        PolicyDTO.builder()
            .withName("policy1")
            .withPolicyType("custom")
            .withContent(contentDTO)
            .withAudit(AuditDTO.builder().withCreator("test").build())
            .build();
    PolicyDTO policy2 =
        PolicyDTO.builder()
            .withName("policy2")
            .withPolicyType("custom")
            .withContent(contentDTO)
            .withAudit(AuditDTO.builder().withCreator("test").build())
            .build();
    PolicyDTO[] policies = new PolicyDTO[] {policy1, policy2};
    PolicyListResponse resp = new PolicyListResponse(policies);
    buildMockResource(Method.GET, path, null, resp, SC_OK);

    Policy[] actualPolicies = supportsPolicies.listPolicyInfos();
    Assertions.assertEquals(2, actualPolicies.length);
    Assertions.assertEquals("policy1", actualPolicies[0].name());
    Assertions.assertEquals("policy2", actualPolicies[1].name());

    // Return empty list
    PolicyListResponse resp1 = new PolicyListResponse(new PolicyDTO[0]);
    buildMockResource(Method.GET, path, null, resp1, SC_OK);

    Policy[] actualPolicies1 = supportsPolicies.listPolicyInfos();
    Assertions.assertArrayEquals(new Policy[0], actualPolicies1);

    // Test throw NotFoundException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NotFoundException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, errorResp, SC_NOT_FOUND);

    Throwable ex = Assertions.assertThrows(NotFoundException.class, supportsPolicies::listPolicies);
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse errorResp1 = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, path, null, errorResp1, SC_INTERNAL_SERVER_ERROR);

    Throwable ex1 = Assertions.assertThrows(RuntimeException.class, supportsPolicies::listPolicies);
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));
  }

  private void testGetPolicy(SupportsPolicies supportsPolicies, MetadataObject metadataObject)
      throws JsonProcessingException {
    String path =
        "/api/metalakes/"
            + METALAKE_NAME
            + "/objects/"
            + metadataObject.type().name().toLowerCase(Locale.ROOT)
            + "/"
            + metadataObject.fullName()
            + "/policies/policy1";

    PolicyDTO policy1 =
        PolicyDTO.builder()
            .withName("policy1")
            .withPolicyType("test")
            .withContent(contentDTO)
            .withAudit(AuditDTO.builder().withCreator("test").build())
            .build();

    PolicyResponse resp = new PolicyResponse(policy1);
    buildMockResource(Method.GET, path, null, resp, SC_OK);

    Policy actualPolicy = supportsPolicies.getPolicy("policy1");
    Assertions.assertEquals("policy1", actualPolicy.name());

    // Test throw NoSuchPolicyException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchPolicyException.class.getSimpleName(), "mock error");
    buildMockResource(Method.GET, path, null, errorResp, SC_NOT_FOUND);

    Throwable ex =
        Assertions.assertThrows(
            NoSuchPolicyException.class, () -> supportsPolicies.getPolicy("policy1"));
    Assertions.assertTrue(ex.getMessage().contains("mock error"));

    // Test throw internal error
    ErrorResponse errorResp1 = ErrorResponse.internalError("mock error");
    buildMockResource(Method.GET, path, null, errorResp1, SC_INTERNAL_SERVER_ERROR);

    Throwable ex1 =
        Assertions.assertThrows(
            RuntimeException.class, () -> supportsPolicies.getPolicy("policy1"));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));
  }

  private void testAssociatePolicies(
      SupportsPolicies supportsPolicies, MetadataObject metadataObject)
      throws JsonProcessingException {
    String path =
        "/api/metalakes/"
            + METALAKE_NAME
            + "/objects/"
            + metadataObject.type().name().toLowerCase(Locale.ROOT)
            + "/"
            + metadataObject.fullName()
            + "/policies";

    String[] policiesToAdd = new String[] {"policy1", "policy2"};
    String[] policiesToRemove = new String[] {"policy3", "policy4"};
    PoliciesAssociateRequest request =
        new PoliciesAssociateRequest(policiesToAdd, policiesToRemove);

    NameListResponse resp = new NameListResponse(policiesToAdd);
    buildMockResource(Method.POST, path, request, resp, SC_OK);

    String[] actualPolicies = supportsPolicies.associatePolicies(policiesToAdd, policiesToRemove);
    Assertions.assertArrayEquals(policiesToAdd, actualPolicies);

    // Test throw internal error
    ErrorResponse errorResp1 = ErrorResponse.internalError("mock error");
    buildMockResource(Method.POST, path, request, errorResp1, SC_INTERNAL_SERVER_ERROR);

    Throwable ex1 =
        Assertions.assertThrows(
            RuntimeException.class,
            () -> supportsPolicies.associatePolicies(policiesToAdd, policiesToRemove));
    Assertions.assertTrue(ex1.getMessage().contains("mock error"));
  }
}
