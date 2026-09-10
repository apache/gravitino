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

import static org.apache.hc.core5.http.HttpStatus.SC_OK;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.messaging.TopicDTO;
import org.apache.gravitino.dto.model.ModelDTO;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.SQLRepresentationDTO;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.dto.rel.ViewDTO;
import org.apache.gravitino.dto.responses.SecretsResponse;
import org.apache.gravitino.rel.Dialects;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.secret.SupportsSecrets;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Behavioral tests for client {@link SupportsSecrets} wiring on metadata objects. */
public class TestSupportSecrets extends TestBase {

  private static final String METALAKE_NAME = "metalake_secrets";

  private static GravitinoMetalake metalake;
  private static RelationalCatalog relationalCatalog;
  private static RelationalTable relationalTable;
  private static GenericView genericView;
  private static GenericTopic genericTopic;
  private static GenericModel genericModel;

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();
    metalake = TestGravitinoMetalake.createMetalake(client, METALAKE_NAME);

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

    Namespace tableNs = Namespace.of(METALAKE_NAME, "catalog1", "schema1");
    relationalTable =
        RelationalTable.from(
            tableNs,
            TableDTO.builder()
                .withName("table1")
                .withComment("comment")
                .withColumns(
                    new ColumnDTO[] {
                      ColumnDTO.builder()
                          .withName("id")
                          .withDataType(Types.IntegerType.get())
                          .build()
                    })
                .withProperties(Collections.emptyMap())
                .withAudit(AuditDTO.builder().withCreator("test").build())
                .build(),
            client.restClient());

    genericView =
        new GenericView(
            ViewDTO.builder()
                .withName("view1")
                .withComment("comment")
                .withColumns(new ColumnDTO[0])
                .withRepresentations(
                    new SQLRepresentationDTO[] {
                      SQLRepresentationDTO.builder()
                          .withDialect(Dialects.TRINO)
                          .withSql("SELECT 1")
                          .build()
                    })
                .withProperties(Collections.emptyMap())
                .withAudit(AuditDTO.builder().withCreator("test").build())
                .build(),
            client.restClient(),
            tableNs);

    genericTopic =
        new GenericTopic(
            TopicDTO.builder()
                .withName("topic1")
                .withComment("comment")
                .withProperties(Collections.emptyMap())
                .withAudit(AuditDTO.builder().withCreator("test").build())
                .build(),
            client.restClient(),
            tableNs);

    genericModel =
        new GenericModel(
            ModelDTO.builder()
                .withName("model1")
                .withComment("comment")
                .withProperties(Collections.emptyMap())
                .withLatestVersion(0)
                .withAudit(AuditDTO.builder().withCreator("test").build())
                .build(),
            client.restClient(),
            tableNs);
  }

  @Test
  public void testGetSecretsForMetalake() throws JsonProcessingException {
    testGetSecrets(
        metalake.supportsSecrets(),
        MetadataObjects.of(null, METALAKE_NAME, MetadataObject.Type.METALAKE));
  }

  @Test
  public void testGetSecretsForCatalog() throws JsonProcessingException {
    testGetSecrets(
        relationalCatalog.supportsSecrets(),
        MetadataObjects.of(null, relationalCatalog.name(), MetadataObject.Type.CATALOG));
  }

  @Test
  public void testGetSecretsForTable() throws JsonProcessingException {
    testGetSecrets(
        relationalTable.supportsSecrets(),
        MetadataObjects.of("catalog1.schema1", relationalTable.name(), MetadataObject.Type.TABLE));
  }

  @Test
  public void testGetSecretsForView() throws JsonProcessingException {
    testGetSecrets(
        genericView.supportsSecrets(),
        MetadataObjects.of("catalog1.schema1", genericView.name(), MetadataObject.Type.VIEW));
  }

  @Test
  public void testGetSecretsForTopic() throws JsonProcessingException {
    testGetSecrets(
        genericTopic.supportsSecrets(),
        MetadataObjects.of("catalog1.schema1", genericTopic.name(), MetadataObject.Type.TOPIC));
  }

  @Test
  public void testGetSecretsForModel() throws JsonProcessingException {
    testGetSecrets(
        genericModel.supportsSecrets(),
        MetadataObjects.of("catalog1.schema1", genericModel.name(), MetadataObject.Type.MODEL));
  }

  private void testGetSecrets(SupportsSecrets supportsSecrets, MetadataObject metadataObject)
      throws JsonProcessingException {
    String path =
        "/api/metalakes/"
            + METALAKE_NAME
            + "/objects/"
            + metadataObject.type().name().toLowerCase(Locale.ROOT)
            + "/"
            + metadataObject.fullName()
            + "/secrets";

    Map<String, String> expected = ImmutableMap.of("jdbc-password", "s3cr3t", "custom-token", "t");
    SecretsResponse resp = new SecretsResponse(expected);
    buildMockResource(Method.GET, path, null, resp, SC_OK);

    Map<String, String> secrets = supportsSecrets.getSecrets();
    Assertions.assertEquals(expected, secrets);

    SecretsResponse empty = new SecretsResponse(Collections.emptyMap());
    buildMockResource(Method.GET, path, null, empty, SC_OK);
    Assertions.assertTrue(supportsSecrets.getSecrets().isEmpty());
  }
}
