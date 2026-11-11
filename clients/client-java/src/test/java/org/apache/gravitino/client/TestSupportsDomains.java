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
import java.util.Locale;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainRelation;
import org.apache.gravitino.domain.SupportsDomains;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.domain.DomainDTO;
import org.apache.gravitino.dto.file.FilesetDTO;
import org.apache.gravitino.dto.messaging.TopicDTO;
import org.apache.gravitino.dto.model.ModelDTO;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.dto.rel.ViewDTO;
import org.apache.gravitino.dto.responses.DomainListResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.tag.TagValue;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Tests domain lookup through all six leaf resource interfaces. */
public class TestSupportsDomains extends TestBase {
  private static final String NAME = "asset/a";
  private static final Namespace NAMESPACE = Namespace.of("metalake", "cat", "db");

  /**
   * Verifies domain client behavior.
   *
   * @throws Exception if HTTP mock setup fails
   */
  @Test
  public void testAllLeafResources() throws Exception {
    TableDTO table = Mockito.mock(TableDTO.class);
    Mockito.when(table.name()).thenReturn(NAME);
    ViewDTO view = Mockito.mock(ViewDTO.class);
    Mockito.when(view.name()).thenReturn(NAME);
    FilesetDTO fileset = Mockito.mock(FilesetDTO.class);
    Mockito.when(fileset.name()).thenReturn(NAME);
    TopicDTO topic = Mockito.mock(TopicDTO.class);
    Mockito.when(topic.name()).thenReturn(NAME);
    ModelDTO model = Mockito.mock(ModelDTO.class);
    Mockito.when(model.name()).thenReturn(NAME);
    SemanticModel semanticModel = Mockito.mock(SemanticModel.class);
    Mockito.when(semanticModel.name()).thenReturn(NAME);
    Mockito.when(semanticModel.properties()).thenReturn(Collections.emptyMap());
    SupportsDomains[] operations = {
      RelationalTable.from(NAMESPACE, table, client.restClient()).supportsDomains(),
      new GenericView(view, client.restClient(), NAMESPACE).supportsDomains(),
      new GenericFileset(fileset, client.restClient(), NAMESPACE).supportsDomains(),
      new GenericTopic(topic, client.restClient(), NAMESPACE).supportsDomains(),
      new GenericModel(model, client.restClient(), NAMESPACE).supportsDomains(),
      new GenericSemanticModel(semanticModel, client.restClient(), NAMESPACE).supportsDomains()
    };
    MetadataObject.Type[] types = {
      MetadataObject.Type.TABLE,
      MetadataObject.Type.VIEW,
      MetadataObject.Type.FILESET,
      MetadataObject.Type.TOPIC,
      MetadataObject.Type.MODEL,
      MetadataObject.Type.SEMANTIC_MODEL
    };
    DomainRelation[] relations = {
      DomainRelation.direct(),
      DomainRelation.inherited(MetadataObjects.parse("cat.db", MetadataObject.Type.SCHEMA)),
      DomainRelation.viaSubdomain("Trading/a. b")
    };
    DomainDTO dto =
        new DomainDTO(
            "Finance/a. b",
            null,
            Collections.emptyMap(),
            Domain.Status.PUBLISHED,
            null,
            TagValue.of("dept", "finance"),
            AuditDTO.builder()
                .withCreator("test")
                .withCreateTime(Instant.parse("2026-01-01T00:00:00Z"))
                .build(),
            relations);
    for (int i = 0; i < operations.length; i++) {
      String path =
          "/api/metalakes/metalake/objects/"
              + types[i].name().toLowerCase(Locale.ROOT)
              + "/"
              + RESTUtils.encodeString(
                  MetadataObjects.of(Arrays.asList("cat", "db", NAME), types[i]).fullName())
              + "/domains";
      buildMockResource(
          Method.GET, path, null, new NameListResponse(new String[] {dto.getName()}), 200);
      Assertions.assertArrayEquals(new String[] {dto.getName()}, operations[i].listDomains());
      buildMockResource(
          Method.GET,
          path,
          Collections.singletonMap("details", "true"),
          null,
          new DomainListResponse(new DomainDTO[] {dto}),
          200);
      Domain domain = operations[i].listDomainsInfo()[0];
      Assertions.assertEquals(dto.getName(), domain.name());
      Assertions.assertEquals(TagValue.of("dept", "finance"), domain.backingTag().get());
      Assertions.assertArrayEquals(relations, domain.relations());
      domain.relations()[0] = null;
      Assertions.assertArrayEquals(relations, domain.relations());
      buildMockResource(Method.GET, path, null, ErrorResponse.forbidden("denied", null), 403);
      Assertions.assertThrows(ForbiddenException.class, operations[i]::listDomains);
    }
  }
}
