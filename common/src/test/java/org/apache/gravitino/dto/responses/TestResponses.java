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
package org.apache.gravitino.dto.responses;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.SchemaDTO;
import org.apache.gravitino.dto.authorization.GroupDTO;
import org.apache.gravitino.dto.authorization.RoleDTO;
import org.apache.gravitino.dto.authorization.SecurableObjectDTO;
import org.apache.gravitino.dto.authorization.UserDTO;
import org.apache.gravitino.dto.model.ModelDTO;
import org.apache.gravitino.dto.model.ModelVersionDTO;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.dto.rel.partitioning.Partitioning;
import org.apache.gravitino.dto.tag.TagDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.json.JsonUtils;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

public class TestResponses {
  @Test
  void testBaseDefaultCode() throws IllegalArgumentException {
    BaseResponse base = new BaseResponse();
    assertEquals(0, base.getCode());
  }

  @Test
  void testBaseValidateNegativeNumber() throws IllegalArgumentException {
    BaseResponse base = new BaseResponse(-1);
    assertThrows(IllegalArgumentException.class, () -> base.validate());
  }

  @Test
  void testBaseValidate() throws IllegalArgumentException {
    BaseResponse base = new BaseResponse();
    base.validate(); // No exception thrown
  }

  @Test
  void testDropped() throws IllegalArgumentException {
    DropResponse drop = new DropResponse();

    assertNull(drop.dropped());
  }

  @Test
  void testDroppedTrue() throws IllegalArgumentException {
    DropResponse drop = new DropResponse(true);

    assertTrue(drop.dropped());
  }

  @Test
  void testEntityListResponse() throws IllegalArgumentException {
    NameIdentifier[] identsA = {NameIdentifier.parse("TableA")};
    EntityListResponse entityList = new EntityListResponse(identsA);
    entityList.validate(); // No exception thrown
    NameIdentifier[] identsB = entityList.identifiers();
    assertEquals(1, identsB.length);
    assertEquals("TableA", identsB[0].name());
  }

  @Test
  void testEntityListResponseException() throws IllegalArgumentException {
    EntityListResponse entityList = new EntityListResponse();
    assertThrows(IllegalArgumentException.class, () -> entityList.validate());
  }

  @Test
  void testMetalakeResponse() throws IllegalArgumentException {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    MetalakeDTO metalake = MetalakeDTO.builder().withName("Metalake").withAudit(audit).build();
    MetalakeResponse response = new MetalakeResponse(metalake);
    response.validate(); // No exception thrown
  }

  @Test
  void testMetalakeResponseException() throws IllegalArgumentException {
    MetalakeResponse response = new MetalakeResponse();
    assertThrows(IllegalArgumentException.class, () -> response.validate());
  }

  @Test
  void testMetalakeListResponse() throws IllegalArgumentException {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    MetalakeDTO metalake = MetalakeDTO.builder().withName("Metalake").withAudit(audit).build();
    MetalakeListResponse response = new MetalakeListResponse(new MetalakeDTO[] {metalake});
    response.validate(); // No exception thrown
  }

  @Test
  void testMetalakeListResponseException() throws IllegalArgumentException {
    MetalakeListResponse response = new MetalakeListResponse();
    assertThrows(IllegalArgumentException.class, () -> response.validate());
  }

  @Test
  void testCatalogResponse() throws IllegalArgumentException {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    CatalogDTO catalog =
        CatalogDTO.builder()
            .withName("CatalogA")
            .withComment("comment")
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withAudit(audit)
            .build();
    CatalogResponse catalogResponse = new CatalogResponse(catalog);
    catalogResponse.validate(); // No exception thrown
  }

  @Test
  void testCatalogException() throws IllegalArgumentException {
    CatalogResponse catalog = new CatalogResponse();
    assertThrows(IllegalArgumentException.class, () -> catalog.validate());
  }

  @Test
  void testCatalogListResponse() throws IllegalArgumentException {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    CatalogDTO catalog =
        CatalogDTO.builder()
            .withName("CatalogA")
            .withComment("comment")
            .withType(Catalog.Type.RELATIONAL)
            .withProvider("test")
            .withAudit(audit)
            .build();
    CatalogListResponse response = new CatalogListResponse(new CatalogDTO[] {catalog});
    response.validate(); // No exception thrown
  }

  @Test
  void testCatalogListException() throws IllegalArgumentException {
    CatalogListResponse response = new CatalogListResponse();
    assertThrows(IllegalArgumentException.class, () -> response.validate());
  }

  @Test
  void testSchemaResponse() throws IllegalArgumentException {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    SchemaDTO schema =
        SchemaDTO.builder().withName("SchemaA").withComment("comment").withAudit(audit).build();
    SchemaResponse schemaResponse = new SchemaResponse(schema);
    schemaResponse.validate(); // No exception thrown
  }

  @Test
  void testSchemaException() throws IllegalArgumentException {
    SchemaResponse schema = new SchemaResponse();
    assertThrows(IllegalArgumentException.class, () -> schema.validate());
  }

  @Test
  void testTableResponse() throws IllegalArgumentException {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    ColumnDTO column =
        ColumnDTO.builder().withName("ColumnA").withDataType(Types.ByteType.get()).build();
    TableDTO table =
        TableDTO.builder()
            .withName("TableA")
            .withComment("comment")
            .withColumns(new ColumnDTO[] {column})
            .withAudit(audit)
            .withPartitioning(Partitioning.EMPTY_PARTITIONING)
            .build();
    TableResponse tableResponse = new TableResponse(table);
    tableResponse.validate(); // No exception thrown
  }

  @Test
  void testTableException() throws IllegalArgumentException {
    TableResponse table = new TableResponse();
    assertThrows(IllegalArgumentException.class, () -> table.validate());
  }

  @Test
  void testRestErrorResponse() throws IllegalArgumentException {
    ErrorResponse error = ErrorResponse.restError("Rest error");
    error.validate(); // No exception thrown
  }

  @Test
  void testIllegalArgumentsErrorResponse() throws IllegalArgumentException {
    ErrorResponse error = ErrorResponse.illegalArguments("illegal arguments error");
    error.validate(); // No exception thrown
  }

  @Test
  void testNotFoundErrorResponse() throws IllegalArgumentException {
    ErrorResponse error = ErrorResponse.notFound("error type", "not found error");
    error.validate(); // No exception thrown
  }

  @Test
  void testAlreadyExistsErrorResponse() throws IllegalArgumentException {
    ErrorResponse error = ErrorResponse.alreadyExists("error type", "already exists error");
    error.validate(); // No exception thrown
  }

  @Test
  void testNonEmptyErrorResponse() throws IllegalArgumentException {
    ErrorResponse error = ErrorResponse.nonEmpty("error type", "non empty error");
    error.validate(); // No exception thrown
  }

  @Test
  void testUnknownErrorResponse() throws IllegalArgumentException {
    ErrorResponse error = ErrorResponse.unknownError("unknown error");
    error.validate(); // No exception thrown
  }

  @Test
  void testOAuthTokenResponse() throws IllegalArgumentException {
    OAuth2TokenResponse response =
        new OAuth2TokenResponse("Bearer xx", null, "Bearer", null, null, null);
    response.validate();
  }

  @Test
  void testOAuthTokenException() throws IllegalArgumentException {
    OAuth2TokenResponse response = new OAuth2TokenResponse();
    assertThrows(IllegalArgumentException.class, () -> response.validate());
  }

  @Test
  void testOAuthErrorResponse() throws IllegalArgumentException {
    OAuth2ErrorResponse response = new OAuth2ErrorResponse("invalid_grant", "error");
    response.validate(); // No exception thrown
  }

  @Test
  void testOAuthErrorException() throws IllegalArgumentException {
    OAuth2ErrorResponse response = new OAuth2ErrorResponse();
    assertThrows(IllegalArgumentException.class, () -> response.validate());
  }

  @Test
  void testUserResponse() throws IllegalArgumentException {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    UserDTO user = UserDTO.builder().withName("user1").withAudit(audit).build();
    UserResponse response = new UserResponse(user);
    response.validate(); // No exception thrown
  }

  @Test
  void testUserResponseException() throws IllegalArgumentException {
    UserResponse user = new UserResponse();
    assertThrows(IllegalArgumentException.class, () -> user.validate());
  }

  @Test
  void testGroupResponse() throws IllegalArgumentException {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    GroupDTO group = GroupDTO.builder().withName("group1").withAudit(audit).build();
    GroupResponse response = new GroupResponse(group);
    response.validate(); // No exception thrown
  }

  @Test
  void testGroupResponseException() throws IllegalArgumentException {
    GroupResponse group = new GroupResponse();
    assertThrows(IllegalArgumentException.class, () -> group.validate());
  }

  @Test
  void testRoleResponse() throws IllegalArgumentException {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    SecurableObject securableObject =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));
    RoleDTO role =
        RoleDTO.builder()
            .withName("role1")
            .withSecurableObjects(new SecurableObjectDTO[] {DTOConverters.toDTO(securableObject)})
            .withAudit(audit)
            .build();
    RoleResponse response = new RoleResponse(role);
    response.validate(); // No exception thrown
  }

  @Test
  void testRoleResponseException() throws IllegalArgumentException {
    RoleResponse role = new RoleResponse();
    assertThrows(IllegalArgumentException.class, () -> role.validate());
  }

  @Test
  void testNameListResponse() throws JsonProcessingException {
    String[] names = new String[] {"name1", "name2"};
    NameListResponse response = new NameListResponse(names);
    assertDoesNotThrow(response::validate);

    String serJson = JsonUtils.objectMapper().writeValueAsString(response);
    NameListResponse deserResponse =
        JsonUtils.objectMapper().readValue(serJson, NameListResponse.class);
    assertEquals(response, deserResponse);
    assertArrayEquals(names, deserResponse.getNames());

    NameListResponse response1 = new NameListResponse();
    Exception e = assertThrows(IllegalArgumentException.class, response1::validate);
    assertEquals("\"names\" must not be null", e.getMessage());
  }

  @Test
  void testTagListResponse() throws JsonProcessingException {
    TagDTO tag1 = TagDTO.builder().withName("tag1").withComment("comment1").build();
    TagDTO tag2 = TagDTO.builder().withName("tag2").withComment("comment2").build();
    TagDTO[] tags = new TagDTO[] {tag1, tag2};
    TagListResponse response = new TagListResponse(tags);
    assertDoesNotThrow(response::validate);

    String serJson = JsonUtils.objectMapper().writeValueAsString(response);
    TagListResponse deserResponse =
        JsonUtils.objectMapper().readValue(serJson, TagListResponse.class);
    assertEquals(response, deserResponse);
    assertArrayEquals(tags, deserResponse.getTags());

    TagListResponse response1 = new TagListResponse();
    Exception e = assertThrows(IllegalArgumentException.class, response1::validate);
    assertEquals("\"tags\" must not be null", e.getMessage());
  }

  @Test
  void testTagResponse() throws JsonProcessingException {
    TagDTO tag = TagDTO.builder().withName("tag1").withComment("comment1").build();
    TagResponse response = new TagResponse(tag);
    assertDoesNotThrow(response::validate);

    String serJson = JsonUtils.objectMapper().writeValueAsString(response);
    TagResponse deserResponse = JsonUtils.objectMapper().readValue(serJson, TagResponse.class);
    assertEquals(response, deserResponse);
    assertEquals(tag, deserResponse.getTag());

    TagResponse response1 = new TagResponse();
    Exception e = assertThrows(IllegalArgumentException.class, response1::validate);
    assertEquals("\"tag\" must not be null", e.getMessage());
  }

  @Test
  void testFileLocationResponse() {
    String actualFileLocation = "file:/test/1";
    FileLocationResponse response = new FileLocationResponse(actualFileLocation);
    response.validate(); // No exception thrown
  }

  @Test
  void testFileLocationResponseException() {
    FileLocationResponse response = new FileLocationResponse();
    assertThrows(IllegalArgumentException.class, () -> response.validate());
  }

  @Test
  void testModelResponse() throws JsonProcessingException {
    Map<String, String> props = ImmutableMap.of("key", "value");
    AuditDTO audit = AuditDTO.builder().withCreator("user1").withCreateTime(Instant.now()).build();

    ModelDTO modelDTO =
        ModelDTO.builder()
            .withName("model1")
            .withLatestVersion(0)
            .withComment("comment1")
            .withProperties(props)
            .withAudit(audit)
            .build();

    ModelResponse response = new ModelResponse(modelDTO);
    String serJson = JsonUtils.objectMapper().writeValueAsString(response);
    ModelResponse deserResponse = JsonUtils.objectMapper().readValue(serJson, ModelResponse.class);

    assertEquals(response, deserResponse);

    ModelResponse response1 = new ModelResponse();
    assertThrows(IllegalArgumentException.class, response1::validate);
  }

  @Test
  void testModelVersionListResponse() throws JsonProcessingException {
    ModelVersionListResponse response1 = new ModelVersionListResponse(new int[] {});
    assertDoesNotThrow(response1::validate);

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(response1);
    ModelVersionListResponse deserResponse1 =
        JsonUtils.objectMapper().readValue(serJson1, ModelVersionListResponse.class);
    assertEquals(response1, deserResponse1);
    assertArrayEquals(new int[] {}, deserResponse1.getVersions());

    ModelVersionListResponse response2 = new ModelVersionListResponse(new int[] {1, 2});
    assertDoesNotThrow(response2::validate);

    String serJson2 = JsonUtils.objectMapper().writeValueAsString(response2);
    ModelVersionListResponse deserResponse2 =
        JsonUtils.objectMapper().readValue(serJson2, ModelVersionListResponse.class);
    assertEquals(response2, deserResponse2);
    assertArrayEquals(new int[] {1, 2}, deserResponse2.getVersions());
  }

  @Test
  void testModelVersionInfoListResponse() throws JsonProcessingException {
    ModelVersionInfoListResponse response1 =
        new ModelVersionInfoListResponse(new ModelVersionDTO[] {});
    assertDoesNotThrow(response1::validate);

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(response1);
    ModelVersionInfoListResponse deserResponse1 =
        JsonUtils.objectMapper().readValue(serJson1, ModelVersionInfoListResponse.class);
    assertEquals(response1, deserResponse1);
    assertArrayEquals(new ModelVersionDTO[] {}, deserResponse1.getVersions());

    ModelVersionDTO v1 =
        ModelVersionDTO.builder()
            .withVersion(0)
            .withComment("model version1 comment")
            .withAliases(new String[] {"alias1", "alias2"})
            .withUris(ImmutableMap.of("n1", "u1"))
            .withProperties(ImmutableMap.of("key", "value"))
            .withAudit(
                AuditDTO.builder().withCreator("user1").withCreateTime(Instant.now()).build())
            .build();
    ModelVersionDTO v2 =
        ModelVersionDTO.builder()
            .withVersion(1)
            .withComment("model version2 comment")
            .withAliases(new String[] {"alias3", "alias4"})
            .withUris(ImmutableMap.of("n2", "u2"))
            .withProperties(ImmutableMap.of("key", "value"))
            .withAudit(
                AuditDTO.builder().withCreator("user1").withCreateTime(Instant.now()).build())
            .build();

    ModelVersionInfoListResponse response2 =
        new ModelVersionInfoListResponse(new ModelVersionDTO[] {v1, v2});
    assertDoesNotThrow(response2::validate);

    String serJson2 = JsonUtils.objectMapper().writeValueAsString(response2);
    ModelVersionInfoListResponse deserResponse2 =
        JsonUtils.objectMapper().readValue(serJson2, ModelVersionInfoListResponse.class);
    assertEquals(response2, deserResponse2);
    assertArrayEquals(new ModelVersionDTO[] {v1, v2}, deserResponse2.getVersions());
  }

  @Test
  void testModelVersionResponse() throws JsonProcessingException {
    Map<String, String> props = ImmutableMap.of("key", "value");
    AuditDTO audit = AuditDTO.builder().withCreator("user1").withCreateTime(Instant.now()).build();

    ModelVersionDTO modelVersionDTO =
        ModelVersionDTO.builder()
            .withVersion(0)
            .withComment("model version comment")
            .withAliases(new String[] {"alias1", "alias2"})
            .withUris(ImmutableMap.of("n1", "u1"))
            .withProperties(props)
            .withAudit(audit)
            .build();

    ModelVersionResponse response = new ModelVersionResponse(modelVersionDTO);
    response.validate(); // No exception thrown

    String serJson = JsonUtils.objectMapper().writeValueAsString(response);
    ModelVersionResponse deserResponse =
        JsonUtils.objectMapper().readValue(serJson, ModelVersionResponse.class);
    assertEquals(response, deserResponse);

    ModelVersionResponse response1 = new ModelVersionResponse();
    assertThrows(IllegalArgumentException.class, response1::validate);
  }
}
