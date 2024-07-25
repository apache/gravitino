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
package org.apache.gravitino.dto.tag;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.json.JsonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMetadataObjectDTO {

  @Test
  void testObjectSerDe() throws JsonProcessingException {

    // Test metalake object
    MetadataObjectDTO metalakeDTO =
        MetadataObjectDTO.builder()
            .withName("metalake_test")
            .withType(MetadataObject.Type.METALAKE)
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(metalakeDTO);
    String expected = "{\"fullName\":\"metalake_test\",\"type\":\"metalake\"}";
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected), JsonUtils.objectMapper().readTree(serJson));

    MetadataObjectDTO deserMetalakeDTO =
        JsonUtils.objectMapper().readValue(serJson, MetadataObjectDTO.class);
    Assertions.assertEquals(metalakeDTO, deserMetalakeDTO);

    // Test catalog object
    MetadataObjectDTO catalogDTO =
        MetadataObjectDTO.builder()
            .withName("catalog_test")
            .withType(MetadataObject.Type.CATALOG)
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(catalogDTO);
    expected = "{\"fullName\":\"catalog_test\",\"type\":\"catalog\"}";
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected), JsonUtils.objectMapper().readTree(serJson));

    MetadataObjectDTO deserCatalogDTO =
        JsonUtils.objectMapper().readValue(serJson, MetadataObjectDTO.class);
    Assertions.assertEquals(catalogDTO, deserCatalogDTO);

    // Test schema object
    MetadataObjectDTO schemaDTO =
        MetadataObjectDTO.builder()
            .withName("schema_test")
            .withParent("catalog_test")
            .withType(MetadataObject.Type.SCHEMA)
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(schemaDTO);
    expected = "{\"fullName\":\"catalog_test.schema_test\",\"type\":\"schema\"}";
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected), JsonUtils.objectMapper().readTree(serJson));

    MetadataObjectDTO deserSchemaDTO =
        JsonUtils.objectMapper().readValue(serJson, MetadataObjectDTO.class);
    Assertions.assertEquals(schemaDTO, deserSchemaDTO);

    // Test table object
    MetadataObjectDTO tableDTO =
        MetadataObjectDTO.builder()
            .withName("table_test")
            .withParent("catalog_test.schema_test")
            .withType(MetadataObject.Type.TABLE)
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(tableDTO);
    expected = "{\"fullName\":\"catalog_test.schema_test.table_test\",\"type\":\"table\"}";
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected), JsonUtils.objectMapper().readTree(serJson));

    MetadataObjectDTO deserTableDTO =
        JsonUtils.objectMapper().readValue(serJson, MetadataObjectDTO.class);
    Assertions.assertEquals(tableDTO, deserTableDTO);

    // Test column object
    MetadataObjectDTO columnDTO =
        MetadataObjectDTO.builder()
            .withName("column_test")
            .withParent("catalog_test.schema_test.table_test")
            .withType(MetadataObject.Type.COLUMN)
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(columnDTO);
    expected =
        "{\"fullName\":\"catalog_test.schema_test.table_test.column_test\",\"type\":\"column\"}";
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected), JsonUtils.objectMapper().readTree(serJson));

    MetadataObjectDTO deserColumnDTO =
        JsonUtils.objectMapper().readValue(serJson, MetadataObjectDTO.class);
    Assertions.assertEquals(columnDTO, deserColumnDTO);

    // Test topic object
    MetadataObjectDTO topicDTO =
        MetadataObjectDTO.builder()
            .withName("topic_test")
            .withParent("catalog_test.schema_test")
            .withType(MetadataObject.Type.TOPIC)
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(topicDTO);
    expected = "{\"fullName\":\"catalog_test.schema_test.topic_test\",\"type\":\"topic\"}";
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected), JsonUtils.objectMapper().readTree(serJson));

    MetadataObjectDTO deserTopicDTO =
        JsonUtils.objectMapper().readValue(serJson, MetadataObjectDTO.class);
    Assertions.assertEquals(topicDTO, deserTopicDTO);

    // Test fileset object
    MetadataObjectDTO filesetDTO =
        MetadataObjectDTO.builder()
            .withName("fileset_test")
            .withParent("catalog_test.schema_test")
            .withType(MetadataObject.Type.FILESET)
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(filesetDTO);
    expected = "{\"fullName\":\"catalog_test.schema_test.fileset_test\",\"type\":\"fileset\"}";
    Assertions.assertEquals(
        JsonUtils.objectMapper().readTree(expected), JsonUtils.objectMapper().readTree(serJson));

    MetadataObjectDTO deserFilesetDTO =
        JsonUtils.objectMapper().readValue(serJson, MetadataObjectDTO.class);
    Assertions.assertEquals(filesetDTO, deserFilesetDTO);
  }
}
