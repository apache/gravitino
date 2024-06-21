/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.dto.tag;

import com.datastrato.gravitino.MetadataObject;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.json.JsonUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTagDTO {

  @Test
  public void testTagSerDe() throws JsonProcessingException {
    AuditDTO audit = AuditDTO.builder().withCreator("user1").withCreateTime(Instant.now()).build();

    TagDTO tagDTO =
        TagDTO.builder().withName("tag_test").withComment("tag comment").withAudit(audit).build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(tagDTO);
    TagDTO deserTagDTO = JsonUtils.objectMapper().readValue(serJson, TagDTO.class);
    Assertions.assertEquals(tagDTO, deserTagDTO);

    Assertions.assertEquals("tag_test", deserTagDTO.name());
    Assertions.assertEquals("tag comment", deserTagDTO.comment());
    Assertions.assertEquals(audit, deserTagDTO.auditInfo());
    Assertions.assertNull(deserTagDTO.properties());

    // Test tag with property
    Map<String, String> properties = ImmutableMap.of("key", "value");
    TagDTO tagDTO1 =
        TagDTO.builder()
            .withName("tag_test")
            .withComment("tag comment")
            .withAudit(audit)
            .withProperties(properties)
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(tagDTO1);
    TagDTO deserTagDTO1 = JsonUtils.objectMapper().readValue(serJson, TagDTO.class);
    Assertions.assertEquals(tagDTO1, deserTagDTO1);

    Assertions.assertEquals(properties, deserTagDTO1.properties());

    // Test tag with inherited
    TagDTO tagDTO2 =
        TagDTO.builder()
            .withName("tag_test")
            .withComment("tag comment")
            .withAudit(audit)
            .withInherited(Optional.empty())
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(tagDTO2);
    TagDTO deserTagDTO2 = JsonUtils.objectMapper().readValue(serJson, TagDTO.class);
    Assertions.assertEquals(tagDTO2, deserTagDTO2);
    Assertions.assertEquals(Optional.empty(), deserTagDTO2.inherited());

    TagDTO tagDTO3 =
        TagDTO.builder()
            .withName("tag_test")
            .withComment("tag comment")
            .withAudit(audit)
            .withInherited(Optional.of(false))
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(tagDTO3);
    TagDTO deserTagDTO3 = JsonUtils.objectMapper().readValue(serJson, TagDTO.class);
    Assertions.assertEquals(Optional.of(false), deserTagDTO3.inherited());

    TagDTO tagDTO4 =
        TagDTO.builder()
            .withName("tag_test")
            .withComment("tag comment")
            .withAudit(audit)
            .withInherited(Optional.of(true))
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(tagDTO4);
    TagDTO deserTagDTO4 = JsonUtils.objectMapper().readValue(serJson, TagDTO.class);
    Assertions.assertEquals(Optional.of(true), deserTagDTO4.inherited());

    // Test tag with metadata objects
    MetadataObjectDTO metalakeDTO =
        MetadataObjectDTO.builder()
            .withName("metalake_test")
            .withType(MetadataObject.Type.METALAKE)
            .build();

    MetadataObjectDTO catalogDTO =
        MetadataObjectDTO.builder()
            .withName("catalog_test")
            .withType(MetadataObject.Type.CATALOG)
            .build();

    MetadataObjectDTO schemaDTO =
        MetadataObjectDTO.builder()
            .withName("schema_test")
            .withParent("catalog_test")
            .withType(MetadataObject.Type.SCHEMA)
            .build();

    MetadataObjectDTO tableDTO =
        MetadataObjectDTO.builder()
            .withName("table_test")
            .withParent("catalog_test.schema_test")
            .withType(MetadataObject.Type.TABLE)
            .build();

    MetadataObjectDTO[] objectDTOs =
        new MetadataObjectDTO[] {metalakeDTO, catalogDTO, schemaDTO, tableDTO};

    TagDTO tagDTO5 =
        TagDTO.builder()
            .withName("tag_test")
            .withComment("tag comment")
            .withAudit(audit)
            .withObjects(objectDTOs)
            .build();

    serJson = JsonUtils.objectMapper().writeValueAsString(tagDTO5);
    TagDTO deserTagDTO5 = JsonUtils.objectMapper().readValue(serJson, TagDTO.class);
    Assertions.assertEquals(tagDTO5, deserTagDTO5);
    Assertions.assertArrayEquals(objectDTOs, deserTagDTO5.objects());
  }
}
