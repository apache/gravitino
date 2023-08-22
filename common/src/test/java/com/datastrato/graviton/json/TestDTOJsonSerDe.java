/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.json;

import com.datastrato.graviton.Catalog;
import com.datastrato.graviton.dto.AuditDTO;
import com.datastrato.graviton.dto.CatalogDTO;
import com.datastrato.graviton.dto.MetalakeDTO;
import com.datastrato.graviton.dto.rel.ColumnDTO;
import com.datastrato.graviton.dto.rel.TableDTO;
import com.google.common.collect.ImmutableMap;
import io.substrait.type.StringTypeVisitor;
import io.substrait.type.TypeCreator;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDTOJsonSerDe {

  private final String auditJson =
      "{\"creator\":%s,\"createTime\":%s,\"lastModifier\":%s,\"lastModifiedTime\":%s}";

  private final String metalakeJson = "{\"name\":%s,\"comment\":%s,\"properties\":%s,\"audit\":%s}";

  private final String columnJson = "{\"name\":%s,\"type\":%s,\"comment\":%s}";

  private final String tableJson =
      "{\"name\":%s,\"comment\":%s,\"columns\":[%s],\"properties\":%s,\"audit\":%s}";

  private String withQuotes(String str) {
    return "\"" + str + "\"";
  }

  @Test
  public void testAuditDTOJsonSerDe() throws Exception {
    Instant now = Instant.now();
    String creator = "creator";
    String modifier = "modifier";

    // Test with required fields
    AuditDTO audit = AuditDTO.builder().withCreator(creator).withCreateTime(now).build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(audit);
    String expectedJson =
        String.format(auditJson, withQuotes(creator), withQuotes(now.toString()), null, null);
    Assertions.assertEquals(expectedJson, serJson);
    AuditDTO deserAudit = JsonUtils.objectMapper().readValue(serJson, AuditDTO.class);
    Assertions.assertEquals(audit, deserAudit);

    // Test with optional fields
    AuditDTO audit1 =
        AuditDTO.builder()
            .withCreator(creator)
            .withCreateTime(now)
            .withLastModifier(modifier)
            .withLastModifiedTime(now)
            .build();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(audit1);
    String expectedJson1 =
        String.format(
            auditJson,
            withQuotes(creator),
            withQuotes(now.toString()),
            withQuotes(modifier),
            withQuotes(now.toString()));
    Assertions.assertEquals(expectedJson1, serJson1);
    AuditDTO deserAudit1 = JsonUtils.objectMapper().readValue(serJson1, AuditDTO.class);
    Assertions.assertEquals(audit1, deserAudit1);
  }

  @Test
  public void testMetalakeDTOSerDe() throws Exception {
    String name = "metalake";
    String comment = "comment";
    Map<String, String> properties = ImmutableMap.of("k1", "v1", "k2", "v2");
    String creator = "creator";
    Instant now = Instant.now();
    AuditDTO audit = AuditDTO.builder().withCreator(creator).withCreateTime(now).build();

    // Test with required fields
    MetalakeDTO metalake =
        new MetalakeDTO.Builder()
            .withName(name)
            .withComment(comment)
            .withProperties(properties)
            .withAudit(audit)
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(metalake);
    String expectedJson =
        String.format(
            metalakeJson,
            withQuotes(name),
            withQuotes(comment),
            JsonUtils.objectMapper().writeValueAsString(properties),
            String.format(auditJson, withQuotes(creator), withQuotes(now.toString()), null, null));
    Assertions.assertEquals(expectedJson, serJson);
    MetalakeDTO desermetalake = JsonUtils.objectMapper().readValue(serJson, MetalakeDTO.class);
    Assertions.assertEquals(metalake, desermetalake);

    // Test with optional fields
    MetalakeDTO metalake1 = new MetalakeDTO.Builder().withName(name).withAudit(audit).build();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(metalake1);
    String expectedJson1 =
        String.format(
            metalakeJson,
            withQuotes(name),
            null,
            null,
            String.format(auditJson, withQuotes(creator), withQuotes(now.toString()), null, null));
    Assertions.assertEquals(expectedJson1, serJson1);
    MetalakeDTO desermetalake1 = JsonUtils.objectMapper().readValue(serJson1, MetalakeDTO.class);
    Assertions.assertEquals(metalake1, desermetalake1);
  }

  @Test
  public void testCatalogDTOSerDe() throws Exception {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    CatalogDTO catalog =
        new CatalogDTO.Builder()
            .withName("catalog")
            .withType(Catalog.Type.RELATIONAL)
            .withComment("comment")
            .withProperties(ImmutableMap.of("k1", "v1", "k2", "v2"))
            .withAudit(audit)
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(catalog);
    CatalogDTO deserCatalog = JsonUtils.objectMapper().readValue(serJson, CatalogDTO.class);
    Assertions.assertEquals(catalog, deserCatalog);

    // test with optional fields
    CatalogDTO catalog1 =
        new CatalogDTO.Builder()
            .withName("catalog")
            .withType(Catalog.Type.RELATIONAL)
            .withAudit(audit)
            .build();

    String serJson1 = JsonUtils.objectMapper().writeValueAsString(catalog1);
    CatalogDTO deserCatalog1 = JsonUtils.objectMapper().readValue(serJson1, CatalogDTO.class);
    Assertions.assertEquals(catalog1, deserCatalog1);
  }

  @Test
  public void testColumnDTOSerDe() throws Exception {
    String name = "column";
    io.substrait.type.Type type = TypeCreator.NULLABLE.I8;
    String comment = "comment";

    // Test required fields
    ColumnDTO column =
        ColumnDTO.builder().withName(name).withDataType(type).withComment(comment).build();
    String serJson = JsonUtils.objectMapper().writeValueAsString(column);
    String expectedJson =
        String.format(
            columnJson,
            withQuotes(name),
            withQuotes(type.accept(new StringTypeVisitor())),
            withQuotes(comment));
    Assertions.assertEquals(expectedJson, serJson);
    ColumnDTO deserColumn = JsonUtils.objectMapper().readValue(serJson, ColumnDTO.class);
    Assertions.assertEquals(column, deserColumn);
  }

  @Test
  public void testTableDTOSerDe() throws Exception {
    String name = "column";
    io.substrait.type.Type type = TypeCreator.NULLABLE.I8;
    String comment = "comment";
    String creator = "creator";
    Instant now = Instant.now();
    AuditDTO audit = AuditDTO.builder().withCreator(creator).withCreateTime(now).build();

    String tableName = "table";
    String tableComment = "comment";
    Map<String, String> properties = ImmutableMap.of("k1", "v1", "k2", "v2");

    ColumnDTO column =
        ColumnDTO.builder().withName(name).withDataType(type).withComment(comment).build();
    TableDTO table =
        TableDTO.builder()
            .withName(tableName)
            .withComment(tableComment)
            .withColumns(new ColumnDTO[] {column})
            .withProperties(properties)
            .withAudit(audit)
            .build();

    String serJson = JsonUtils.objectMapper().writeValueAsString(table);
    String expectedJson =
        String.format(
            tableJson,
            withQuotes(tableName),
            withQuotes(tableComment),
            String.format(
                columnJson,
                withQuotes(name),
                withQuotes(type.accept(new StringTypeVisitor())),
                withQuotes(comment)),
            JsonUtils.objectMapper().writeValueAsString(properties),
            String.format(auditJson, withQuotes(creator), withQuotes(now.toString()), null, null));
    Assertions.assertEquals(expectedJson, serJson);
  }
}
