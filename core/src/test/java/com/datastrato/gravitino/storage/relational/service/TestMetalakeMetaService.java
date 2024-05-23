/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.service;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.storage.relational.TestJDBCBackend;
import com.datastrato.gravitino.storage.relational.mapper.CatalogMetaMapper;
import com.datastrato.gravitino.storage.relational.mapper.SchemaMetaMapper;
import com.datastrato.gravitino.storage.relational.po.CatalogPO;
import com.datastrato.gravitino.storage.relational.po.SchemaPO;
import com.datastrato.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import com.datastrato.gravitino.storage.relational.utils.SessionUtils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestMetalakeMetaService extends TestJDBCBackend {

  String metalakeName = "metalake";

  @Test
  void insertMetalake() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    MetalakeMetaService metalakeMetaService = MetalakeMetaService.getInstance();

    Assertions.assertDoesNotThrow(() -> metalakeMetaService.insertMetalake(metalake, false));
    BaseMetalake metalakeResult =
        metalakeMetaService.getMetalakeByIdentifier(NameIdentifier.ofMetalake(metalakeName));
    Assertions.assertEquals(metalake, metalakeResult);

    List<CatalogEntity> catalogEntities =
        CatalogMetaService.getInstance().listCatalogsByNamespace(Namespace.ofCatalog(metalakeName));
    Assertions.assertEquals(0, catalogEntities.size());

    List<CatalogPO> catalogPOs =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.listCatalogPOsByMetalakeId(metalake.id()));
    Assertions.assertEquals(1, catalogPOs.size());
    Assertions.assertEquals(
        Entity.SYSTEM_CATALOG_RESERVED_NAME, catalogPOs.get(0).getCatalogName());

    List<SchemaPO> schemaPOS =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.listSchemaPOsByCatalogId(catalogPOs.get(0).getCatalogId()));
    Assertions.assertEquals(3, schemaPOS.size());
    Assertions.assertTrue(
        schemaPOS.stream()
            .anyMatch(schemaPO -> schemaPO.getSchemaName().equals(Entity.USER_SCHEMA_NAME)));
    Assertions.assertTrue(
        schemaPOS.stream()
            .anyMatch(schemaPO -> schemaPO.getSchemaName().equals(Entity.GROUP_SCHEMA_NAME)));
    Assertions.assertTrue(
        schemaPOS.stream()
            .anyMatch(schemaPO -> schemaPO.getSchemaName().equals(Entity.ROLE_SCHEMA_NAME)));
  }

  @Test
  void deleteMetalake() {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    BaseMetalake metalake =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), metalakeName, auditInfo);
    MetalakeMetaService metalakeMetaService = MetalakeMetaService.getInstance();

    // insert metalake
    metalakeMetaService.insertMetalake(metalake, false);
    BaseMetalake metalakeResult =
        metalakeMetaService.getMetalakeByIdentifier(NameIdentifier.ofMetalake(metalakeName));
    Assertions.assertEquals(metalake, metalakeResult);

    List<CatalogEntity> catalogEntities =
        CatalogMetaService.getInstance().listCatalogsByNamespace(Namespace.ofCatalog(metalakeName));
    Assertions.assertEquals(0, catalogEntities.size());

    List<CatalogPO> catalogPOs =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.listCatalogPOsByMetalakeId(metalake.id()));
    Assertions.assertEquals(1, catalogPOs.size());
    Assertions.assertEquals(
        Entity.SYSTEM_CATALOG_RESERVED_NAME, catalogPOs.get(0).getCatalogName());

    List<SchemaPO> schemaPOs =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.listSchemaPOsByCatalogId(catalogPOs.get(0).getCatalogId()));
    Assertions.assertEquals(3, schemaPOs.size());
    Assertions.assertTrue(
        schemaPOs.stream()
            .anyMatch(schemaPO -> schemaPO.getSchemaName().equals(Entity.USER_SCHEMA_NAME)));
    Assertions.assertTrue(
        schemaPOs.stream()
            .anyMatch(schemaPO -> schemaPO.getSchemaName().equals(Entity.GROUP_SCHEMA_NAME)));
    Assertions.assertTrue(
        schemaPOs.stream()
            .anyMatch(schemaPO -> schemaPO.getSchemaName().equals(Entity.ROLE_SCHEMA_NAME)));

    // delete metalake
    metalakeMetaService.deleteMetalake(NameIdentifier.ofMetalake(metalakeName), false);
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> metalakeMetaService.getMetalakeByIdentifier(NameIdentifier.ofMetalake(metalakeName)));

    List<CatalogPO> catalogPOs1 =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.listCatalogPOsByMetalakeId(metalake.id()));
    Assertions.assertEquals(0, catalogPOs1.size());
    int schemaNum = countSchemas(metalake.id());
    Assertions.assertEquals(0, schemaNum);

    // insert metalake
    BaseMetalake metalake1 =
        createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake1", auditInfo);
    metalakeMetaService.insertMetalake(metalake1, false);
    List<CatalogEntity> catalogEntities2 =
        CatalogMetaService.getInstance().listCatalogsByNamespace(Namespace.ofCatalog("metalake1"));
    Assertions.assertEquals(0, catalogEntities2.size());

    List<CatalogPO> catalogPOs2 =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.listCatalogPOsByMetalakeId(metalake1.id()));
    Assertions.assertEquals(1, catalogPOs2.size());

    List<SchemaPO> schemaPOs2 =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class,
            mapper -> mapper.listSchemaPOsByCatalogId(catalogPOs2.get(0).getCatalogId()));
    Assertions.assertEquals(3, schemaPOs2.size());

    // delete metalake with cascade
    metalakeMetaService.deleteMetalake(NameIdentifier.ofMetalake("metalake1"), true);
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> metalakeMetaService.getMetalakeByIdentifier(NameIdentifier.ofMetalake("metalake1")));

    List<CatalogPO> catalogPOs3 =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.listCatalogPOsByMetalakeId(metalake1.id()));
    Assertions.assertEquals(0, catalogPOs3.size());
    int schemaNum1 = countSchemas(metalake1.id());
    Assertions.assertEquals(0, schemaNum1);
  }

  private Integer countSchemas(Long metalakeId) {
    int count = 0;
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT count(*) FROM schema_meta where metalake_id = %d and deleted_at = 0",
                    metalakeId))) {
      while (rs.next()) {
        count = rs.getInt(1);
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return count;
  }
}
