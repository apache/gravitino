/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational.mapper;

import com.datastrato.gravitino.storage.relational.po.CatalogPO;
import java.util.List;
import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

public interface CatalogMetaMapper {
  String TABLE_NAME = "catalog_meta";

  @Select(
      "SELECT id, catalog_name as catalogName, metalake_id as metalakeId,"
          + " type, provider, catalog_comment as catalogComment,"
          + " properties, audit_info as auditInfo"
          + " FROM "
          + TABLE_NAME
          + " WHERE metalake_id = #{metalakeId}")
  List<CatalogPO> listCatalogPOsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @Select(
      "SELECT id FROM "
          + TABLE_NAME
          + " WHERE catalog_name = #{catalogName} and metalake_id = #{metalakeId}")
  Long selectCatalogIdByNameAndMetalakeId(
      @Param("catalogName") String name, @Param("metalakeId") Long metalakeId);

  @Select(
      "SELECT id, catalog_name as catalogName,"
          + " metalake_id as metalakeId, type, provider,"
          + " catalog_comment as catalogComment, properties,"
          + " audit_info as auditInfo"
          + " FROM "
          + TABLE_NAME
          + " WHERE catalog_name = #{catalogName} and metalake_id = #{metalakeId}")
  CatalogPO selectCatalogMetaByNameAndMetalakeId(
      @Param("catalogName") String name, @Param("metalakeId") Long metalakeId);

  @Insert(
      "INSERT INTO "
          + TABLE_NAME
          + "(id, catalog_name, metalake_id, type, provider, catalog_comment, properties, audit_info)"
          + " VALUES("
          + " #{catalogMeta.id},"
          + " #{catalogMeta.catalogName},"
          + " #{catalogMeta.metalakeId},"
          + " #{catalogMeta.type},"
          + " #{catalogMeta.provider},"
          + " #{catalogMeta.catalogComment},"
          + " #{catalogMeta.properties},"
          + " #{catalogMeta.auditInfo}"
          + " )")
  void insertCatalogMeta(@Param("catalogMeta") CatalogPO catalogPO);

  @Insert(
      "INSERT INTO "
          + TABLE_NAME
          + "(id, catalog_name, metalake_id, type, provider, metalake_comment, properties, audit_info)"
          + " VALUES("
          + " #{catalogMeta.id},"
          + " #{catalogMeta.catalogName},"
          + " #{catalogMeta.metalakeId},"
          + " #{catalogMeta.type},"
          + " #{catalogMeta.provider},"
          + " #{catalogMeta.catalogComment},"
          + " #{catalogMeta.properties},"
          + " #{catalogMeta.auditInfo}"
          + " )"
          + " ON DUPLICATE KEY UPDATE"
          + " catalog_name = #{catalogMeta.catalogName},"
          + " metalake_id = #{catalogMeta.metalakeId},"
          + " type = #{catalogMeta.type},"
          + " provider = #{catalogMeta.provider},"
          + " catalog_comment = #{catalogMeta.catalogComment},"
          + " properties = #{catalogMeta.properties},"
          + " audit_info = #{catalogMeta.auditInfo}")
  void insertCatalogMetaOnDuplicateKeyUpdate(@Param("catalogMeta") CatalogPO catalogPO);

  @Update(
      "UPDATE "
          + TABLE_NAME
          + " SET catalog_name = #{newCatalogMeta.metalakeName},"
          + " metalake_id = #{newCatalogMeta.metalakeId},"
          + " type = #{newCatalogMeta.type},"
          + " provider = #{newCatalogMeta.provider},"
          + " catalog_comment = #{newCatalogMeta.catalogComment},"
          + " properties = #{newCatalogMeta.properties},"
          + " audit_info = #{newCatalogMeta.auditInfo}"
          + " WHERE id = #{oldCatalogMeta.id}"
          + " and catalog_name = #{oldCatalogMeta.catalogName}"
          + " and metalake_id = #{oldCatalogMeta.metalakeId}"
          + " and type = #{oldCatalogMeta.type}"
          + " and provider = #{oldCatalogMeta.provider}"
          + " and catalog_comment = #{oldCatalogMeta.catalogComment}"
          + " and properties = #{oldCatalogMeta.properties}"
          + " and audit_info = #{oldCatalogMeta.auditInfo}")
  Integer updateCatalogMeta(
      @Param("newCatalogMeta") CatalogPO newCatalogPO,
      @Param("oldCatalogMeta") CatalogPO oldCatalogPO);

  @Delete("DELETE FROM " + TABLE_NAME + " WHERE id = #{id}")
  Integer deleteCatalogMetasById(@Param("id") Long id);

  @Delete("DELETE FROM " + TABLE_NAME + " WHERE metalake_id = #{metalakeId}")
  Integer deleteCatalogMetasByMetalakeId(@Param("metalakeId") Long metalakeId);
}
