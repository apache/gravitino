/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;

import com.datastrato.graviton.meta.rel.BaseSchema;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import lombok.ToString;

/** A Hive Schema(Database) entity */
@ToString
public class HiveSchema extends BaseSchema {
  private static final String HMS_DB_OWNER = "hive.metastore.database.owner";
  private static final String HMS_DB_OWNER_TYPE = "hive.metastore.database.owner-type";

  private Database innerDb;

  private Configuration conf;

  private HiveSchema() {}

  public static HiveSchema fromInnerDB(Database db, Builder builder) {
    Map<String, String> properties = convertToMetadata(db);

    return builder
        .withName(db.getName())
        .withComment(db.getDescription())
        .withProperties(properties)
        .build();
  }

  public static Map<String, String> convertToMetadata(Database database) {
    Map<String, String> meta = Maps.newHashMap();

    meta.putAll(database.getParameters());
    meta.put("location", database.getLocationUri());
    if (database.getDescription() != null) {
      meta.put("comment", database.getDescription());
    }
    if (database.getOwnerName() != null) {
      meta.put(HMS_DB_OWNER, database.getOwnerName());
      if (database.getOwnerType() != null) {
        meta.put(HMS_DB_OWNER_TYPE, database.getOwnerType().name());
      }
    }

    return meta;
  }

  public Database toInnerDB() {
    if (innerDb != null) {
      return innerDb;
    }

    innerDb = new Database();
    Map<String, String> parameter = Maps.newHashMap();
    innerDb.setName(name());
    innerDb.setLocationUri(databaseLocation(name()));
    if (comment() != null) {
      innerDb.setDescription(comment());
    }

    Optional.ofNullable(properties)
        .orElse(Collections.emptyMap())
        .forEach(
            (key, value) -> {
              if (key.equals("location")) {
                innerDb.setLocationUri(value);
              } else if (key.equals(HMS_DB_OWNER)) {
                innerDb.setOwnerName(value);
              } else if (key.equals(HMS_DB_OWNER_TYPE) && value != null) {
                innerDb.setOwnerType(PrincipalType.valueOf(value));
              } else {
                if (value != null) {
                  parameter.put(key, value);
                }
              }
            });

    if (innerDb.getOwnerName() == null) {
      innerDb.setOwnerName(auditInfo().creator());
      innerDb.setOwnerType(PrincipalType.USER);
    }

    innerDb.setParameters(parameter);

    return innerDb;
  }

  private String databaseLocation(String databaseName) {
    String warehouseLocation = conf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
    Preconditions.checkNotNull(
        warehouseLocation, "Warehouse location is not set: hive.metastore.warehouse.dir=null");
    warehouseLocation = stripTrailingSlash(warehouseLocation);
    return String.format("%s/%s.db", warehouseLocation, databaseName);
  }

  private static String stripTrailingSlash(String path) {
    Preconditions.checkArgument(
        path != null && path.length() > 0, "path must not be null or empty");

    String result = path;
    while (result.endsWith("/")) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }

  public static class Builder extends BaseSchemaBuilder<Builder, HiveSchema> {

    protected Configuration conf;

    public Builder withConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    @Override
    protected HiveSchema internalBuild() {
      HiveSchema hiveSchema = new HiveSchema();
      hiveSchema.id = id;
      hiveSchema.catalogId = catalogId;
      hiveSchema.name = name;
      hiveSchema.namespace = namespace;
      hiveSchema.comment = comment;
      hiveSchema.properties = properties;
      hiveSchema.auditInfo = auditInfo;
      hiveSchema.conf = conf;

      return hiveSchema;
    }
  }
}
