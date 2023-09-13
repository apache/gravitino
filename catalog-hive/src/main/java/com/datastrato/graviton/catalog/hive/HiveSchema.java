/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.rel.BaseSchema;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import lombok.ToString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;

/** Represents a Hive Schema (Database) entity in the Hive Metastore catalog. */
@ToString
public class HiveSchema extends BaseSchema {
  private static final String HMS_DB_OWNER = "hive.metastore.database.owner";
  private static final String HMS_DB_OWNER_TYPE = "hive.metastore.database.owner-type";

  private Database innerDb;

  private Configuration conf;

  private HiveSchema() {}

  /**
   * Creates a new HiveSchema instance from a Database and a Builder.
   *
   * @param db The Database representing the HiveSchema.
   * @param builder The Builder used to construct the HiveSchema.
   * @return A new HiveSchema instance.
   */
  public static HiveSchema fromInnerDB(Database db, Builder builder) {
    Map<String, String> properties = convertToMetadata(db);

    // Get audit info from Hive's Database object. Because Hive's database doesn't store create
    // time, last modifier and last modified time, we only get creator from Hive's database.
    AuditInfo.Builder auditInfoBuilder = new AuditInfo.Builder();
    Optional.ofNullable(db.getOwnerName()).ifPresent(auditInfoBuilder::withCreator);

    return builder
        .withName(db.getName())
        .withComment(db.getDescription())
        .withProperties(properties)
        .withAuditInfo(auditInfoBuilder.build())
        .build();
  }

  /**
   * Converts a Database to metadata represented as a map of key-value pairs.
   *
   * @param database The Database to be converted to metadata.
   * @return A map containing the metadata key-value pairs.
   */
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

  /**
   * Converts this HiveSchema to its corresponding Database.
   *
   * @return The converted Database object.
   */
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

  /** A builder class for constructing HiveSchema instances. */
  public static class Builder extends BaseSchemaBuilder<Builder, HiveSchema> {

    protected Configuration conf;

    /**
     * Sets the Configuration to be used for building the HiveSchema.
     *
     * @param conf The Configuration.
     * @return The Builder instance.
     */
    public Builder withConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    /**
     * Internal method to build a HiveSchema instance using the provided values.
     *
     * @return A new HiveSchema instance with the configured values.
     */
    @Override
    protected HiveSchema internalBuild() {
      HiveSchema hiveSchema = new HiveSchema();
      hiveSchema.id = id;
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
