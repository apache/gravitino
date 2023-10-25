/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive;

import static com.datastrato.gravitino.catalog.hive.HiveSchemaPropertiesMetadata.LOCATION;

import com.datastrato.gravitino.catalog.rel.BaseSchema;
import com.datastrato.gravitino.meta.AuditInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
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

  private Configuration conf;

  private HiveSchema() {}

  /**
   * Creates a new HiveSchema instance from a Database and a Builder.
   *
   * @param db The Database representing the HiveSchema.
   * @param hiveConf The HiveConf used to construct the HiveSchema.
   * @return A new HiveSchema instance.
   */
  public static HiveSchema fromHiveDB(Database db, Configuration hiveConf) {
    Map<String, String> properties = buildSchemaProperties(db);

    // Get audit info from Hive's Database object. Because Hive's database doesn't store create
    // time, last modifier and last modified time, we only get creator from Hive's database.
    AuditInfo.Builder auditInfoBuilder = new AuditInfo.Builder();
    Optional.ofNullable(db.getOwnerName()).ifPresent(auditInfoBuilder::withCreator);

    return new HiveSchema.Builder()
        .withName(db.getName())
        .withComment(db.getDescription())
        .withProperties(properties)
        .withAuditInfo(auditInfoBuilder.build())
        .withConf(hiveConf)
        .build();
  }

  /**
   * Build schema properties from a Hive Database object.
   *
   * @param database The Hive Database object.
   * @return A map of schema properties.
   */
  public static Map<String, String> buildSchemaProperties(Database database) {
    Map<String, String> properties = Maps.newHashMap(database.getParameters());
    properties.put(LOCATION, database.getLocationUri());
    return properties;
  }

  /**
   * Converts this HiveSchema to its corresponding Database.
   *
   * @return The converted Database object.
   */
  public Database toHiveDB() {
    Database hiveDb = new Database();

    hiveDb.setName(name());
    Optional.ofNullable(properties().get(LOCATION)).ifPresent(hiveDb::setLocationUri);
    Optional.ofNullable(comment()).ifPresent(hiveDb::setDescription);

    // TODO: Add more privilege info to Hive's Database object after Gravitino supports privilege.
    hiveDb.setOwnerName(auditInfo().creator());
    hiveDb.setOwnerType(PrincipalType.USER);

    Map<String, String> parameters = Maps.newHashMap(properties());
    parameters.remove(LOCATION);
    hiveDb.setParameters(parameters);

    return hiveDb;
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
      hiveSchema.name = name;
      hiveSchema.comment = comment;
      hiveSchema.properties = properties;
      hiveSchema.auditInfo = auditInfo;
      hiveSchema.conf = conf;

      return hiveSchema;
    }
  }
}
